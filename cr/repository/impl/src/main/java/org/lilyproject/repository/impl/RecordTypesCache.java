/*
 * Copyright 2011 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.repository.impl;

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.repository.api.*;

public class RecordTypesCache {
    private Log log = LogFactory.getLog(getClass());

    // A lock on the monitor needs to be taken when changes are to be made on
    // the nameCache, on the count variable, on the nameCacheOutOfDate boolean
    // or if a bucket monitor needs to be added.
    private final Object monitor = new Object();
    // A lock on a bucket monitor needs to be taken when changes are to be made
    // on a bucket.
    private final Map<String, Object> bucketMonitors = new HashMap<String, Object>();
    // The nameCacheOutOfData should be set to true when an update happens on
    // a bucket. This means that if the nameCache is requested, it should be
    // refreshed first. Once it is refreshed it can be put back to false.
    private volatile boolean nameCacheOutOfDate = false;
    // The count indicates how many buckets are being updated. As long as the
    // count is higher than 0, the nameCache can not be updated since this could
    // lead to an inconsistent state (two types could get the same name).
    private volatile int count = 0;

    private Map<QName, RecordType> nameCache;
    private Map<String, Map<SchemaId, RecordType>> buckets;

    public RecordTypesCache() {
        nameCache = new HashMap<QName, RecordType>();
        buckets = new HashMap<String, Map<SchemaId, RecordType>>();
    }

    private Map<QName, RecordType> getNameCache() throws InterruptedException {
        // First check if the name cache is out of date
        if (nameCacheOutOfDate) {
            synchronized (monitor) {
                // Wait until no buckets are being updated
                while (count > 0) {
                    monitor.wait();
                }
                // Re-initialize the nameCache
                Map<QName, RecordType> newNameCache = new HashMap<QName, RecordType>();
                for (Map<SchemaId, RecordType> bucket : buckets.values()) {
                    for (RecordType recordType : bucket.values())
                        newNameCache.put(recordType.getName(), recordType);
                }
                nameCache = newNameCache;
                nameCacheOutOfDate = false;
            }
        }
        return nameCache;
    }

    /**
     * Increment the number of buckets being updated
     */
    private void incCount() {
        synchronized (monitor) {
            count++;
            nameCacheOutOfDate = true;
            monitor.notify();
        }
    }

    /**
     * Decrement the number of buckets being updated and mark the nameCache out
     * of date.
     */
    private void decCount() {
        synchronized (monitor) {
            count--;
            nameCacheOutOfDate = true;
            monitor.notify();
        }
    }

    /**
     * Return the monitor of a bucket and create it if it does not exist yet.
     * 
     * @param bucketId
     * @return
     */
    private Object getBucketMonitor(String bucketId) {
        Object bucketMonitor = bucketMonitors.get(bucketId);
        if (bucketMonitor == null) {
            // If the bucket does not exist yet we need to create it
            // Take the lock on the monitor to avoid that another call would
            // created it at the same time
            synchronized (monitor) {
                // Make sure it hasn't been created meanwhile (= between
                // checking for null and taking the lock on the monitor)
                bucketMonitor = bucketMonitors.get(bucketId);
                if (bucketMonitor == null) {
                    bucketMonitor = new Object();
                    bucketMonitors.put(bucketId, bucketMonitor);
                }
            }
        }
        return bucketMonitor;
    }

    /**
     * Return all record types in the cache. To avoid inconsistencies between
     * buckets, we get the nameCache first.
     * 
     * @return
     * @throws InterruptedException
     */
    public Collection<RecordType> getRecordTypes() throws InterruptedException {
        List<RecordType> recordTypes = new ArrayList<RecordType>();
        for (RecordType recordType : getNameCache().values()) {
            recordTypes.add(recordType.clone());
        }
        return recordTypes;
    }

    /**
     * Return the record type based on its name
     * 
     * @param name
     * @return
     * @throws InterruptedException
     */
    public RecordType getRecordType(QName name) throws InterruptedException {
        return getNameCache().get(name);
    }

    /**
     * Get the record type based on its id
     * 
     * @param id
     * @return
     */
    public RecordType getRecordType(SchemaId id) {
        String bucketId = AbstractSchemaCache.encodeHex(id.getBytes());
        Map<SchemaId, RecordType> bucket = buckets.get(bucketId);
        if (bucket == null)
            return null;
        return bucket.get(id);
    }

    /**
     * Refreshes the whole cache to contain the given list of field types.
     * 
     * @param fieldTypes
     * @throws InterruptedException
     */
    public void refreshRecordTypes(List<RecordType> recordTypes) throws InterruptedException {
        // Since we will update all buckets, we take the lock on the monitor for
        // the whole operation
        synchronized (monitor) {
            while (count > 0) {
                monitor.wait();
            }
            // The nameCache can be made up to date as well since everything is
            // being updated
            nameCache = new HashMap<QName, RecordType>(recordTypes.size());
            buckets = new HashMap<String, Map<SchemaId, RecordType>>();
            for (RecordType recordType : recordTypes) {
                nameCache.put(recordType.getName(), recordType);
                String bucketId = AbstractSchemaCache.encodeHex(recordType.getId().getBytes());
                Map<SchemaId, RecordType> bucket = buckets.get(bucketId);
                if (bucket == null) {
                    bucket = new HashMap<SchemaId, RecordType>();
                    buckets.put(bucketId, bucket);
                }
                bucket.put(recordType.getId(), recordType);
            }
            nameCacheOutOfDate = false;
        }
    }

    /**
     * Refresh one bucket with the field types contained in the TypeBucket
     * 
     * @param typeBucket
     */
    public void refreshRecordTypeBucket(TypeBucket typeBucket) {
        String bucketId = typeBucket.getBucketId();

        // Get a lock on the bucket to be updated
        synchronized (getBucketMonitor(bucketId)) {
            // First increment the number of buckets that are being updated and
            // mark the nameCache out of date.
            incCount();
            List<RecordType> recordTypes = typeBucket.getRecordTypes();
            Map<SchemaId, RecordType> newBucket = new HashMap<SchemaId, RecordType>(recordTypes.size());
            // Fill a the bucket with the new record types
            for (RecordType recordType : recordTypes) {
                newBucket.put(recordType.getId(), recordType);
            }
            buckets.put(bucketId, newBucket);
            // Decrement the number of buckets that are being updated again.
            decCount();
        }
    }

    /**
     * Update the cache to contain the new fieldType
     * 
     * @param fieldType
     */
    public void update(RecordType recordType) {
        SchemaId id = recordType.getId();
        String bucketId = AbstractSchemaCache.encodeHex(id.getBytes());
        // Get a lock on the bucket to be updated
        synchronized (getBucketMonitor(bucketId)) {
            // First increment the number of buckets that are being updated and
            // mark the nameCache out of date.
            incCount();
            Map<SchemaId, RecordType> bucket = buckets.get(bucketId);
            // If the bucket does not exist yet, create it
            if (bucket == null) {
                bucket = new HashMap<SchemaId, RecordType>();
                buckets.put(bucketId, bucket);
            }
            // Decrement the number of buckets that are being updated again.
            bucket.put(id, recordType);
            decCount();
        }
    }

}