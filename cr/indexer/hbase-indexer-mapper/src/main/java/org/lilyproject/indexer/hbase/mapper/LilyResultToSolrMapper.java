package org.lilyproject.indexer.hbase.mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.ngdata.hbaseindexer.Configurable;
import com.ngdata.hbaseindexer.parse.SolrUpdateWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.lilyproject.client.LilyClient;
import org.lilyproject.hbaseindex.IndexNotFoundException;
import org.lilyproject.indexer.derefmap.DependantRecordIdsIterator;
import org.lilyproject.indexer.derefmap.DependencyEntry;
import org.lilyproject.indexer.derefmap.DerefMap;
import org.lilyproject.indexer.derefmap.DerefMapHbaseImpl;
import org.lilyproject.indexer.engine.SolrDocumentBuilder;
import org.lilyproject.indexer.engine.ValueEvaluator;
import org.lilyproject.indexer.model.api.LResultToSolrMapper;
import org.lilyproject.indexer.model.indexerconf.DynamicFieldNameTemplateResolver;
import org.lilyproject.indexer.model.indexerconf.DynamicIndexField;
import org.lilyproject.indexer.model.indexerconf.IndexCase;
import org.lilyproject.indexer.model.indexerconf.IndexField;
import org.lilyproject.indexer.model.indexerconf.IndexerConfException;
import org.lilyproject.indexer.model.indexerconf.LilyIndexerConf;
import org.lilyproject.indexer.model.indexerconf.MappingNode;
import org.lilyproject.indexer.model.util.IndexRecordFilterUtil;
import org.lilyproject.repository.api.AbsoluteRecordId;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.FieldTypeNotFoundException;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.LTable;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordNotFoundException;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.api.VersionNotFoundException;
import org.lilyproject.repository.impl.RecordDecoder;
import org.lilyproject.repository.impl.id.AbsoluteRecordIdImpl;
import org.lilyproject.repository.impl.id.SchemaIdImpl;
import org.lilyproject.sep.LilyEventPublisherManager;
import org.lilyproject.util.Pair;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.HBaseTableFactoryImpl;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.hbase.RepoAndTableUtil;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.repo.RecordEvent;
import org.lilyproject.util.repo.RecordEventHelper;
import org.lilyproject.util.repo.VTaggedRecord;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

import static org.lilyproject.util.repo.RecordEvent.Type.CREATE;
import static org.lilyproject.util.repo.RecordEvent.Type.DELETE;
import static org.lilyproject.util.repo.RecordEvent.Type.INDEX;

public class LilyResultToSolrMapper implements LResultToSolrMapper,Configurable {

    private final Log log = LogFactory.getLog(getClass());

    private String repositoryName;
    private String indexName;

    private ZooKeeperItf zooKeeperItf;
    private RecordDecoder recordDecoder;
    private RepositoryManager repositoryManager;
    private LRepository repository;
    private IdGenerator idGenerator;
    private LilyIndexerConf lilyIndexerConf;
    private ValueEvaluator valueEvaluator;
    private DerefMap derefMap;
    private LilyEventPublisherManager eventPublisherManager;
    private String subscriptionId;

    public LilyResultToSolrMapper(String indexName, LilyIndexerConf lilyIndexerConf, RepositoryManager repositoryManager, ZooKeeperItf zooKeeperItf) {
        setIndexName(indexName);
        this.repositoryManager = repositoryManager;
        this.lilyIndexerConf = lilyIndexerConf;
        this.zooKeeperItf = zooKeeperItf;
    }

    @Override

    public void configure(Map<String, String> params) {
        try {
            String repoParam = Optional.fromNullable(params.get(LResultToSolrMapper.REPO_KEY)).or(RepoAndTableUtil.DEFAULT_REPOSITORY);
            setRepositoryName(repoParam);
            init();

        } catch (Exception e) {
            log.error(e);
        }
    }

    protected void init () throws RepositoryException, InterruptedException, IndexerConfException,
            IndexNotFoundException, IOException {
        repository = repositoryManager.getRepository(repositoryName != null ? repositoryName : RepoAndTableUtil.DEFAULT_REPOSITORY);
        idGenerator = repository.getIdGenerator();

        valueEvaluator = new ValueEvaluator(lilyIndexerConf);
        recordDecoder = new RecordDecoder(repository.getTypeManager(), repository.getIdGenerator(), repository.getRecordFactory());
        if (lilyIndexerConf.containsDerefExpressions()) {
            HBaseTableFactory tableFactory = new HBaseTableFactoryImpl(LilyClient.getHBaseConfiguration(zooKeeperItf));
            eventPublisherManager = new LilyEventPublisherManager(tableFactory);
            derefMap = DerefMapHbaseImpl.create(repository.getRepositoryName(), indexName,
                    LilyClient.getHBaseConfiguration(zooKeeperItf), null, repository.getIdGenerator());
        }
    }

    public void stop () {
        Closer.close(eventPublisherManager);
        Closer.close(repository);
        Closer.close(repositoryManager);
        Closer.close(zooKeeperItf);
    }

    @Override
    public boolean containsRequiredData(Result result) {
        try {
            // If we have a request to reindex then all the information we need should be in the payload
            if (result.containsColumn(LilyHBaseSchema.RecordCf.DATA.bytes, LilyHBaseSchema.RecordColumn.PAYLOAD.bytes)) {
                RecordEvent event = new RecordEvent(result.getFamilyMap(LilyHBaseSchema.RecordCf.DATA.bytes)
                        .get(LilyHBaseSchema.RecordColumn.PAYLOAD.bytes), idGenerator);
                return event.getType().equals(INDEX);
            }
        } catch (IOException e) {
            log.error("Unable to decode record event", e);
        }
        // in other cases force a get for safe measure.
        return false;
    }

    @Override
    public boolean isRelevantKV(final KeyValue kv) {
        if (!Arrays.equals(LilyHBaseSchema.RecordCf.DATA.bytes,kv.getFamily())) {
            return false;
        }
        final byte[] qualifier = kv.getQualifier();
        if (qualifier[0] == LilyHBaseSchema.RecordColumn.DATA_PREFIX) {
            SchemaId fieldId = new SchemaIdImpl(Bytes.tail(qualifier, qualifier.length - 1));
            FieldType fieldType  = null;
            try {
                fieldType = repository.getTypeManager().getFieldTypeById(fieldId);
            } catch (IllegalArgumentException e) {
                // no qualifier
                return false;
            } catch (FieldTypeNotFoundException e) {
                // field doesn't exist
                return false;
            } catch (RepositoryException e) {
                log.warn(e);
            } catch (InterruptedException e) {
                log.warn(e);
            }

            if (fieldType == null) {
                return false;
            }

            StaticFieldTypeFinder finder = new StaticFieldTypeFinder(fieldType);
            lilyIndexerConf.getIndexFields().visitAll(finder);
            if (finder.foundRelevant) {
                return true;
            }

            for (DynamicIndexField indexField : lilyIndexerConf.getDynamicFields()) {
                if (indexField.matches(fieldType).match) {
                    return true;
                }
            }
        } else if (qualifier[0] == LilyHBaseSchema.RecordColumn.SYSTEM_PREFIX) {            
            List<LilyHBaseSchema.RecordColumn> columns = Arrays.asList(LilyHBaseSchema.RecordColumn.values());
            Collection<LilyHBaseSchema.RecordColumn> filtered = Collections2.filter(columns,
                    new Predicate<LilyHBaseSchema.RecordColumn>() {
                @Override
                public boolean apply(LilyHBaseSchema.RecordColumn input) {
                    return Arrays.equals(input.bytes, qualifier);
                }
            });
            return !filtered.isEmpty();
        } else {
            // is this a lily key value?
        }


        return false;
    }

    @Override
    public Get getGet(byte[] row) {
        return new Get(row);
    }

    @Override
    public void map(Result result, SolrUpdateWriter solrUpdateWriter) {
        try {
            Record record = recordDecoder.decodeRecord(result);
            RecordEvent event = new RecordEvent(result.getFamilyMap(LilyHBaseSchema.RecordCf.DATA.bytes)
                    .get(LilyHBaseSchema.RecordColumn.PAYLOAD.bytes), idGenerator);

            String tableName = event.getTableName();
            LTable table = repository.getTable(tableName != null ? tableName : LilyHBaseSchema.Table.RECORD.name);

            if (event.getType().equals(INDEX)) {
                if (log.isDebugEnabled()) {
                    log.debug(String.format("Record %1$s: reindex requested for these vtags: %2$s", record.getId(),
                            vtagSetToNameString(event.getVtagsToIndex())));
                }
                RecordEventHelper eventHelper = new RecordEventHelper(event, null, repository.getTypeManager());
                VTaggedRecord vtRecord = new VTaggedRecord(record.getId(), eventHelper, table, repository);

                IndexCase indexCase = lilyIndexerConf.getIndexCase(table.getTableName(), vtRecord.getRecord());
                Set<SchemaId> vtagsToIndex = event.getVtagsToIndex();
                if (indexCase == null) {
                    return;
                }

                // Only keep vtags which should be indexed
                vtagsToIndex.retainAll(indexCase.getVersionTags());
                // Only keep vtags which exist on the record
                vtagsToIndex.retainAll(vtRecord.getVTags().keySet());

                log.debug(vtagsToIndex.toString());
                index(table, vtRecord, vtagsToIndex, solrUpdateWriter);
            } else if (event.getType().equals(DELETE)) {
                solrUpdateWriter.deleteByQuery("lily.id:" + ClientUtils.escapeQueryChars(record.getId().toString()));

                if (log.isDebugEnabled()) {
                    log.debug(String.format("Record %1$s: deleted from index (if present) because of " +
                            "delete record event", record.getId()));
                }

                // After this we can go to update denormalized data
                if (derefMap != null) {
                    updateDenormalizedData(repository.getRepositoryName(), event.getTableName(), record.getId(),
                            null, null);
                }
            } else {
                // CREATE or UPDATE
                VTaggedRecord vtRecord;

                // Based on the partial old/new record state stored in the RecordEvent, determine whether we
                // now match a different IndexCase than before, and if so, if the new case would have less vtags
                // than the old one, perform the necessary deletes on Solr.
                Pair<Record,Record> oldAndNewRecords =
                        IndexRecordFilterUtil.getOldAndNewRecordForRecordFilterEvaluation(record.getId(), event,
                                repository);
                Record oldRecord = oldAndNewRecords.getV1();
                Record newRecord = oldAndNewRecords.getV2();
                IndexCase caseOld = oldRecord != null ? lilyIndexerConf.getIndexCase(
                        event.getTableName(), oldRecord) : null;
                IndexCase caseNew = newRecord != null ? lilyIndexerConf.getIndexCase(
                        event.getTableName(), newRecord) : null;

                if (oldRecord != null && newRecord != null) {
                    if (caseOld != null && caseNew != null) {
                        Set<SchemaId> droppedVtags = new HashSet<SchemaId>(caseOld.getVersionTags());
                        droppedVtags.removeAll(caseNew.getVersionTags());

                        if (droppedVtags.size() > 0) {
                            // Perform deletes
                            for (SchemaId vtag : droppedVtags) {
                                solrUpdateWriter.deleteById(LilyResultToSolrMapper.getIndexId(tableName,
                                        record.getId(), vtag));
                            }
                        }
                    }
                }

                // This is an optimization: an IndexCase with empty vtags list means that this record is
                // included in this index only to trigger updating of denormalized data.
                boolean doIndexing = true;
                if (caseNew != null) {
                    doIndexing = caseNew.getVersionTags().size() > 0;
                } else if (caseNew == null && caseOld != null) {
                    // caseNew == null means either the record has been deleted, or means the record does
                    // not match the recordFilter anymore. In either case, we only need to trigger update
                    // of denormalized data (if the vtags list was empty on caseOld).
                    doIndexing = caseOld.getVersionTags().size() > 0;
                }

                RecordEventHelper eventHelper = new RecordEventHelper(event, null, repository.getTypeManager());

                if (doIndexing) {
                    try {
                        // Read the vtags of the record. Note that while this algorithm is running, the record can
                        // meanwhile undergo changes. However, we continuously work with the snapshot of the vtags
                        // mappings read here. The processing of later events will bring the index up to date with
                        // any new changes.
                        vtRecord = new VTaggedRecord(record.getId(), eventHelper,
                                repository.getTable(event.getTableName()), repository);
                    } catch (RecordNotFoundException e) {
                        // The record has been deleted in the meantime.
                        // For now, we do nothing, when the delete event is received the record will be removed
                        // from the index (as well as update of denormalized data).
                        // TODO: we should process all outstanding messages for the record (up to delete) in one go
                        return;
                    }

                    handleRecordCreateUpdate(vtRecord, table, solrUpdateWriter);
                }

                if (derefMap != null) {
                    updateDenormalizedData(repository.getRepositoryName(), event.getTableName(), record.getId(),
                            eventHelper.getUpdatedFieldsByScope(), eventHelper.getModifiedVTags());
                }
            }
        } catch (Exception e) {
            log.warn("Something went wrong while indexing", e);
        }
    }

    private void updateDenormalizedData(String repo, String table, RecordId recordId, Map<Scope, Set<FieldType>> updatedFieldsByScope,
                                        Set<SchemaId> changedVTagFields)
            throws RepositoryException, InterruptedException, IOException {

        Multimap<AbsoluteRecordId, SchemaId> referrersAndVTags = ArrayListMultimap.create();

        Set<SchemaId> allVTags = lilyIndexerConf.getVtags();

        if (log.isDebugEnabled()) {
            log.debug("Updating denormalized data for " + recordId + ", vtags: " + changedVTagFields);
        }

        // The reason to iterate over all vtags is because a field from a record without versions might be
        // dereferenced into multiple vtagged versions of another record, and we don't know what the [indexed]
        // vtags of that other record are.
        for (SchemaId vtag : allVTags) {
            if ((changedVTagFields != null && changedVTagFields.contains(vtag)) || updatedFieldsByScope == null) {
                // changed vtags or delete: reindex regardless of fields
                AbsoluteRecordId absRecordId = new AbsoluteRecordIdImpl(table, recordId);
                DependantRecordIdsIterator dependants = derefMap.findDependantsOf(absRecordId);
                if (log.isDebugEnabled()) {
                    log.debug("changed vtag: dependants of " + recordId + ": " +
                            depIds(derefMap.findDependantsOf(absRecordId)));
                }
                while (dependants.hasNext()) {
                    referrersAndVTags.put(dependants.next(), vtag);
                }
            } else {
                // vtag didn't change, but some fields did change:
                Set<SchemaId> fields = new HashSet<SchemaId>();
                for (Scope scope : updatedFieldsByScope.keySet()) {
                    fields.addAll(toSchemaIds(updatedFieldsByScope.get(scope)));
                }
                AbsoluteRecordId absRecordId = new AbsoluteRecordIdImpl(table, recordId);
                final DependantRecordIdsIterator dependants = derefMap.findDependantsOf(absRecordId, fields, vtag);
                while (dependants.hasNext()) {
                    referrersAndVTags.put(dependants.next(), vtag);
                }
            }
        }

        if (log.isDebugEnabled()) {
            log.debug(String.format("Record %1$s: found %2$s records (times vtags) to be updated because they " +
                    "might contain outdated denormalized data." +
                    " The records are: %3$s", recordId,
                    referrersAndVTags.keySet().size(), referrersAndVTags.keySet()));
        }

        //
        // Now add an index message to each of the found referrers, their actual indexing
        // will be triggered by the message queue.
        //
        for (AbsoluteRecordId referrer : referrersAndVTags.keySet()) {
            RecordEvent payload = new RecordEvent();
            payload.setTableName(referrer.getTable());
            payload.setType(INDEX);
            for (SchemaId vtag : referrersAndVTags.get(referrer)) {
                payload.addVTagToIndex(vtag);
            }
            RecordEvent.IndexRecordFilterData filterData = new RecordEvent.IndexRecordFilterData();
            filterData.setSubscriptionInclusions(ImmutableSet.of(this.subscriptionId));
            payload.setIndexRecordFilterData(filterData);

            try {
                eventPublisherManager.getEventPublisher(repo, referrer.getTable()
                ).publishEvent(referrer.getRecordId().toBytes(), payload.toJsonBytes());
            } catch (Exception e) {
                // We failed to put the message: this is pretty important since it means the record's index
                // won't get updated, therefore log as error, but after this we continue with the next one.
                log.error("Error putting index message on queue of record " + referrer, e);
                //metrics.errors.inc();
            }
            //metrics.lastReindexRequestedTimestamp.set(System.currentTimeMillis());



        }
    }

    private Set<SchemaId> toSchemaIds(Set<FieldType> fieldTypes) {
        return new HashSet<SchemaId>(Collections2.transform(fieldTypes, new Function<FieldType, SchemaId>() {
            @Override
            public SchemaId apply(FieldType input) {
                return input.getId();
            }
        }));
    }

    private List<AbsoluteRecordId> depIds(DependantRecordIdsIterator dependants) throws IOException {
        List<AbsoluteRecordId> recordIds = Lists.newArrayList();
        while (dependants.hasNext()) {
            recordIds.add(dependants.next());
        }
        return recordIds;
    }

    @Override
    public LRepository getRepository() {
        return this.repository;
    }

    protected static String getIndexId(String table, RecordId recordId, SchemaId vtag) {
        return table + "-" + recordId + "-" + vtag.toString();
    }

    private String evalName(DynamicIndexField dynField, DynamicIndexField.DynamicIndexFieldMatch match, FieldType fieldType) {
        // Calculate the name, then add the value
        Map<String, Object> nameContext = new HashMap<String, Object>();
        nameContext.put("namespace", fieldType.getName().getNamespace());
        nameContext.put("name", fieldType.getName().getName());

        ValueType valueType = fieldType.getValueType();
        nameContext.put("type", formatValueTypeName(valueType));
        nameContext.put("baseType", valueType.getBaseName().toLowerCase());

        // If there's no nested value type, revert to the current value type. This is practical for dynamic
        // fields that match on types like "*,LIST<+>".
        ValueType nestedValueType = valueType.getNestedValueType() != null ? valueType.getNestedValueType() : valueType;
        nameContext.put("nestedType", formatValueTypeName(nestedValueType));
        nameContext.put("nestedBaseType", nestedValueType.getBaseName().toLowerCase());

        nameContext.put("deepestNestedBaseType", valueType.getDeepestValueType().getBaseName().toLowerCase());

        boolean isList = valueType.getBaseName().equals("LIST");
        nameContext.put("multiValue", isList);
        nameContext.put("list", isList);

        if (match.nameMatch != null) {
            nameContext.put("nameMatch", match.nameMatch);
        }
        if (match.namespaceMatch != null) {
            nameContext.put("namespaceMatch", match.namespaceMatch);
        }
        return dynField.getNameTemplate().format(new DynamicFieldNameTemplateResolver(nameContext));
    }

    private String formatValueTypeName(ValueType valueType) {
        StringBuilder builder = new StringBuilder();

        while (valueType != null) {
            if (builder.length() > 0) {
                builder.append("_");
            }
            builder.append(valueType.getBaseName().toLowerCase());
            valueType = valueType.getNestedValueType();
        }

        return builder.toString();
    }

    private void processDependencies(String table, Record record, SchemaId vtag, SolrDocumentBuilder solrDocumentBuilder)
            throws IOException, RepositoryException, InterruptedException {
        if (log.isDebugEnabled()) {
            log.debug("Constructed Solr doc: " + solrDocumentBuilder.build());
            log.debug("Updating dependencies for " + record.getId());
            logDependencies(record.getId(), solrDocumentBuilder.getDependencies());
        }

        if (derefMap != null) {
            derefMap.updateDependants(new AbsoluteRecordIdImpl(table, record.getId()),
                    vtag, solrDocumentBuilder.getDependencies());
        }
    }

    private void logDependencies(RecordId recordId, Map<DependencyEntry, Set<SchemaId>> dependencies) {
        for (DependencyEntry entry : dependencies.keySet()) {
            StringBuilder line = new StringBuilder();
            line.append(recordId).append(" | ")
                    .append(entry.getDependency())
                    .append("|").append(entry.getMoreDimensionedVariants())
                    .append("|").append(dependencies.get(entry));
            log.debug(line.toString());
        }
    }

    private void handleRecordCreateUpdate(VTaggedRecord vtRecord, LTable table, SolrUpdateWriter solrUpdateWriter) throws Exception {

        RecordEvent event = vtRecord.getRecordEvent();
        Map<Long, Set<SchemaId>> vtagsByVersion = vtRecord.getVTagsByVersion();

        // Determine the IndexCase:
        //  The indexing of all versions is determined by the record type of the non-versioned scope.
        //  This makes that the indexing behavior of all versions is equal, and can be changed (the
        //  record type of the versioned scope is immutable).
        IndexCase indexCase = lilyIndexerConf.getIndexCase(table.getTableName(), vtRecord.getRecord());

        if (indexCase == null) {
            // The record should not be indexed

            // The record might however have been previously indexed, therefore we need to perform a delete
            // (note that IndexAwareMQFeeder only sends events to us if either the current or old record
            //  state matched the inclusion rules)

            //commenting out because we don't want to delete a record that may have been indexed by another indexer

            //solrUpdateWriter.deleteByQuery("lily.id:" + ClientUtils.escapeQueryChars(vtRecord.getId().toString()));

            //if (log.isDebugEnabled()) {
            //    log.debug(String.format("Record %1$s: no index case found, record deleted from index.",
            //            vtRecord.getId()));
            //}

            // The data from this record might be denormalized into other index entries
            // After this we go to update denormalized data
        } else {
            final Set<SchemaId> vtagsToIndex;

            if (event.getType().equals(CREATE)) {
                // New record: just index everything
                vtagsToIndex = Sets.intersection(indexCase.getVersionTags(), vtRecord.getVTags().keySet());
                //vtagsToIndex = indexer.retainExistingVtagsOnly(indexCase.getVersionTags(), vtRecord);
                // After this we go to the indexing

            } else if (event.getRecordTypeChanged()) {
                // When the record type changes, the rules to index (= the IndexCase) change

                // Delete everything: we do not know the previous record type, so we do not know what
                // version tags were indexed, so we simply delete everything
                solrUpdateWriter.deleteByQuery("lily.id:" + ClientUtils.escapeQueryChars(vtRecord.getId().toString()));

                if (log.isDebugEnabled()) {
                    log.debug(String.format("Record %1$s: deleted existing entries from index (if present) " +
                            "because of record type change", vtRecord.getId()));
                }

                // Reindex all needed vtags
                vtagsToIndex = Sets.intersection(indexCase.getVersionTags(), vtRecord.getVTags().keySet());

                // After this we go to the indexing
            } else { // a normal update

                //
                // Handle changes to non-versioned fields
                //
                if (lilyIndexerConf.changesAffectIndex(vtRecord, Scope.NON_VERSIONED)) {
                    vtagsToIndex = Sets.intersection(indexCase.getVersionTags(), vtRecord.getVTags().keySet());
                    // After this we go to the treatment of changed vtag fields
                    if (log.isDebugEnabled()) {
                        log.debug(
                                String.format("Record %1$s: non-versioned fields changed, will reindex all vtags.",
                                        vtRecord.getId()));
                    }
                } else {
                    vtagsToIndex = new HashSet<SchemaId>();
                }

                //
                // Handle changes to versioned(-mutable) fields
                //
                // If there were non-versioned fields changed, then we already reindex all versions
                // so this can be skipped.
                //
                // In the case of newly created versions that should be indexed: this will often be
                // accompanied by corresponding changes to vtag fields, which are handled next, and in which case
                // it would work as well if this code would not be here.
                //
                if (vtagsToIndex.isEmpty() && (event.getVersionCreated() != -1 || event.getVersionUpdated() != -1)) {
                    if (lilyIndexerConf.changesAffectIndex(vtRecord, Scope.VERSIONED)
                            || lilyIndexerConf.changesAffectIndex(vtRecord, Scope.VERSIONED_MUTABLE)) {

                        long version =
                                event.getVersionCreated() != -1 ? event.getVersionCreated() : event.getVersionUpdated();
                        if (vtagsByVersion.containsKey(version)) {
                            Set<SchemaId> tmp = new HashSet<SchemaId>();
                            tmp.addAll(indexCase.getVersionTags());
                            tmp.retainAll(vtagsByVersion.get(version));
                            vtagsToIndex.addAll(tmp);

                            if (log.isDebugEnabled()) {
                                log.debug(String.format("Record %1$s: versioned(-mutable) fields changed, will " +
                                        "index for all tags of modified version %2$s that require indexing: %3$s",
                                        vtRecord.getId(), version, Joiner.on(",").join(vtagsToIndex)));
                            }
                        }
                    }
                }

                //
                // Handle changes to vtag fields themselves
                //
                Map<SchemaId, Long> vtags = vtRecord.getVTags();
                Set<SchemaId> changedVTagFields = vtRecord.getRecordEventHelper().getModifiedVTags();
                // Remove the vtags which are going to be reindexed anyway
                changedVTagFields.removeAll(vtagsToIndex);
                for (SchemaId vtag : changedVTagFields) {
                    if (vtags.containsKey(vtag) && indexCase.getVersionTags().contains(vtag)) {
                        if (log.isDebugEnabled()) {
                            log.debug(String.format("Record %1$s: will index for created or updated vtag %2$s",
                                    vtRecord.getId(), this.safeLoadTagName(vtag)));
                        }
                        vtagsToIndex.add(vtag);
                    } else {
                        // The vtag does not exist anymore on the document, or does not need to be indexed: delete from index
                        solrUpdateWriter.deleteById(LilyResultToSolrMapper.getIndexId(table.getTableName(),
                                vtRecord.getId(), vtag));
                        if (log.isDebugEnabled()) {
                            log.debug(String.format("Record %1$s: deleted from index for deleted vtag %2$s",
                                    vtRecord.getId(), this.safeLoadTagName(vtag)));
                        }
                    }
                }
            }


            //
            // Index
            //
            this.index(table, vtRecord, vtagsToIndex, solrUpdateWriter);
        }
    }

    private Map<Long, Set<SchemaId>> getVtagsByVersion(Set<SchemaId> vtagsToIndex, Map<SchemaId, Long> vtags) {
        Map<Long, Set<SchemaId>> result = new HashMap<Long, Set<SchemaId>>();

        for (SchemaId vtag : vtagsToIndex) {
            Long version = vtags.get(vtag);
            if (version != null) {
                Set<SchemaId> vtagsOfVersion = result.get(version);
                if (vtagsOfVersion == null) {
                    vtagsOfVersion = new HashSet<SchemaId>();
                    result.put(version, vtagsOfVersion);
                }
                vtagsOfVersion.add(vtag);
            }
        }

        return result;
    }

    private void index(LTable table, VTaggedRecord vtRecord, Set<SchemaId> vtagsToIndex, SolrUpdateWriter solrUpdateWriter) throws Exception {
        IdRecord idRecord = vtRecord.getRecord();
        Map<Long, Set<SchemaId>> vtagsToIndexByVersion = getVtagsByVersion(vtagsToIndex, vtRecord.getVTags());
        for (Map.Entry<Long, Set<SchemaId>> entry : vtagsToIndexByVersion.entrySet()) {

            IdRecord version = null;
            try {
                version = vtRecord.getIdRecord(entry.getKey());
            } catch (VersionNotFoundException e) {
                // ok
            } catch (RecordNotFoundException e) {
                // ok
            }

            if (version == null) {
                // If the version does not exist, we pro-actively delete it, though the IndexUpdater should
                // do this any way when it later receives a message about the delete.
                for (SchemaId vtag : entry.getValue()) {
                    solrUpdateWriter.deleteById(LilyResultToSolrMapper.getIndexId(table.getTableName(), vtRecord.getId(), vtag));
                }

                if (log.isDebugEnabled()) {
                    log.debug(String.format("Record %1$s, version %2$s: does not exist, deleted index" +
                            " entries for vtags %3$s", vtRecord.getId().toString(), entry.getKey(),
                            vtagSetToNameString(entry.getValue())));
                }
            } else {
                for (SchemaId vtag : entry.getValue()) {
                    SolrDocumentBuilder solrDocumentBuilder = new SolrDocumentBuilder(repository, lilyIndexerConf.getRecordFilter(),
                            lilyIndexerConf.getSystemFields(), valueEvaluator, table.getTableName(), version,
                            getIndexId(table.getTableName(), vtRecord.getId(), vtag), vtag, entry.getKey());

                    lilyIndexerConf.getIndexFields().collectIndexUpdate(solrDocumentBuilder);

                    if (!lilyIndexerConf.getDynamicFields().isEmpty()) {
                        for (Map.Entry<SchemaId, Object> field : idRecord.getFieldsById().entrySet()) {
                            FieldType fieldType = repository.getTypeManager().getFieldTypeById(field.getKey());
                            for (DynamicIndexField dynField : lilyIndexerConf.getDynamicFields()) {
                                DynamicIndexField.DynamicIndexFieldMatch match = dynField.matches(fieldType);
                                if (match.match) {
                                    String fieldName = evalName(dynField, match, fieldType);

                                    List<String> values = valueEvaluator.format(table.getTableName(), vtRecord.getRecord(), fieldType, dynField.extractContext(),
                                            dynField.getFormatter(), repository);

                                    solrDocumentBuilder.addField(fieldName, values);

                                    if (!dynField.getContinue()) {
                                        // stop on first match, unless continue attribute is true
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    if (solrDocumentBuilder.isEmptyDocument()) {
                        // No single field was added to the Solr document.
                        // In this case we do not add it to the index.
                        // Besides being somewhat logical, it should also be noted that if a record would not contain
                        // any (modified) fields that serve as input to indexFields, we would never have arrived here
                        // anyway. It is only because some fields potentially would resolve to a value (potentially:
                        // because with deref-expressions we are never sure) that we did.

                        // There can be a previous entry in the index which we should try to delete
                        solrUpdateWriter.deleteById(LilyResultToSolrMapper.getIndexId(table.getTableName(),
                                vtRecord.getId(), vtag));
                        //metrics.deletesById.inc();

                        if (log.isDebugEnabled()) {
                            log.debug(String.format("Record %1$s, vtag %2$s: no index fields produced output, " +
                                    "removed from index if present", vtRecord.getId(), safeLoadTagName(vtag)));
                        }

                        processDependencies(table.getTableName(), idRecord, vtag, solrDocumentBuilder);
                    } else {
                        SolrInputDocument solrDoc = solrDocumentBuilder.build();

                        processDependencies(table.getTableName(), idRecord, vtag, solrDocumentBuilder);
                        solrUpdateWriter.add(solrDoc);
                        //log.debug("index response " + solrShardMgr.getSolrClient(record.getId()).add(solrDoc).toString());
                        //metrics.adds.inc();

                        if (log.isDebugEnabled()) {
                            log.debug(String.format("Record %1$s, vtag %2$s: indexed, doc = %3$s", vtRecord.getId(),
                                    safeLoadTagName(vtag), solrDoc));
                        }
                    }
                }
            }
        }
    }

    protected String vtagSetToNameString(Set<SchemaId> vtags) {
        StringBuilder builder = new StringBuilder();
        for (SchemaId vtag : vtags) {
            if (builder.length() > 0) {
                builder.append(", ");
            }
            builder.append(safeLoadTagName(vtag));
        }
        return builder.toString();
    }

    private static class StaticFieldTypeFinder implements Predicate<MappingNode> {
        boolean foundRelevant = false;
        final FieldType fieldType;
        private StaticFieldTypeFinder(FieldType fieldType) {
            this.fieldType = fieldType;
        }

        @Override
        public boolean apply(MappingNode mappingNode) {
            if (mappingNode instanceof IndexField) {
                IndexField indexField = (IndexField)mappingNode;
                if (fieldType.equals(indexField.getValue().getTargetFieldType())) {
                    foundRelevant = true;
                }
            }
            return !foundRelevant;
        }
    }

    /**
     * Lookup name of field type, for use in debug logs. Beware, this might be slow.
     */
    protected String safeLoadTagName(SchemaId fieldTypeId) {
        if (fieldTypeId == null) {
            return "null";
        }

        try {
            return this.repository.getTypeManager().getFieldTypeById(fieldTypeId).getName().getName();
        } catch (Throwable t) {
            return "failed to load name";
        }
    }

    protected void setRepositoryManager(RepositoryManager repositoryManager) {
        this.repositoryManager = repositoryManager;
    }

    protected void setRepositoryName(String repositoryName) {
        this.repositoryName = repositoryName;
    }

    protected void setIndexName(String indexName) {
        this.indexName = indexName;
        subscriptionId = "Indexer_" + indexName;
    }
}
