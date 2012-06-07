/*
 * Copyright 2012 NGDATA nv
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
package org.lilyproject.tools.import_.json.filters;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.repository.api.filter.RecordVariantFilter;
import org.lilyproject.tools.import_.json.JsonFormatException;
import org.lilyproject.tools.import_.json.Namespaces;
import org.lilyproject.util.json.JsonFormat;
import org.lilyproject.util.json.JsonUtil;

public class RecordVariantFilterJson implements RecordFilterJsonConverter<RecordVariantFilter> {
    @Override
    public boolean supports(String typeName) {
        return typeName.equals(RecordVariantFilter.class.getName());
    }

    @Override
    public ObjectNode toJson(RecordVariantFilter filter, Namespaces namespaces, Repository repository,
                             RecordFilterJsonConverter<RecordFilter> converter)
            throws RepositoryException, InterruptedException {

        ObjectNode node = JsonFormat.OBJECT_MAPPER.createObjectNode();

        if (filter.getRecordId() != null) {
            node.put("recordId", filter.getRecordId().toString());
        }

        return node;
    }

    @Override
    public RecordVariantFilter fromJson(JsonNode node, Namespaces namespaces, Repository repository,
                                        RecordFilterJsonConverter<RecordFilter> converter)
            throws JsonFormatException, RepositoryException, InterruptedException {

        String recordId = JsonUtil.getString(node, "recordId", null);
        if (recordId != null)
            return new RecordVariantFilter(repository.getIdGenerator().fromString(recordId));
        else
            throw new IllegalStateException("expected non null recordId in json input");
    }
}
