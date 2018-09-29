/*
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
package com.facebook.presto.kafkastream;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class KafkaTable
{
    private final String name;
    private final List<KafkaColumn> columns;
    private final List<ColumnMetadata> columnsMetadata;
    private final String source;
    private final String entityName;
    private final String keyDeserializer;
    private final String valueDeserializer;
    private final String groupId;
    private final String applicationId;
    private final boolean enableAutoCommit;
    private final String storeName;
    private final Set<Set<String>> indexableKeys;

    @JsonCreator
    public KafkaTable(
            @JsonProperty("name") String name,
            @JsonProperty("columns") List<KafkaColumn> columns,
            @JsonProperty("source") String source,
            @JsonProperty("entityName") String entityName,
            @JsonProperty("sourceType") String sourceType,
            @JsonProperty("keyDeserializer") String keyDeserializer,
            @JsonProperty("valueDeserializer") String valueDeserializer,
            @JsonProperty("groupId") String groupId,
            @JsonProperty("applicationId") String applicationId,
            @JsonProperty("enableAutoCommit") boolean enableAutoCommit,
            @JsonProperty("storeName") String storeName,
            @JsonProperty("indexableKeys") Set<Set<String>> indexableKeys)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = requireNonNull(name, "name is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.source = requireNonNull(source, "source is null");
        this.entityName = requireNonNull(entityName, "entity name is null");
        this.keyDeserializer = requireNonNull(keyDeserializer, "keyDeserializer is null");
        this.valueDeserializer =
                requireNonNull(valueDeserializer, "valueDeserializer is null");
        this.groupId = requireNonNull(groupId, "groupId is null");
        this.applicationId = requireNonNull(applicationId, "application id is null");
        this.enableAutoCommit =
                requireNonNull(enableAutoCommit, "enableAutoCommit is null");
        this.storeName = requireNonNull(storeName, "storeName is null");

        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        for (KafkaColumn column : this.columns) {
            columnsMetadata.add(new KafkaColumnMetadata(
                    column.getName(),
                    column.getType(),
                    column.getJsonPath()));
        }
        this.columnsMetadata = columnsMetadata.build();
        this.indexableKeys = indexableKeys;
    }

    public boolean containsIndexableColumns(Set<ColumnHandle> indexableColumns)
    {
        Set<String> keyColumns =
                indexableColumns.stream().map(KafkaColumnHandle.class::cast).map(
                        KafkaColumnHandle::getColumnName).collect(toImmutableSet());
        return indexableKeys.contains(keyColumns);
    }
    @JsonProperty
    public String getName()
    {
        return name;
    }
    @JsonProperty
    public List<KafkaColumn> getColumns()
    {
        return columns;
    }
    @JsonProperty
    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columnsMetadata;
    }
    @JsonProperty
    public String getSource()
    {
        return source;
    }
    @JsonProperty
    public String getEntityName()
    {
        return entityName;
    }
    @JsonProperty
    public String getKeyDeserializer()
    {
        return keyDeserializer;
    }
    @JsonProperty
    public String getValueDeserializer()
    {
        return valueDeserializer;
    }
    @JsonProperty
    public String getGroupId()
    {
        return groupId;
    }
    @JsonProperty
    public String getApplicationId()
    {
        return applicationId;
    }
    @JsonProperty
    public boolean isEnableAutoCommit()
    {
        return enableAutoCommit;
    }
    @JsonProperty
    public String getStoreName()
    {
        return storeName;
    }
    public Set<Set<String>> getIndexableKeys()
    {
        return indexableKeys;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((applicationId == null) ? 0 : applicationId.hashCode());
        result = prime * result + ((columns == null) ? 0 : columns.hashCode());
        result = prime * result + ((columnsMetadata == null) ? 0 : columnsMetadata.hashCode());
        result = prime * result + (enableAutoCommit ? 1231 : 1237);
        result = prime * result + ((entityName == null) ? 0 : entityName.hashCode());
        result = prime * result + ((groupId == null) ? 0 : groupId.hashCode());
        result = prime * result + ((indexableKeys == null) ? 0 : indexableKeys.hashCode());
        result = prime * result + ((keyDeserializer == null) ? 0 : keyDeserializer.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((source == null) ? 0 : source.hashCode());
        result = prime * result + ((storeName == null) ? 0 : storeName.hashCode());
        result = prime * result + ((valueDeserializer == null) ? 0 : valueDeserializer.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        KafkaTable other = (KafkaTable) obj;
        if (applicationId == null) {
            if (other.applicationId != null) {
                return false;
            }
        }
        else if (!applicationId.equals(other.applicationId)) {
            return false;
        }
        if (columns == null) {
            if (other.columns != null) {
                return false;
            }
        }
        else if (!columns.equals(other.columns)) {
            return false;
        }
        if (columnsMetadata == null) {
            if (other.columnsMetadata != null) {
                return false;
            }
        }
        else if (!columnsMetadata.equals(other.columnsMetadata)) {
            return false;
        }
        if (enableAutoCommit != other.enableAutoCommit) {
            return false;
        }
        if (entityName == null) {
            if (other.entityName != null) {
                return false;
            }
        }
        else if (!entityName.equals(other.entityName)) {
            return false;
        }
        if (groupId == null) {
            if (other.groupId != null) {
                return false;
            }
        }
        else if (!groupId.equals(other.groupId)) {
            return false;
        }
        if (indexableKeys == null) {
            if (other.indexableKeys != null) {
                return false;
            }
        }
        else if (!indexableKeys.equals(other.indexableKeys)) {
            return false;
        }
        if (keyDeserializer == null) {
            if (other.keyDeserializer != null) {
                return false;
            }
        }
        else if (!keyDeserializer.equals(other.keyDeserializer)) {
            return false;
        }
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        }
        else if (!name.equals(other.name)) {
            return false;
        }
        if (source == null) {
            if (other.source != null) {
                return false;
            }
        }
        else if (!source.equals(other.source)) {
            return false;
        }
        if (storeName == null) {
            if (other.storeName != null) {
                return false;
            }
        }
        else if (!storeName.equals(other.storeName)) {
            return false;
        }
        if (valueDeserializer == null) {
            if (other.valueDeserializer != null) {
                return false;
            }
        }
        else if (!valueDeserializer.equals(other.valueDeserializer)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        return "KafkaTable [name=" + name + ", columns=" + columns + ", columnsMetadata=" + columnsMetadata
                + ", source=" + source + ", entityName=" + entityName + ", keyDeserializer=" + keyDeserializer
                + ", valueDeserializer=" + valueDeserializer + ", groupId=" + groupId + ", applicationId="
                + applicationId + ", enableAutoCommit=" + enableAutoCommit + ", storeName=" + storeName
                + ", indexableKeys=" + indexableKeys + "]";
    }
}
