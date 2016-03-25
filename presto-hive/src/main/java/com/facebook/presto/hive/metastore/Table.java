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
package com.facebook.presto.hive.metastore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class Table
{
    private final String databaseName;
    private final String tableName;
    private final String owner;
    private final String tableType; // This is not an enum because some Hive implementations define additional table types.
    private final List<Column> dataColumns;
    private final List<Column> partitionColumns;
    private final Storage storage;
    private final Map<String, String> parameters;
    private final Optional<String> viewOriginalText;
    private final Optional<String> viewExpandedText;

    @JsonCreator
    public Table(
            @JsonProperty("databaseName") String databaseName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("owner") String owner,
            @JsonProperty("tableType") String tableType,
            @JsonProperty("storage") Storage storage,
            @JsonProperty("dataColumns") List<Column> dataColumns,
            @JsonProperty("partitionColumns") List<Column> partitionColumns,
            @JsonProperty("parameters") Map<String, String> parameters,
            @JsonProperty("viewOriginalText") Optional<String> viewOriginalText,
            @JsonProperty("viewExpandedText") Optional<String> viewExpandedText)
    {
        this.databaseName = requireNonNull(databaseName, "databaseName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.owner = requireNonNull(owner, "owner is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        this.storage = requireNonNull(storage, "storage is null");
        this.dataColumns = ImmutableList.copyOf(requireNonNull(dataColumns, "dataColumns is null"));
        this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
        this.parameters = ImmutableMap.copyOf(requireNonNull(parameters, "parameters is null"));
        this.viewOriginalText = requireNonNull(viewOriginalText, "viewOriginalText is null");
        this.viewExpandedText = requireNonNull(viewExpandedText, "viewExpandedText is null");
    }

    @JsonProperty
    public String getDatabaseName()
    {
        return databaseName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getOwner()
    {
        return owner;
    }

    @JsonProperty
    public String getTableType()
    {
        return tableType;
    }

    @JsonProperty
    public List<Column> getDataColumns()
    {
        return dataColumns;
    }

    @JsonProperty
    public List<Column> getPartitionColumns()
    {
        return partitionColumns;
    }

    @JsonProperty
    public Storage getStorage()
    {
        return storage;
    }

    @JsonProperty
    public Map<String, String> getParameters()
    {
        return parameters;
    }

    @JsonProperty
    public Optional<String> getViewOriginalText()
    {
        return viewOriginalText;
    }

    @JsonProperty
    public Optional<String> getViewExpandedText()
    {
        return viewExpandedText;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("databaseName", databaseName)
                .add("tableName", tableName)
                .toString();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(Table table)
    {
        return new Builder(table);
    }

    public static class Builder
    {
        private final Storage.Builder storageBuilder;
        private String databaseName;
        private String tableName;
        private String owner;
        private String tableType;
        private List<Column> dataColumns;
        private List<Column> partitionColumns;
        private Map<String, String> parameters;
        private Optional<String> viewOriginalText = Optional.empty();
        private Optional<String> viewExpandedText = Optional.empty();

        private Builder()
        {
            storageBuilder = Storage.builder();
        }

        private Builder(Table table)
        {
            databaseName = table.databaseName;
            tableName = table.tableName;
            owner = table.owner;
            tableType = table.tableType;
            storageBuilder = Storage.builder(table.getStorage());
            dataColumns = table.dataColumns;
            partitionColumns = table.partitionColumns;
            parameters = table.parameters;
            viewOriginalText = table.viewOriginalText;
            viewExpandedText = table.viewExpandedText;
        }

        public Builder setDatabaseName(String databaseName)
        {
            this.databaseName = databaseName;
            return this;
        }

        public Builder setTableName(String tableName)
        {
            this.tableName = tableName;
            return this;
        }

        public Builder setOwner(String owner)
        {
            this.owner = owner;
            return this;
        }

        public Builder setTableType(String tableType)
        {
            this.tableType = tableType;
            return this;
        }

        public Storage.Builder getStorageBuilder()
        {
            return storageBuilder;
        }

        public Builder setDataColumns(List<Column> dataColumns)
        {
            this.dataColumns = dataColumns;
            return this;
        }

        public Builder setPartitionColumns(List<Column> partitionColumns)
        {
            this.partitionColumns = partitionColumns;
            return this;
        }

        public Builder setParameters(Map<String, String> parameters)
        {
            this.parameters = parameters;
            return this;
        }

        public Builder setViewOriginalText(Optional<String> viewOriginalText)
        {
            this.viewOriginalText = viewOriginalText;
            return this;
        }

        public Builder setViewExpandedText(Optional<String> viewExpandedText)
        {
            this.viewExpandedText = viewExpandedText;
            return this;
        }

        public Table build()
        {
            return new Table(
                    databaseName,
                    tableName,
                    owner,
                    tableType,
                    storageBuilder.build(),
                    dataColumns,
                    partitionColumns,
                    parameters,
                    viewOriginalText,
                    viewExpandedText);
        }
    }
}
