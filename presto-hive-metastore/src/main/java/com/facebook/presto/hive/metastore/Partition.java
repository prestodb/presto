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
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class Partition
{
    private final String databaseName;
    private final String tableName;
    private final List<String> values;
    private final Storage storage;
    private final List<Column> columns;
    private final Map<String, String> parameters;
    private final Optional<Integer> partitionVersion;
    private final boolean eligibleToIgnore;
    private final boolean sealedPartition;

    @JsonCreator
    public Partition(
            @JsonProperty("databaseName") String databaseName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("values") List<String> values,
            @JsonProperty("storage") Storage storage,
            @JsonProperty("columns") List<Column> columns,
            @JsonProperty("parameters") Map<String, String> parameters,
            @JsonProperty("partitionVersion") Optional<Integer> partitionVersion,
            @JsonProperty("eligibleToIgnore") boolean eligibleToIgnore,
            @JsonProperty("sealedPartition") boolean sealedPartition)
    {
        this.databaseName = requireNonNull(databaseName, "databaseName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.values = ImmutableList.copyOf(requireNonNull(values, "values is null"));
        this.storage = requireNonNull(storage, "storage is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.parameters = ImmutableMap.copyOf(requireNonNull(parameters, "parameters is null"));
        this.partitionVersion = requireNonNull(partitionVersion, "partitionVersion is null");
        this.eligibleToIgnore = eligibleToIgnore;
        this.sealedPartition = sealedPartition;
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
    public List<String> getValues()
    {
        return values;
    }

    @JsonProperty
    public Storage getStorage()
    {
        return storage;
    }

    @JsonProperty
    public List<Column> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Map<String, String> getParameters()
    {
        return parameters;
    }

    @JsonProperty
    public Optional<Integer> getPartitionVersion()
    {
        return partitionVersion;
    }

    @JsonProperty
    public boolean isEligibleToIgnore()
    {
        return eligibleToIgnore;
    }

    @JsonProperty
    public boolean isSealedPartition()
    {
        return sealedPartition;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("databaseName", databaseName)
                .add("tableName", tableName)
                .add("values", values)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Partition partition = (Partition) o;
        return Objects.equals(databaseName, partition.databaseName) &&
                Objects.equals(tableName, partition.tableName) &&
                Objects.equals(values, partition.values) &&
                Objects.equals(storage, partition.storage) &&
                Objects.equals(columns, partition.columns) &&
                Objects.equals(parameters, partition.parameters) &&
                Objects.equals(partitionVersion, partition.partitionVersion) &&
                Objects.equals(eligibleToIgnore, partition.eligibleToIgnore) &&
                Objects.equals(sealedPartition, partition.sealedPartition);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(databaseName, tableName, values, storage, columns, parameters, partitionVersion, eligibleToIgnore, sealedPartition);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(Partition partition)
    {
        return new Builder(partition);
    }

    public static class Builder
    {
        private final Storage.Builder storageBuilder;
        private String databaseName;
        private String tableName;
        private List<String> values;
        private List<Column> columns;
        private Map<String, String> parameters = ImmutableMap.of();
        private Optional<Integer> partitionVersion = Optional.empty();
        private boolean isEligibleToIgnore;
        private boolean isSealedPartition = true;

        private Builder()
        {
            this.storageBuilder = Storage.builder();
        }

        private Builder(Partition partition)
        {
            this.storageBuilder = Storage.builder(partition.getStorage());
            this.databaseName = partition.getDatabaseName();
            this.tableName = partition.getTableName();
            this.values = partition.getValues();
            this.columns = partition.getColumns();
            this.parameters = partition.getParameters();
            this.partitionVersion = partition.getPartitionVersion();
            this.isEligibleToIgnore = partition.isEligibleToIgnore();
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

        public Builder setValues(List<String> values)
        {
            this.values = values;
            return this;
        }

        public Storage.Builder getStorageBuilder()
        {
            return storageBuilder;
        }

        public Builder withStorage(Consumer<Storage.Builder> consumer)
        {
            consumer.accept(storageBuilder);
            return this;
        }

        public Builder setColumns(List<Column> columns)
        {
            this.columns = columns;
            return this;
        }

        public Builder setParameters(Map<String, String> parameters)
        {
            this.parameters = parameters;
            return this;
        }

        public Builder setPartitionVersion(int partitionVersion)
        {
            this.partitionVersion = Optional.of(partitionVersion);
            return this;
        }

        public Builder setEligibleToIgnore(boolean isEligibleToIgnore)
        {
            this.isEligibleToIgnore = isEligibleToIgnore;
            return this;
        }

        public Builder setSealedPartition(boolean isSealedPartition)
        {
            this.isSealedPartition = isSealedPartition;
            return this;
        }

        public Partition build()
        {
            return new Partition(databaseName, tableName, values, storageBuilder.build(), columns, parameters, partitionVersion, isEligibleToIgnore, isSealedPartition);
        }
    }
}
