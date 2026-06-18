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

import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;

import java.util.Arrays;
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
    private final Optional<String> catalogName;
    private final String databaseName;
    private final String tableName;
    private final List<String> values;
    private final Storage storage;
    private final List<Column> columns;
    private final Map<String, String> parameters;
    private final Optional<Long> partitionVersion;
    private final boolean eligibleToIgnore;
    private final boolean sealedPartition;
    private final int createTime;
    private final long lastDataCommitTime;
    private final Optional<byte[]> rowIdPartitionComponent;

    @JsonCreator
    public Partition(
            @JsonProperty("databaseName") String databaseName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("values") List<String> values,
            @JsonProperty("storage") Storage storage,
            @JsonProperty("columns") List<Column> columns,
            @JsonProperty("parameters") Map<String, String> parameters,
            @JsonProperty("partitionVersion") Optional<Long> partitionVersion,
            @JsonProperty("eligibleToIgnore") boolean eligibleToIgnore,
            @JsonProperty("sealedPartition") boolean sealedPartition,
            @JsonProperty("createTime") int createTime,
            @JsonProperty("lastDataCommitTime") long lastDataCommitTime,
            @JsonProperty("rowIdPartitionComponent") Optional<byte[]> rowIdPartitionComponent)
    {
        this(
                Optional.empty(),
                databaseName,
                tableName,
                values,
                storage,
                columns,
                parameters,
                partitionVersion,
                eligibleToIgnore,
                sealedPartition,
                createTime,
                lastDataCommitTime,
                rowIdPartitionComponent);
    }
    public Partition(
            Optional<String> catalogName,
            String databaseName,
            String tableName,
            List<String> values,
            Storage storage,
            List<Column> columns,
            Map<String, String> parameters,
            Optional<Long> partitionVersion,
            boolean eligibleToIgnore,
            boolean sealedPartition,
            int createTime,
            long lastDataCommitTime,
            Optional<byte[]> rowIdPartitionComponent)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.databaseName = requireNonNull(databaseName, "databaseName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.values = ImmutableList.copyOf(requireNonNull(values, "values is null"));
        this.storage = requireNonNull(storage, "storage is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.parameters = ImmutableMap.copyOf(requireNonNull(parameters, "parameters is null"));
        this.partitionVersion = requireNonNull(partitionVersion, "partitionVersion is null");
        this.eligibleToIgnore = eligibleToIgnore;
        this.sealedPartition = sealedPartition;
        this.createTime = createTime;
        this.lastDataCommitTime = lastDataCommitTime;
        this.rowIdPartitionComponent = requireNonNull(rowIdPartitionComponent);
    }

    @JsonProperty
    public Optional<String> getCatalogName()
    {
        return catalogName;
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

    @JsonIgnore
    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(databaseName, tableName);
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
    public Optional<Long> getPartitionVersion()
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

    @JsonProperty
    public int getCreateTime()
    {
        return createTime;
    }

    @JsonProperty
    public long getLastDataCommitTime()
    {
        return lastDataCommitTime;
    }

    /**
     * A unique identifier for a specific version of a
     * specific partition in a specific table.
     */
    @JsonProperty
    public Optional<byte[]> getRowIdPartitionComponent()
    {
        if (rowIdPartitionComponent.isPresent()) {
            byte[] copy = Arrays.copyOf(rowIdPartitionComponent.get(), rowIdPartitionComponent.get().length);
            return Optional.of(copy);
        }
        return Optional.empty();
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
        return Objects.equals(catalogName, partition.catalogName) &&
                Objects.equals(databaseName, partition.databaseName) &&
                Objects.equals(tableName, partition.tableName) &&
                Objects.equals(values, partition.values) &&
                Objects.equals(storage, partition.storage) &&
                Objects.equals(columns, partition.columns) &&
                Objects.equals(parameters, partition.parameters) &&
                Objects.equals(partitionVersion, partition.partitionVersion) &&
                Objects.equals(eligibleToIgnore, partition.eligibleToIgnore) &&
                Objects.equals(sealedPartition, partition.sealedPartition) &&
                Objects.equals(createTime, partition.getCreateTime()) &&
                Objects.equals(lastDataCommitTime, partition.getLastDataCommitTime());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, databaseName, tableName, values, storage, columns, parameters, partitionVersion, eligibleToIgnore, sealedPartition, createTime, lastDataCommitTime);
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
        private Optional<String> catalogName;
        private String databaseName;
        private String tableName;
        private List<String> values;
        private List<Column> columns;
        private Map<String, String> parameters = ImmutableMap.of();
        private Optional<Long> partitionVersion = Optional.empty();
        private boolean isEligibleToIgnore;
        private boolean isSealedPartition = true;
        private int createTime;
        private long lastDataCommitTime;
        private Optional<byte[]> rowIdPartitionComponent = Optional.empty();

        private Builder()
        {
            this.storageBuilder = Storage.builder();
        }

        private Builder(Partition partition)
        {
            this.storageBuilder = Storage.builder(partition.getStorage());
            this.catalogName = partition.getCatalogName();
            this.databaseName = partition.getDatabaseName();
            this.tableName = partition.getTableName();
            this.values = partition.getValues();
            this.columns = partition.getColumns();
            this.parameters = partition.getParameters();
            this.partitionVersion = partition.getPartitionVersion();
            this.isEligibleToIgnore = partition.isEligibleToIgnore();
            this.createTime = partition.getCreateTime();
            this.lastDataCommitTime = partition.getLastDataCommitTime();
            this.rowIdPartitionComponent = partition.getRowIdPartitionComponent();
        }

        public Builder setCatalogName(Optional<String> catalogName)
        {
            this.catalogName = catalogName;
            return this;
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

        public Builder setPartitionVersion(long partitionVersion)
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

        public Builder setCreateTime(int createTime)
        {
            this.createTime = createTime;
            return this;
        }

        public Builder setLastDataCommitTime(long lastDataCommitTime)
        {
            this.lastDataCommitTime = lastDataCommitTime;
            return this;
        }

        /**
         * Sets a unique identifier for a specific version of a
         * specific partition in a specific table. This value will normally be
         * supplied by Metastore, along with other metadata.
         */
        public Builder setRowIdPartitionComponent(Optional<byte[]> rowIdPartitionComponent)
        {
            this.rowIdPartitionComponent = rowIdPartitionComponent;
            return this;
        }

        public Partition build()
        {
            return new Partition(catalogName, databaseName, tableName, values, storageBuilder.build(), columns, parameters, partitionVersion, isEligibleToIgnore, isSealedPartition, createTime, lastDataCommitTime, rowIdPartitionComponent);
        }
    }
}
