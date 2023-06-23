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

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.makeStorageDescriptor;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class Partition
        extends org.apache.hadoop.hive.metastore.api.Partition
{
    private final Storage storage;
    private final List<Column> columns;
    private final Optional<Long> partitionVersion;
    private final boolean eligibleToIgnore;
    private final boolean sealedPartition;
    private final long lastDataCommitTime;

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
            @JsonProperty("lastAccessTime") int lastAccessTime)
    {
        super(values, databaseName, tableName, createTime, lastAccessTime, makeStorageDescriptor(tableName, columns, storage, new HiveColumnConverter()), parameters);
        this.storage = requireNonNull(storage, "storage is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.partitionVersion = requireNonNull(partitionVersion, "partitionVersion is null");
        this.eligibleToIgnore = eligibleToIgnore;
        this.sealedPartition = sealedPartition;
        this.lastDataCommitTime = lastDataCommitTime;
    }

    @JsonProperty
    public String getDatabaseName()
    {
        return super.getDbName();
    }

    @JsonProperty
    public String getTableName()
    {
        return super.getTableName();
    }

    @JsonIgnore
    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(this.getDatabaseName(), this.getTableName());
    }

    @JsonProperty
    public List<String> getValues()
    {
        return super.getValues();
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
        return super.getParameters();
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
        return super.getCreateTime();
    }

    @JsonProperty
    public long getLastDataCommitTime()
    {
        return lastDataCommitTime;
    }

    @JsonProperty
    public int getLastAccessTime()
    {
        return super.getLastAccessTime();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("databaseName", this.getDatabaseName())
                .add("tableName", this.getTableName())
                .add("values", this.getValues())
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
        return Objects.equals(this.getDbName(), partition.getDatabaseName()) &&
                Objects.equals(this.getTableName(), partition.getTableName()) &&
                Objects.equals(this.getValues(), partition.getValues()) &&
                Objects.equals(storage, partition.storage) &&
                Objects.equals(columns, partition.columns) &&
                Objects.equals(this.getParameters(), partition.getParameters()) &&
                Objects.equals(partitionVersion, partition.partitionVersion) &&
                eligibleToIgnore == partition.eligibleToIgnore &&
                sealedPartition == partition.sealedPartition &&
                this.getCreateTime() == partition.getCreateTime() &&
                lastDataCommitTime == partition.getLastDataCommitTime() &&
                this.getLastAccessTime() == partition.getLastAccessTime();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), storage, columns, partitionVersion, eligibleToIgnore, sealedPartition, lastDataCommitTime);
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
        private Optional<Long> partitionVersion = Optional.empty();
        private boolean isEligibleToIgnore;
        private boolean isSealedPartition = true;
        private int createTime;
        private long lastDataCommitTime;
        private int lastAccessTime;

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
            this.createTime = partition.getCreateTime();
            this.lastDataCommitTime = partition.getLastDataCommitTime();
            this.lastAccessTime = partition.getLastAccessTime();
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

        public Builder setLastAccessTime(int lastAccessTime)
        {
            this.lastAccessTime = lastAccessTime;
            return this;
        }

        public Partition build()
        {
            return new Partition(
                    databaseName,
                    tableName,
                    values,
                    storageBuilder.build(),
                    columns,
                    parameters,
                    partitionVersion,
                    isEligibleToIgnore,
                    isSealedPartition,
                    createTime,
                    lastDataCommitTime,
                    lastAccessTime);
        }
    }
}
