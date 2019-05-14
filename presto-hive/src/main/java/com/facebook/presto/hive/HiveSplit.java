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
package com.facebook.presto.hive;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.RowExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class HiveSplit
        implements ConnectorSplit
{
    private final String path;
    private final long start;
    private final long length;
    private final long fileSize;
    private final Properties schema;
    private final List<HivePartitionKey> partitionKeys;
    private final List<HostAddress> addresses;
    private final String database;
    private final String table;
    private final String partitionName;
    private final TupleDomain<Subfield> domainPredicate;
    private final RowExpression remainingPredicate;
    private final Map<String, HiveColumnHandle> predicateColumns;
    private final OptionalInt readBucketNumber;
    private final OptionalInt tableBucketNumber;
    private final boolean forceLocalScheduling;
    private final Map<Integer, HiveType> columnCoercions; // key: hiveColumnIndex
    private final Optional<BucketConversion> bucketConversion;
    private final boolean s3SelectPushdownEnabled;

    @JsonCreator
    public HiveSplit(
            @JsonProperty("database") String database,
            @JsonProperty("table") String table,
            @JsonProperty("partitionName") String partitionName,
            @JsonProperty("path") String path,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("fileSize") long fileSize,
            @JsonProperty("schema") Properties schema,
            @JsonProperty("partitionKeys") List<HivePartitionKey> partitionKeys,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("readBucketNumber") OptionalInt readBucketNumber,
            @JsonProperty("tableBucketNumber") OptionalInt tableBucketNumber,
            @JsonProperty("forceLocalScheduling") boolean forceLocalScheduling,
            @JsonProperty("domainPredicate") TupleDomain<Subfield> domainPredicate,
            @JsonProperty("remainingPredicate") RowExpression remainingPredicate,
            @JsonProperty("predicateColumns") Map<String, HiveColumnHandle> predicateColumns,
            @JsonProperty("columnCoercions") Map<Integer, HiveType> columnCoercions,
            @JsonProperty("bucketConversion") Optional<BucketConversion> bucketConversion,
            @JsonProperty("s3SelectPushdownEnabled") boolean s3SelectPushdownEnabled)
    {
        checkArgument(start >= 0, "start must be positive");
        checkArgument(length >= 0, "length must be positive");
        checkArgument(fileSize >= 0, "fileSize must be positive");
        requireNonNull(database, "database is null");
        requireNonNull(table, "table is null");
        requireNonNull(partitionName, "partitionName is null");
        requireNonNull(path, "path is null");
        requireNonNull(schema, "schema is null");
        requireNonNull(partitionKeys, "partitionKeys is null");
        requireNonNull(addresses, "addresses is null");
        requireNonNull(readBucketNumber, "readBucketNumber is null");
        requireNonNull(tableBucketNumber, "tableBucketNumber is null");
        requireNonNull(columnCoercions, "columnCoercions is null");
        requireNonNull(bucketConversion, "bucketConversion is null");

        this.database = database;
        this.table = table;
        this.partitionName = partitionName;
        this.path = path;
        this.start = start;
        this.length = length;
        this.fileSize = fileSize;
        this.schema = schema;
        this.partitionKeys = ImmutableList.copyOf(partitionKeys);
        this.addresses = ImmutableList.copyOf(addresses);
        this.readBucketNumber = readBucketNumber;
        this.tableBucketNumber = tableBucketNumber;
        this.forceLocalScheduling = forceLocalScheduling;
        this.domainPredicate = requireNonNull(domainPredicate, "domainPredicate is null");
        this.remainingPredicate = requireNonNull(remainingPredicate, "remainingPredicate is null");
        this.predicateColumns = requireNonNull(predicateColumns, "predicateColumns is null");
        this.columnCoercions = columnCoercions;
        this.bucketConversion = bucketConversion;
        this.s3SelectPushdownEnabled = s3SelectPushdownEnabled;
    }

    @JsonProperty
    public String getDatabase()
    {
        return database;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public String getPartitionName()
    {
        return partitionName;
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public long getStart()
    {
        return start;
    }

    @JsonProperty
    public long getLength()
    {
        return length;
    }

    @JsonProperty
    public long getFileSize()
    {
        return fileSize;
    }

    @JsonProperty
    public Properties getSchema()
    {
        return schema;
    }

    @JsonProperty
    public List<HivePartitionKey> getPartitionKeys()
    {
        return partitionKeys;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @JsonProperty
    public OptionalInt getReadBucketNumber()
    {
        return readBucketNumber;
    }

    @JsonProperty
    public OptionalInt getTableBucketNumber()
    {
        return tableBucketNumber;
    }

    @JsonProperty
    public TupleDomain<Subfield> getDomainPredicate()
    {
        return domainPredicate;
    }

    @JsonProperty
    public RowExpression getRemainingPredicate()
    {
        return remainingPredicate;
    }

    @JsonProperty
    public Map<String, HiveColumnHandle> getPredicateColumns()
    {
        return predicateColumns;
    }

    @JsonProperty
    public boolean isForceLocalScheduling()
    {
        return forceLocalScheduling;
    }

    @JsonProperty
    public Map<Integer, HiveType> getColumnCoercions()
    {
        return columnCoercions;
    }

    @JsonProperty
    public Optional<BucketConversion> getBucketConversion()
    {
        return bucketConversion;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return !forceLocalScheduling;
    }

    @JsonProperty
    public boolean isS3SelectPushdownEnabled()
    {
        return s3SelectPushdownEnabled;
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("path", path)
                .put("start", start)
                .put("length", length)
                .put("fileSize", fileSize)
                .put("hosts", addresses)
                .put("database", database)
                .put("table", table)
                .put("forceLocalScheduling", forceLocalScheduling)
                .put("partitionName", partitionName)
                .put("s3SelectPushdownEnabled", s3SelectPushdownEnabled)
                .build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(path)
                .addValue(start)
                .addValue(length)
                .addValue(fileSize)
                .addValue(domainPredicate)
                .addValue(remainingPredicate)
                .addValue(s3SelectPushdownEnabled)
                .toString();
    }

    public static class BucketConversion
    {
        private final int tableBucketCount;
        private final int partitionBucketCount;
        private final List<HiveColumnHandle> bucketColumnNames;
        // tableBucketNumber is needed, but can be found in tableBucketNumber field of HiveSplit.

        @JsonCreator
        public BucketConversion(
                @JsonProperty("tableBucketCount") int tableBucketCount,
                @JsonProperty("partitionBucketCount") int partitionBucketCount,
                @JsonProperty("bucketColumnHandles") List<HiveColumnHandle> bucketColumnHandles)
        {
            this.tableBucketCount = tableBucketCount;
            this.partitionBucketCount = partitionBucketCount;
            this.bucketColumnNames = requireNonNull(bucketColumnHandles, "bucketColumnHandles is null");
        }

        @JsonProperty
        public int getTableBucketCount()
        {
            return tableBucketCount;
        }

        @JsonProperty
        public int getPartitionBucketCount()
        {
            return partitionBucketCount;
        }

        @JsonProperty
        public List<HiveColumnHandle> getBucketColumnHandles()
        {
            return bucketColumnNames;
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
            BucketConversion that = (BucketConversion) o;
            return tableBucketCount == that.tableBucketCount &&
                    partitionBucketCount == that.partitionBucketCount &&
                    Objects.equals(bucketColumnNames, that.bucketColumnNames);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableBucketCount, partitionBucketCount, bucketColumnNames);
        }
    }
}
