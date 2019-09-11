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

import com.facebook.presto.hive.HiveBucketing.HiveBucketFilter;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.RowExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.spi.relation.LogicalRowExpressions.TRUE_CONSTANT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class HiveTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final SchemaTableName schemaTableName;
    private final List<ColumnHandle> partitionColumns;
    private final TupleDomain<Subfield> domainPredicate;
    private final RowExpression remainingPredicate;
    private final Map<String, HiveColumnHandle> predicateColumns;
    private final TupleDomain<ColumnHandle> partitionColumnPredicate;
    private final Optional<HiveBucketHandle> bucketHandle;
    private final Optional<HiveBucketFilter> bucketFilter;

    // coordinator-only properties
    @Nullable
    private final List<HivePartition> partitions;

    // Keep this for use in connectors which wrap Hive connectors. This is needed for making altered copies of HiveTableLayoutHandle.
    @Nullable
    private Function<RowExpression, String> rowExpressionFormatter;

    private final String layoutString;

    @JsonCreator
    public HiveTableLayoutHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("partitionColumns") List<ColumnHandle> partitionColumns,
            @JsonProperty("domainPredicate") TupleDomain<Subfield> domainPredicate,
            @JsonProperty("remainingPredicate") RowExpression remainingPredicate,
            @JsonProperty("predicateColumns") Map<String, HiveColumnHandle> predicateColumns,
            @JsonProperty("partitionColumnPredicate") TupleDomain<ColumnHandle> partitionColumnPredicate,
            @JsonProperty("bucketHandle") Optional<HiveBucketHandle> bucketHandle,
            @JsonProperty("bucketFilter") Optional<HiveBucketFilter> bucketFilter)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "table is null");
        this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
        this.domainPredicate = requireNonNull(domainPredicate, "domainPredicate is null");
        this.remainingPredicate = requireNonNull(remainingPredicate, "remainingPredicate is null");
        this.predicateColumns = requireNonNull(predicateColumns, "predicateColumns is null");
        this.partitionColumnPredicate = requireNonNull(partitionColumnPredicate, "partitionColumnPredicate is null");
        this.partitions = null;
        this.bucketHandle = requireNonNull(bucketHandle, "bucketHandle is null");
        this.bucketFilter = requireNonNull(bucketFilter, "bucketFilter is null");

        // on a worker
        layoutString = toStringHelper(schemaTableName.toString())
                .omitNullValues()
                .add("buckets", bucketHandle.map(HiveBucketHandle::getReadBucketCount).orElse(null))
                .toString();
    }

    public HiveTableLayoutHandle(
            SchemaTableName schemaTableName,
            List<ColumnHandle> partitionColumns,
            List<HivePartition> partitions,
            TupleDomain<Subfield> domainPredicate,
            RowExpression remainingPredicate,
            Map<String, HiveColumnHandle> predicateColumns,
            TupleDomain<ColumnHandle> partitionColumnPredicate,
            Optional<HiveBucketHandle> bucketHandle,
            Optional<HiveBucketFilter> bucketFilter,
            ConnectorSession session,
            Function<RowExpression, String> rowExpressionFormatter)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "table is null");
        this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
        this.partitions = requireNonNull(partitions, "partitions is null");
        this.domainPredicate = requireNonNull(domainPredicate, "domainPredicate is null");
        this.remainingPredicate = requireNonNull(remainingPredicate, "remainingPredicate is null");
        this.predicateColumns = requireNonNull(predicateColumns, "predicateColumns is null");
        this.partitionColumnPredicate = requireNonNull(partitionColumnPredicate, "partitionColumnPredicate is null");
        this.bucketHandle = requireNonNull(bucketHandle, "bucketHandle is null");
        this.bucketFilter = requireNonNull(bucketFilter, "bucketFilter is null");

        // on coordinator
        requireNonNull(session, "session is null");
        this.rowExpressionFormatter = requireNonNull(rowExpressionFormatter, "rowExpressionFormatter is null");

        layoutString = toStringHelper(schemaTableName.toString())
                .omitNullValues()
                .add("buckets", bucketHandle.map(HiveBucketHandle::getReadBucketCount).orElse(null))
                .add("filter", TRUE_CONSTANT.equals(remainingPredicate) ? null : rowExpressionFormatter.apply(remainingPredicate))
                .add("domains", domainPredicate.isAll() ? null : domainPredicate.toString(session))
                .toString();
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public List<ColumnHandle> getPartitionColumns()
    {
        return partitionColumns;
    }

    /**
     * Partitions are dropped when HiveTableLayoutHandle is serialized.
     *
     * @return list of partitions if available, {@code Optional.empty()} if dropped
     */
    @JsonIgnore
    public Optional<List<HivePartition>> getPartitions()
    {
        return Optional.ofNullable(partitions);
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
    public TupleDomain<ColumnHandle> getPartitionColumnPredicate()
    {
        return partitionColumnPredicate;
    }

    @JsonProperty
    public Optional<HiveBucketHandle> getBucketHandle()
    {
        return bucketHandle;
    }

    @JsonProperty
    public Optional<HiveBucketFilter> getBucketFilter()
    {
        return bucketFilter;
    }

    public Function<RowExpression, String> getRowExpressionFormatter()
    {
        return rowExpressionFormatter;
    }

    @Override
    public String toString()
    {
        return layoutString;
    }
}
