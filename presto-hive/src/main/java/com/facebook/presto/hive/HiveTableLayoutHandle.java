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

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.plan.PlanCanonicalizationStrategy;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.TupleDomain.ColumnDomain;
import com.facebook.presto.hive.HiveBucketing.HiveBucketFilter;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.relation.RowExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.predicate.TupleDomain.toLinkedMap;
import static com.facebook.presto.common.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.expressions.CanonicalRowExpressionRewriter.canonicalizeRowExpression;
import static com.facebook.presto.hive.HiveMetadata.createPredicate;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

public final class HiveTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final SchemaTableName schemaTableName;
    private final String tablePath;
    private final List<HiveColumnHandle> partitionColumns;
    private final List<Column> dataColumns;
    private final Map<String, String> tableParameters;
    private final TupleDomain<Subfield> domainPredicate;
    private final RowExpression remainingPredicate;
    private final Map<String, HiveColumnHandle> predicateColumns;
    private final TupleDomain<ColumnHandle> partitionColumnPredicate;
    private final Optional<HiveBucketHandle> bucketHandle;
    private final Optional<HiveBucketFilter> bucketFilter;
    private final boolean pushdownFilterEnabled;
    private final String layoutString;
    private final Optional<Set<HiveColumnHandle>> requestedColumns;
    private final boolean partialAggregationsPushedDown;
    private final boolean appendRowNumberEnabled;
    // coordinator-only properties
    @Nullable
    private final List<HivePartition> partitions;

    @JsonCreator
    public HiveTableLayoutHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("tablePath") String tablePath,
            @JsonProperty("partitionColumns") List<HiveColumnHandle> partitionColumns,
            @JsonProperty("dataColumns") List<Column> dataColumns,
            @JsonProperty("tableParameters") Map<String, String> tableParameters,
            @JsonProperty("domainPredicate") TupleDomain<Subfield> domainPredicate,
            @JsonProperty("remainingPredicate") RowExpression remainingPredicate,
            @JsonProperty("predicateColumns") Map<String, HiveColumnHandle> predicateColumns,
            @JsonProperty("partitionColumnPredicate") TupleDomain<ColumnHandle> partitionColumnPredicate,
            @JsonProperty("bucketHandle") Optional<HiveBucketHandle> bucketHandle,
            @JsonProperty("bucketFilter") Optional<HiveBucketFilter> bucketFilter,
            @JsonProperty("pushdownFilterEnabled") boolean pushdownFilterEnabled,
            @JsonProperty("layoutString") String layoutString,
            @JsonProperty("requestedColumns") Optional<Set<HiveColumnHandle>> requestedColumns,
            @JsonProperty("partialAggregationsPushedDown") boolean partialAggregationsPushedDown,
            @JsonProperty("appendRowNumber") boolean appendRowNumberEnabled)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "table is null");
        this.tablePath = requireNonNull(tablePath, "tablePath is null");
        this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
        this.dataColumns = ImmutableList.copyOf(requireNonNull(dataColumns, "dataColumns is null"));
        this.tableParameters = ImmutableMap.copyOf(requireNonNull(tableParameters, "tableProperties is null"));
        this.domainPredicate = requireNonNull(domainPredicate, "domainPredicate is null");
        this.remainingPredicate = requireNonNull(remainingPredicate, "remainingPredicate is null");
        this.predicateColumns = requireNonNull(predicateColumns, "predicateColumns is null");
        this.partitionColumnPredicate = requireNonNull(partitionColumnPredicate, "partitionColumnPredicate is null");
        this.partitions = null;
        this.bucketHandle = requireNonNull(bucketHandle, "bucketHandle is null");
        this.bucketFilter = requireNonNull(bucketFilter, "bucketFilter is null");
        this.pushdownFilterEnabled = pushdownFilterEnabled;
        this.layoutString = requireNonNull(layoutString, "layoutString is null");
        this.requestedColumns = requireNonNull(requestedColumns, "requestedColumns is null");
        this.partialAggregationsPushedDown = partialAggregationsPushedDown;
        this.appendRowNumberEnabled = appendRowNumberEnabled;
    }

    public HiveTableLayoutHandle(
            SchemaTableName schemaTableName,
            String tablePath,
            List<HiveColumnHandle> partitionColumns,
            List<Column> dataColumns,
            Map<String, String> tableParameters,
            List<HivePartition> partitions,
            TupleDomain<Subfield> domainPredicate,
            RowExpression remainingPredicate,
            Map<String, HiveColumnHandle> predicateColumns,
            TupleDomain<ColumnHandle> partitionColumnPredicate,
            Optional<HiveBucketHandle> bucketHandle,
            Optional<HiveBucketFilter> bucketFilter,
            boolean pushdownFilterEnabled,
            String layoutString,
            Optional<Set<HiveColumnHandle>> requestedColumns,
            boolean partialAggregationsPushedDown,
            boolean appendRowNumberEnabled)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "table is null");
        this.tablePath = requireNonNull(tablePath, "tablePath is null");
        this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
        this.dataColumns = ImmutableList.copyOf(requireNonNull(dataColumns, "dataColumns is null"));
        this.tableParameters = ImmutableMap.copyOf(requireNonNull(tableParameters, "tableProperties is null"));
        this.partitions = requireNonNull(partitions, "partitions is null");
        this.domainPredicate = requireNonNull(domainPredicate, "domainPredicate is null");
        this.remainingPredicate = requireNonNull(remainingPredicate, "remainingPredicate is null");
        this.predicateColumns = requireNonNull(predicateColumns, "predicateColumns is null");
        this.partitionColumnPredicate = requireNonNull(partitionColumnPredicate, "partitionColumnPredicate is null");
        this.bucketHandle = requireNonNull(bucketHandle, "bucketHandle is null");
        this.bucketFilter = requireNonNull(bucketFilter, "bucketFilter is null");
        this.pushdownFilterEnabled = pushdownFilterEnabled;
        this.layoutString = requireNonNull(layoutString, "layoutString is null");
        this.requestedColumns = requireNonNull(requestedColumns, "requestedColumns is null");
        this.partialAggregationsPushedDown = partialAggregationsPushedDown;
        this.appendRowNumberEnabled = appendRowNumberEnabled;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public String getTablePath()
    {
        return tablePath;
    }

    @JsonProperty
    public List<HiveColumnHandle> getPartitionColumns()
    {
        return partitionColumns;
    }

    @JsonProperty
    public List<Column> getDataColumns()
    {
        return dataColumns;
    }

    @JsonProperty
    public Map<String, String> getTableParameters()
    {
        return tableParameters;
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

    @JsonProperty
    public boolean isPushdownFilterEnabled()
    {
        return pushdownFilterEnabled;
    }

    @JsonProperty
    public String getLayoutString()
    {
        return layoutString;
    }

    @JsonProperty
    public Optional<Set<HiveColumnHandle>> getRequestedColumns()
    {
        return requestedColumns;
    }

    @Override
    public String toString()
    {
        return layoutString;
    }

    @JsonProperty
    public boolean isPartialAggregationsPushedDown()
    {
        return partialAggregationsPushedDown;
    }

    @JsonProperty
    public boolean isAppendRowNumberEnabled()
    {
        return appendRowNumberEnabled;
    }

    @Override
    public Object getIdentifier(Optional<ConnectorSplit> split, PlanCanonicalizationStrategy canonicalizationStrategy)
    {
        TupleDomain<Subfield> domainPredicate = this.domainPredicate;

        // If split is provided, we would update the identifier based on split runtime information.
        if (split.isPresent() && (split.get() instanceof HiveSplit) && domainPredicate.getColumnDomains().isPresent()) {
            HiveSplit hiveSplit = (HiveSplit) split.get();
            Set<Subfield> subfields = hiveSplit.getRedundantColumnDomains().stream()
                    .map(column -> new Subfield(((HiveColumnHandle) column).getName()))
                    .collect(toImmutableSet());
            List<ColumnDomain<Subfield>> columnDomains = domainPredicate.getColumnDomains().get().stream()
                    .filter(columnDomain -> !subfields.contains(columnDomain.getColumn()))
                    .collect(toImmutableList());
            domainPredicate = TupleDomain.fromColumnDomains(Optional.of(columnDomains));
        }

        // Identifier is used to identify if the table layout is providing the same set of data.
        // To achieve this, we need table name, data column predicates and bucket filter.
        // We did not include other fields because they are either table metadata or partition column predicate,
        // which is unrelated to identifier purpose, or has already been applied as the boundary of split.
        return ImmutableMap.builder()
                .put("schemaTableName", schemaTableName)
                .put("domainPredicate", domainPredicate.canonicalize(false))
                .put("remainingPredicate", canonicalizeRowExpression(remainingPredicate, false))
                .put("constraint", getConstraint(canonicalizationStrategy))
                // TODO: Decide what to do with bucketFilter when canonicalizing
                .put("bucketFilter", bucketFilter)
                .build();
    }

    private TupleDomain<ColumnHandle> getConstraint(PlanCanonicalizationStrategy canonicalizationStrategy)
    {
        if (canonicalizationStrategy == PlanCanonicalizationStrategy.DEFAULT) {
            return TupleDomain.all();
        }
        // Canonicalize constraint by removing constants when column is a partition key. This assumes
        // all partitions are similar, and will have similar statistics like size, cardinality etc.
        // Constants are only removed from point checks, and not range checks. Example:
        // `x = 1` is equivalent to `x = 1000`
        // `x > 1` is NOT equivalent to `x > 1000`
        TupleDomain<ColumnHandle> constraint = createPredicate(ImmutableList.copyOf(partitionColumns), partitions);
        if (pushdownFilterEnabled) {
            constraint = getDomainPredicate()
                    .transform(subfield -> subfield.getPath().isEmpty() ? subfield.getRootName() : null)
                    .transform(getPredicateColumns()::get)
                    .transform(ColumnHandle.class::cast)
                    .intersect(constraint);
        }

        constraint = withColumnDomains(constraint.getDomains().get().entrySet().stream()
                .sorted(comparing(entry -> entry.getKey().toString()))
                .collect(toLinkedMap(Map.Entry::getKey, entry -> entry.getValue().canonicalize(isPartitionKey(entry.getKey())))));
        return constraint;
    }

    private static boolean isPartitionKey(ColumnHandle columnHandle)
    {
        return columnHandle instanceof HiveColumnHandle && ((HiveColumnHandle) columnHandle).isPartitionKey();
    }
}
