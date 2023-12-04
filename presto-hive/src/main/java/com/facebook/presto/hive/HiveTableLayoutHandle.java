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
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.relation.RowExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.expressions.CanonicalRowExpressionRewriter.canonicalizeRowExpression;
import static com.facebook.presto.hive.MetadataUtils.createPredicate;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class HiveTableLayoutHandle
        extends BaseHiveTableLayoutHandle
{
    private final SchemaTableName schemaTableName;
    private final String tablePath;
    private final List<HiveColumnHandle> partitionColumns;
    private final List<Column> dataColumns;
    private final Map<String, String> tableParameters;
    private final Map<String, HiveColumnHandle> predicateColumns;
    private final Optional<HiveBucketHandle> bucketHandle;
    private final Optional<HiveBucketFilter> bucketFilter;
    private final String layoutString;
    private final Optional<Set<HiveColumnHandle>> requestedColumns;
    private final boolean partialAggregationsPushedDown;
    private final boolean appendRowNumberEnabled;
    private final boolean footerStatsUnreliable;

    // coordinator-only properties
    private final Optional<List<HivePartition>> partitions;
    private final Optional<HiveTableHandle> hiveTableHandle;

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
            @JsonProperty("appendRowNumber") boolean appendRowNumberEnabled,
            @JsonProperty("footerStatsUnreliable") boolean footerStatsUnreliable)
    {
        this(
                schemaTableName,
                tablePath,
                partitionColumns,
                dataColumns,
                tableParameters,
                domainPredicate,
                remainingPredicate,
                predicateColumns,
                partitionColumnPredicate,
                bucketHandle,
                bucketFilter,
                pushdownFilterEnabled,
                layoutString,
                requestedColumns,
                partialAggregationsPushedDown,
                appendRowNumberEnabled,
                Optional.empty(),
                footerStatsUnreliable,
                Optional.empty());
    }

    protected HiveTableLayoutHandle(
            SchemaTableName schemaTableName,
            String tablePath,
            List<HiveColumnHandle> partitionColumns,
            List<Column> dataColumns,
            Map<String, String> tableParameters,
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
            boolean appendRowNumberEnabled,
            Optional<List<HivePartition>> partitions,
            boolean footerStatsUnreliable,
            Optional<HiveTableHandle> hiveTableHandle)
    {
        super(domainPredicate, remainingPredicate, pushdownFilterEnabled, partitionColumnPredicate, partitions);

        this.schemaTableName = requireNonNull(schemaTableName, "table is null");
        this.tablePath = requireNonNull(tablePath, "tablePath is null");
        this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
        this.dataColumns = ImmutableList.copyOf(requireNonNull(dataColumns, "dataColumns is null"));
        this.tableParameters = ImmutableMap.copyOf(requireNonNull(tableParameters, "tableProperties is null"));
        this.predicateColumns = requireNonNull(predicateColumns, "predicateColumns is null");
        this.bucketHandle = requireNonNull(bucketHandle, "bucketHandle is null");
        this.bucketFilter = requireNonNull(bucketFilter, "bucketFilter is null");
        this.layoutString = requireNonNull(layoutString, "layoutString is null");
        this.requestedColumns = requireNonNull(requestedColumns, "requestedColumns is null");
        this.partialAggregationsPushedDown = partialAggregationsPushedDown;
        this.appendRowNumberEnabled = appendRowNumberEnabled;
        this.partitions = requireNonNull(partitions, "partitions is null");
        this.footerStatsUnreliable = footerStatsUnreliable;
        this.hiveTableHandle = requireNonNull(hiveTableHandle, "hiveTableHandle is null");
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
     * HiveTableHandle is dropped when HiveTableLayoutHandle is serialized.
     *
     * @return HiveTableHandle if available, {@code Optional.empty()} if dropped
     */
    @JsonIgnore
    public Optional<HiveTableHandle> getHiveTableHandle()
    {
        return hiveTableHandle;
    }

    @JsonProperty
    public Map<String, HiveColumnHandle> getPredicateColumns()
    {
        return predicateColumns;
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

    @JsonProperty
    public boolean isFooterStatsUnreliable()
    {
        return footerStatsUnreliable;
    }

    @Override
    public Object getIdentifier(Optional<ConnectorSplit> split, PlanCanonicalizationStrategy canonicalizationStrategy)
    {
        TupleDomain<Subfield> domainPredicate = this.getDomainPredicate();

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
                .put("domainPredicate", canonicalizeDomainPredicate(domainPredicate, getPredicateColumns(), canonicalizationStrategy))
                .put("remainingPredicate", canonicalizeRowExpression(this.getRemainingPredicate(), false))
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
        TupleDomain<ColumnHandle> constraint = createPredicate(ImmutableList.copyOf(partitionColumns), partitions.get());
        constraint = getDomainPredicate()
                .transform(subfield -> subfield.getPath().isEmpty() ? subfield.getRootName() : null)
                .transform(getPredicateColumns()::get)
                .transform(ColumnHandle.class::cast)
                .intersect(constraint);

        constraint = constraint.canonicalize(HiveTableLayoutHandle::isPartitionKey);
        return constraint;
    }

    @VisibleForTesting
    public static TupleDomain<Subfield> canonicalizeDomainPredicate(TupleDomain<Subfield> domainPredicate, Map<String, HiveColumnHandle> predicateColumns, PlanCanonicalizationStrategy strategy)
    {
        if (strategy == PlanCanonicalizationStrategy.DEFAULT) {
            return domainPredicate.canonicalize(ignored -> false);
        }
        return domainPredicate
                .transform(subfield -> {
                    if (!subfield.getPath().isEmpty() || !predicateColumns.containsKey(subfield.getRootName())) {
                        return subfield;
                    }
                    return isPartitionKey(predicateColumns.get(subfield.getRootName())) ? null : subfield;
                })
                .canonicalize(ignored -> false);
    }

    private static boolean isPartitionKey(ColumnHandle columnHandle)
    {
        return columnHandle instanceof HiveColumnHandle && ((HiveColumnHandle) columnHandle).isPartitionKey();
    }

    public Table getTable(SemiTransactionalHiveMetastore metastore, MetastoreContext metastoreContext)
    {
        Optional<Table> table;
        if (hiveTableHandle.isPresent()) {
            table = metastore.getTable(metastoreContext, hiveTableHandle.get());
        }
        else {
            table = metastore.getTable(metastoreContext, schemaTableName.getSchemaName(), schemaTableName.getTableName());
        }
        return table.orElseThrow(() -> new TableNotFoundException(schemaTableName));
    }

    public Builder builder()
    {
        return new Builder()
                .setSchemaTableName(getSchemaTableName())
                .setTablePath(getTablePath())
                .setPartitionColumns(getPartitionColumns())
                .setDataColumns(getDataColumns())
                .setTableParameters(getTableParameters())
                .setDomainPredicate(getDomainPredicate())
                .setRemainingPredicate(getRemainingPredicate())
                .setPredicateColumns(getPredicateColumns())
                .setPartitionColumnPredicate(getPartitionColumnPredicate())
                .setBucketHandle(getBucketHandle())
                .setBucketFilter(getBucketFilter())
                .setPushdownFilterEnabled(isPushdownFilterEnabled())
                .setLayoutString(getLayoutString())
                .setRequestedColumns(getRequestedColumns())
                .setPartialAggregationsPushedDown(isPartialAggregationsPushedDown())
                .setAppendRowNumberEnabled(isAppendRowNumberEnabled())
                .setPartitions(getPartitions())
                .setFooterStatsUnreliable(isFooterStatsUnreliable())
                .setHiveTableHandle(getHiveTableHandle());
    }

    public static class Builder
    {
        private SchemaTableName schemaTableName;
        private String tablePath;
        private List<HiveColumnHandle> partitionColumns;
        private List<Column> dataColumns;
        private Map<String, String> tableParameters;
        private TupleDomain<Subfield> domainPredicate;
        private RowExpression remainingPredicate;
        private Map<String, HiveColumnHandle> predicateColumns;
        private TupleDomain<ColumnHandle> partitionColumnPredicate;
        private Optional<HiveBucketHandle> bucketHandle;
        private Optional<HiveBucketFilter> bucketFilter;
        private boolean pushdownFilterEnabled;
        private String layoutString;
        private Optional<Set<HiveColumnHandle>> requestedColumns;
        private boolean partialAggregationsPushedDown;
        private boolean appendRowNumberEnabled;
        private boolean footerStatsUnreliable;

        private Optional<List<HivePartition>> partitions;
        private Optional<HiveTableHandle> hiveTableHandle = Optional.empty();

        public Builder setSchemaTableName(SchemaTableName schemaTableName)
        {
            this.schemaTableName = schemaTableName;
            return this;
        }

        public Builder setTablePath(String tablePath)
        {
            this.tablePath = tablePath;
            return this;
        }

        public Builder setPartitionColumns(List<HiveColumnHandle> partitionColumns)
        {
            this.partitionColumns = partitionColumns;
            return this;
        }

        public Builder setDataColumns(List<Column> dataColumns)
        {
            this.dataColumns = dataColumns;
            return this;
        }

        public Builder setTableParameters(Map<String, String> tableParameters)
        {
            this.tableParameters = tableParameters;
            return this;
        }

        public Builder setDomainPredicate(TupleDomain<Subfield> domainPredicate)
        {
            this.domainPredicate = domainPredicate;
            return this;
        }

        public Builder setRemainingPredicate(RowExpression remainingPredicate)
        {
            this.remainingPredicate = remainingPredicate;
            return this;
        }

        public Builder setPredicateColumns(Map<String, HiveColumnHandle> predicateColumns)
        {
            this.predicateColumns = predicateColumns;
            return this;
        }

        public Builder setPartitionColumnPredicate(TupleDomain<ColumnHandle> partitionColumnPredicate)
        {
            this.partitionColumnPredicate = partitionColumnPredicate;
            return this;
        }

        public Builder setBucketHandle(Optional<HiveBucketHandle> bucketHandle)
        {
            this.bucketHandle = bucketHandle;
            return this;
        }

        public Builder setBucketFilter(Optional<HiveBucketFilter> bucketFilter)
        {
            this.bucketFilter = bucketFilter;
            return this;
        }

        public Builder setPushdownFilterEnabled(boolean pushdownFilterEnabled)
        {
            this.pushdownFilterEnabled = pushdownFilterEnabled;
            return this;
        }

        public Builder setLayoutString(String layoutString)
        {
            this.layoutString = layoutString;
            return this;
        }

        public Builder setRequestedColumns(Optional<Set<HiveColumnHandle>> requestedColumns)
        {
            this.requestedColumns = requestedColumns;
            return this;
        }

        public Builder setPartialAggregationsPushedDown(boolean partialAggregationsPushedDown)
        {
            this.partialAggregationsPushedDown = partialAggregationsPushedDown;
            return this;
        }

        public Builder setAppendRowNumberEnabled(boolean appendRowNumberEnabled)
        {
            this.appendRowNumberEnabled = appendRowNumberEnabled;
            return this;
        }

        public Builder setPartitions(List<HivePartition> partitions)
        {
            requireNonNull(partitions, "partitions is null");
            return setPartitions(Optional.of(partitions));
        }

        public Builder setPartitions(Optional<List<HivePartition>> partitions)
        {
            requireNonNull(partitions, "partitions is null");
            this.partitions = partitions;
            return this;
        }

        public Builder setFooterStatsUnreliable(boolean footerStatsUnreliable)
        {
            this.footerStatsUnreliable = footerStatsUnreliable;
            return this;
        }
        public Builder setHiveTableHandle(Optional<HiveTableHandle> hiveTableHandle)
        {
            this.hiveTableHandle = requireNonNull(hiveTableHandle, "hiveTableHandle is null");
            return this;
        }

        public Builder setHiveTableHandle(HiveTableHandle hiveTableHandle)
        {
            requireNonNull(hiveTableHandle, "hiveTableHandle is null");
            this.hiveTableHandle = Optional.of(hiveTableHandle);
            return this;
        }

        public HiveTableLayoutHandle build()
        {
            return new HiveTableLayoutHandle(
                    schemaTableName,
                    tablePath,
                    partitionColumns,
                    dataColumns,
                    tableParameters,
                    domainPredicate,
                    remainingPredicate,
                    predicateColumns,
                    partitionColumnPredicate,
                    bucketHandle,
                    bucketFilter,
                    pushdownFilterEnabled,
                    layoutString,
                    requestedColumns,
                    partialAggregationsPushedDown,
                    appendRowNumberEnabled,
                    partitions,
                    footerStatsUnreliable,
                    hiveTableHandle);
        }
    }
}
