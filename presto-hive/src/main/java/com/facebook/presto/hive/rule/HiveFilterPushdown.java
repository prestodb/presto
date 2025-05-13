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
package com.facebook.presto.hive.rule;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.hive.HiveBucketHandle;
import com.facebook.presto.hive.HiveBucketing;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveMetadata;
import com.facebook.presto.hive.HivePartitionManager;
import com.facebook.presto.hive.HivePartitionResult;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.HiveTableHandle;
import com.facebook.presto.hive.HiveTableLayoutHandle;
import com.facebook.presto.hive.HiveTransactionManager;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.google.common.base.Functions;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.hive.HiveSessionProperties.isParquetPushdownFilterEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isPushdownFilterEnabled;
import static com.facebook.presto.hive.HiveTableProperties.getHiveStorageFormat;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getMetastoreHeaders;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isUserDefinedTypeEncodingEnabled;
import static com.facebook.presto.hive.rule.FilterPushdownUtils.getDomainPredicate;
import static com.facebook.presto.hive.rule.FilterPushdownUtils.getPredicateColumnNames;
import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

/**
 * Runs during both logical and physical phases of connector-aided plan optimization.
 * In most cases filter pushdown will occur during logical phase. However, in cases
 * when new filter is added between logical and physical phases, e.g. a filter on a join
 * key from one side of a join is added to the other side, the new filter will get
 * merged with the one already pushed down.
 */
public class HiveFilterPushdown
        implements ConnectorPlanOptimizer
{
    private final RowExpressionService rowExpressionService;
    private final StandardFunctionResolution functionResolution;
    private final FunctionMetadataManager functionMetadataManager;
    protected final HiveTransactionManager transactionManager;
    private final HivePartitionManager partitionManager;

    public HiveFilterPushdown(
            RowExpressionService rowExpressionService,
            StandardFunctionResolution functionResolution,
            FunctionMetadataManager functionMetadataManager,
            HiveTransactionManager transactionManager,
            HivePartitionManager partitionManager)
    {
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        return rewriteWith(new SubfieldExtractionRewriter(session, idAllocator, rowExpressionService, functionResolution, functionMetadataManager, transactionManager, partitionManager, tableHandle -> getConnectorMetadata(transactionManager, tableHandle)), maxSubplan);
    }

    public static class SubfieldExtractionRewriter
            extends BaseSubfieldExtractionRewriter
    {
        private final HivePartitionManager partitionManager;
        public SubfieldExtractionRewriter(
                ConnectorSession session,
                PlanNodeIdAllocator idAllocator,
                RowExpressionService rowExpressionService,
                StandardFunctionResolution functionResolution,
                FunctionMetadataManager functionMetadataManager,
                HiveTransactionManager transactionManager,
                HivePartitionManager partitionManager,
                Function<TableHandle, ConnectorMetadata> transactionToMetadata)
        {
            super(session, idAllocator, rowExpressionService, functionResolution, functionMetadataManager, transactionToMetadata);

            this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        }

        @Override
        public ConnectorPushdownFilterResult getConnectorPushdownFilterResult(
                Map<String, ColumnHandle> columnHandles,
                ConnectorMetadata metadata,
                ConnectorSession session,
                RemainingExpressions remainingExpressions,
                DomainTranslator.ExtractionResult<Subfield> decomposedFilter,
                RowExpression optimizedRemainingExpression,
                Constraint<ColumnHandle> constraint,
                Optional<ConnectorTableLayoutHandle> currentLayoutHandle,
                ConnectorTableHandle tableHandle)
        {
            HivePartitionResult hivePartitionResult = partitionManager.getPartitions(((HiveMetadata) metadata).getMetastore(), tableHandle, constraint, session);

            TupleDomain<ColumnHandle> unenforcedConstraint = hivePartitionResult.getUnenforcedConstraint();

            TupleDomain<Subfield> domainPredicate = getDomainPredicate(decomposedFilter, unenforcedConstraint);

            Set<String> predicateColumnNames = getPredicateColumnNames(optimizedRemainingExpression, domainPredicate);

            Map<String, HiveColumnHandle> predicateColumns = predicateColumnNames.stream()
                    .map(columnHandles::get)
                    .map(HiveColumnHandle.class::cast)
                    .collect(toImmutableMap(HiveColumnHandle::getName, Functions.identity()));

            HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
            SchemaTableName tableName = hiveTableHandle.getSchemaTableName();

            SemiTransactionalHiveMetastore metastore = ((HiveMetadata) metadata).getMetastore();

            MetastoreContext context = new MetastoreContext(
                    session.getIdentity(),
                    session.getQueryId(),
                    session.getClientInfo(),
                    session.getClientTags(),
                    session.getSource(),
                    getMetastoreHeaders(session),
                    isUserDefinedTypeEncodingEnabled(session),
                    metastore.getColumnConverterProvider(),
                    session.getWarningCollector(), session.getRuntimeStats());
            Table table = metastore.getTable(context, hiveTableHandle)
                    .orElseThrow(() -> new TableNotFoundException(tableName));

            String layoutString = createTableLayoutString(
                    session,
                    rowExpressionService,
                    tableName,
                    hivePartitionResult.getBucketHandle(),
                    hivePartitionResult.getBucketFilter(),
                    remainingExpressions.getRemainingExpression(),
                    domainPredicate);

            Optional<Set<HiveColumnHandle>> requestedColumns = currentLayoutHandle.map(layout -> ((HiveTableLayoutHandle) layout).getRequestedColumns()).orElse(Optional.empty());

            boolean appendRowNumber = currentLayoutHandle.map(layout -> ((HiveTableLayoutHandle) layout).isAppendRowNumberEnabled()).orElse(false);

            return new ConnectorPushdownFilterResult(
                    metadata.getTableLayout(
                            session,
                            new HiveTableLayoutHandle.Builder()
                                    .setSchemaTableName(tableName)
                                    .setTablePath(table.getStorage().getLocation())
                                    .setPartitionColumns(hivePartitionResult.getPartitionColumns())
                                    .setDataColumns(pruneColumnComments(hivePartitionResult.getDataColumns()))
                                    .setTableParameters(hivePartitionResult.getTableParameters())
                                    .setDomainPredicate(domainPredicate)
                                    .setRemainingPredicate(remainingExpressions.getRemainingExpression())
                                    .setPredicateColumns(predicateColumns)
                                    .setPartitionColumnPredicate(hivePartitionResult.getEnforcedConstraint())
                                    .setPartitions(hivePartitionResult.getPartitions())
                                    .setBucketHandle(hivePartitionResult.getBucketHandle())
                                    .setBucketFilter(hivePartitionResult.getBucketFilter())
                                    .setPushdownFilterEnabled(true)
                                    .setLayoutString(layoutString)
                                    .setRequestedColumns(requestedColumns)
                                    .setPartialAggregationsPushedDown(false)
                                    .setAppendRowNumberEnabled(appendRowNumber)
                                    .setHiveTableHandle(hiveTableHandle)
                                    .build()),
                    remainingExpressions.getDynamicFilterExpression());
        }

        @Override
        protected boolean isPushdownFilterSupported(ConnectorSession session, TableHandle tableHandle)
        {
            checkArgument(tableHandle.getConnectorHandle() instanceof HiveTableHandle, "pushdownFilter is never supported on a non-hive TableHandle");
            boolean pushdownFilterEnabled = isPushdownFilterEnabled(session);
            if (pushdownFilterEnabled) {
                HiveStorageFormat hiveStorageFormat = getHiveStorageFormat(transactionToMetadata.apply(tableHandle).getTableMetadata(session, tableHandle.getConnectorHandle()).getProperties());
                return hiveStorageFormat == HiveStorageFormat.ORC || hiveStorageFormat == HiveStorageFormat.DWRF || hiveStorageFormat == HiveStorageFormat.PARQUET && isParquetPushdownFilterEnabled(session);
            }
            return false;
        }
    }

    public static ConnectorMetadata getConnectorMetadata(HiveTransactionManager transactionManager, TableHandle tableHandle)
    {
        requireNonNull(transactionManager, "transactionManager is null");
        ConnectorMetadata metadata = transactionManager.get(tableHandle.getTransaction());
        checkState(metadata instanceof HiveMetadata, "metadata must be HiveMetadata");
        return metadata;
    }

    private static List<Column> pruneColumnComments(List<Column> columns)
    {
        return columns.stream()
                .map(column -> new Column(column.getName(), column.getType(), Optional.empty(), column.getTypeMetadata()))
                .collect(toImmutableList());
    }

    private static String createTableLayoutString(
            ConnectorSession session,
            RowExpressionService rowExpressionService,
            SchemaTableName tableName,
            Optional<HiveBucketHandle> bucketHandle,
            Optional<HiveBucketing.HiveBucketFilter> bucketFilter,
            RowExpression remainingPredicate,
            TupleDomain<Subfield> domainPredicate)
    {
        return toStringHelper(tableName.toString())
                .omitNullValues()
                .add("buckets", bucketHandle.map(HiveBucketHandle::getReadBucketCount).orElse(null))
                .add("bucketsToKeep", bucketFilter.map(HiveBucketing.HiveBucketFilter::getBucketsToKeep).orElse(null))
                .add("filter", TRUE_CONSTANT.equals(remainingPredicate) ? null : rowExpressionService.formatRowExpression(session, remainingPredicate))
                .add("domains", domainPredicate.isAll() ? null : domainPredicate.toString(session.getSqlFunctionProperties()))
                .toString();
    }
}
