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
package com.facebook.presto.iceberg.optimizer;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HivePartition;
import com.facebook.presto.hive.rule.BaseSubfieldExtractionRewriter;
import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergTableHandle;
import com.facebook.presto.iceberg.IcebergTableLayoutHandle;
import com.facebook.presto.iceberg.IcebergTransactionManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.TableHandle;
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
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.iceberg.Table;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.hive.rule.FilterPushdownUtils.getDomainPredicate;
import static com.facebook.presto.hive.rule.FilterPushdownUtils.getPredicateColumnNames;
import static com.facebook.presto.iceberg.IcebergSessionProperties.isPushdownFilterEnabled;
import static com.facebook.presto.iceberg.IcebergUtil.getIcebergTable;
import static com.facebook.presto.iceberg.IcebergUtil.getPartitionKeyColumnHandles;
import static com.facebook.presto.iceberg.IcebergUtil.getPartitions;
import static com.facebook.presto.iceberg.IcebergUtil.toHiveColumns;
import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class IcebergFilterPushdown
        implements ConnectorPlanOptimizer
{
    private final RowExpressionService rowExpressionService;
    private final StandardFunctionResolution functionResolution;
    private final FunctionMetadataManager functionMetadataManager;
    private final TypeManager typeManager;
    private final IcebergTransactionManager icebergTransactionManager;

    public IcebergFilterPushdown(
            RowExpressionService rowExpressionService,
            StandardFunctionResolution functionResolution,
            FunctionMetadataManager functionMetadataManager,
            IcebergTransactionManager transactionManager,
            TypeManager typeManager)
    {
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        this.icebergTransactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        if (!isPushdownFilterEnabled(session)) {
            return maxSubplan;
        }
        return rewriteWith(new SubfieldExtractionRewriter(
                session,
                idAllocator,
                rowExpressionService,
                functionResolution,
                functionMetadataManager,
                icebergTransactionManager,
                typeManager), maxSubplan);
    }

    public static class SubfieldExtractionRewriter
            extends BaseSubfieldExtractionRewriter
    {
        private final TypeManager typeManager;

        public SubfieldExtractionRewriter(
                ConnectorSession session,
                PlanNodeIdAllocator idAllocator,
                RowExpressionService rowExpressionService,
                StandardFunctionResolution functionResolution,
                FunctionMetadataManager functionMetadataManager,
                IcebergTransactionManager icebergTransactionManager,
                TypeManager typeManager)
        {
            super(session, idAllocator, rowExpressionService, functionResolution, functionMetadataManager, tableHandle -> getConnectorMetadata(icebergTransactionManager, tableHandle));

            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected ConnectorPushdownFilterResult getConnectorPushdownFilterResult(
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
            checkArgument(metadata instanceof IcebergAbstractMetadata, "metadata must be IcebergAbstractMetadata");
            checkArgument(tableHandle instanceof IcebergTableHandle, "tableHandle must be IcebergTableHandle");

            Table icebergTable = getIcebergTable(metadata, session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
            List<IcebergColumnHandle> partitionColumns = getPartitionKeyColumnHandles((IcebergTableHandle) tableHandle, icebergTable, typeManager);
            TupleDomain<ColumnHandle> unenforcedConstraint = TupleDomain.withColumnDomains(Maps.filterKeys(constraint.getSummary().getDomains().get(), not(Predicates.in(partitionColumns))));

            TupleDomain<Subfield> domainPredicate = getDomainPredicate(decomposedFilter, unenforcedConstraint);

            Set<String> predicateColumnNames = getPredicateColumnNames(optimizedRemainingExpression, domainPredicate);

            Map<String, IcebergColumnHandle> predicateColumns = predicateColumnNames.stream()
                    .map(columnHandles::get)
                    .map(IcebergColumnHandle.class::cast)
                    .collect(toImmutableMap(IcebergColumnHandle::getName, Functions.identity()));

            Optional<Set<IcebergColumnHandle>> requestedColumns = currentLayoutHandle.map(layout -> ((IcebergTableLayoutHandle) layout).getRequestedColumns()).orElse(Optional.empty());

            TupleDomain<ColumnHandle> partitionColumnPredicate = TupleDomain.withColumnDomains(Maps.filterKeys(
                    constraint.getSummary().getDomains().get(), Predicates.in(partitionColumns)));
            RuntimeStats runtimeStats = session.getRuntimeStats();
            List<HivePartition> partitions = getPartitions(
                    typeManager,
                    tableHandle,
                    icebergTable,
                    constraint,
                    partitionColumns,
                    runtimeStats);

            return new ConnectorPushdownFilterResult(
                    metadata.getTableLayout(
                            session,
                            new IcebergTableLayoutHandle.Builder()
                                    .setPartitionColumns(ImmutableList.copyOf(partitionColumns))
                                    .setDataColumns(toHiveColumns(icebergTable.schema().columns()))
                                    .setDomainPredicate(domainPredicate)
                                    .setRemainingPredicate(remainingExpressions.getRemainingExpression())
                                    .setPredicateColumns(predicateColumns)
                                    .setRequestedColumns(requestedColumns)
                                    .setPushdownFilterEnabled(true)
                                    .setPartitionColumnPredicate(partitionColumnPredicate)
                                    .setPartitions(Optional.ofNullable(partitions.size() == 0 ? null : partitions))
                                    .setTable((IcebergTableHandle) tableHandle)
                                    .build()),
                    remainingExpressions.getDynamicFilterExpression());
        }

        @Override
        protected boolean isPushdownFilterSupported(ConnectorSession session, TableHandle tableHandle)
        {
            return isPushdownFilterEnabled(session);
        }
    }

    private static ConnectorMetadata getConnectorMetadata(IcebergTransactionManager icebergTransactionManager, TableHandle tableHandle)
    {
        requireNonNull(icebergTransactionManager, "icebergTransactionManager is null");
        ConnectorMetadata metadata = icebergTransactionManager.get(tableHandle.getTransaction());
        checkState(metadata instanceof IcebergAbstractMetadata, "metadata must be IcebergAbstractMetadata");
        return metadata;
    }
}
