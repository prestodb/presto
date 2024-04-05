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

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.hive.SubfieldExtractor;
import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergTableHandle;
import com.facebook.presto.iceberg.IcebergTransactionManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.expressions.LogicalRowExpressions.FALSE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.SYNTHESIZED;
import static com.facebook.presto.iceberg.IcebergSessionProperties.isPushdownFilterEnabled;
import static com.facebook.presto.iceberg.IcebergTableType.DATA;
import static com.facebook.presto.iceberg.IcebergUtil.getIcebergTable;
import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class IcebergPlanOptimizer
        implements ConnectorPlanOptimizer
{
    private final RowExpressionService rowExpressionService;
    private final StandardFunctionResolution functionResolution;
    private final FunctionMetadataManager functionMetadataManager;
    private final IcebergTransactionManager transactionManager;

    IcebergPlanOptimizer(StandardFunctionResolution functionResolution,
                         RowExpressionService rowExpressionService,
                         FunctionMetadataManager functionMetadataManager,
                         IcebergTransactionManager transactionManager)
    {
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        if (isPushdownFilterEnabled(session)) {
            return maxSubplan;
        }
        return rewriteWith(new FilterPushdownRewriter(functionResolution, rowExpressionService, functionMetadataManager,
                transactionManager, idAllocator, session), maxSubplan);
    }

    private static class FilterPushdownRewriter
            extends ConnectorPlanRewriter<Void>
    {
        private final ConnectorSession session;
        private final RowExpressionService rowExpressionService;
        private final StandardFunctionResolution functionResolution;
        private final FunctionMetadataManager functionMetadataManager;
        private final PlanNodeIdAllocator idAllocator;
        private final IcebergTransactionManager transactionManager;

        public FilterPushdownRewriter(
                StandardFunctionResolution functionResolution,
                RowExpressionService rowExpressionService,
                FunctionMetadataManager functionMetadataManager,
                IcebergTransactionManager transactionManager,
                PlanNodeIdAllocator idAllocator,
                ConnectorSession session)
        {
            this.functionResolution = functionResolution;
            this.rowExpressionService = rowExpressionService;
            this.functionMetadataManager = functionMetadataManager;
            this.transactionManager = transactionManager;
            this.idAllocator = idAllocator;
            this.session = session;
        }

        @Override
        public PlanNode visitFilter(FilterNode filter, RewriteContext<Void> context)
        {
            if (!(filter.getSource() instanceof TableScanNode)) {
                return visitPlan(filter, context);
            }

            TableScanNode tableScan = (TableScanNode) filter.getSource();
            if (((IcebergTableHandle) tableScan.getTable().getConnectorHandle()).getIcebergTableName().getTableType() != DATA) {
                return visitPlan(filter, context);
            }

            Map<String, IcebergColumnHandle> nameToColumnHandlesMapping = tableScan.getAssignments().entrySet().stream()
                    .collect(Collectors.toMap(e -> e.getKey().getName(), e -> (IcebergColumnHandle) e.getValue()));
            Map<IcebergColumnHandle, String> columnHandleToNameMapping = ImmutableBiMap.copyOf(nameToColumnHandlesMapping).inverse();

            RowExpression filterPredicate = filter.getPredicate();
            checkArgument(!filterPredicate.equals(FALSE_CONSTANT), "Filter expression 'FALSE' should not be left to handle here");

            //TODO we should optimize the filter expression
            DomainTranslator.ExtractionResult<Subfield> decomposedFilter = rowExpressionService.getDomainTranslator()
                    .fromPredicate(session, filterPredicate, new SubfieldExtractor(functionResolution, rowExpressionService.getExpressionOptimizer(), session).toColumnExtractor());

            // Only pushdown the range filters which apply to entire columns, because iceberg does not accept the filters on the subfields in nested structures
            TupleDomain<IcebergColumnHandle> entireColumnDomain = decomposedFilter.getTupleDomain()
                    .transform(subfield -> subfield.getPath().isEmpty() ? subfield.getRootName() : null)
                    .transform(nameToColumnHandlesMapping::get);

            TableHandle handle = tableScan.getTable();
            IcebergTableHandle tableHandle = (IcebergTableHandle) handle.getConnectorHandle();
            IcebergAbstractMetadata metadata = (IcebergAbstractMetadata) transactionManager.get(handle.getTransaction());
            Table icebergTable = getIcebergTable(metadata, session, tableHandle.getSchemaTableName());

            // Get predicate expression on subfield
            SubfieldExtractor subfieldExtractor = new SubfieldExtractor(functionResolution, rowExpressionService.getExpressionOptimizer(), session);
            Map<String, Type> columnTypes = nameToColumnHandlesMapping.entrySet().stream()
                    .collect(toImmutableMap(entry -> entry.getKey(), entry -> entry.getValue().getType()));
            TupleDomain<RowExpression> subfieldTupleDomain = decomposedFilter.getTupleDomain()
                    .transform(subfield -> subfield.getPath().isEmpty() ? null : subfield)
                    .transform(subfield -> subfieldExtractor.toRowExpression(subfield, columnTypes.get(subfield.getRootName())));
            RowExpression subfieldPredicate = rowExpressionService.getDomainTranslator().toPredicate(subfieldTupleDomain);

            // Get predicate tuple domain on identity partition columns, which could be enforced by iceberg table itself
            Set<IcebergColumnHandle> identityPartitionColumns = getIdentityPartitionColumnHandles(icebergTable,
                    nameToColumnHandlesMapping.values().stream().collect(Collectors.toList()));
            TupleDomain<ColumnHandle> identityPartitionColumnPredicate = TupleDomain.withColumnDomains(
                    Maps.filterKeys(
                            entireColumnDomain.transform(icebergColumnHandle -> (ColumnHandle) icebergColumnHandle)
                                    .getDomains().get(),
                            Predicates.in(identityPartitionColumns)));

            // Get predicate expression non-identity entire columns
            TupleDomain<RowExpression> nonPartitionColumnPredicate = TupleDomain.withColumnDomains(
                    Maps.filterKeys(
                            entireColumnDomain.transform(icebergColumnHandle -> (ColumnHandle) icebergColumnHandle)
                                    .getDomains().get(),
                            Predicates.not(Predicates.in(identityPartitionColumns))))
                    .transform(columnHandle -> new Subfield(columnHandleToNameMapping.get(columnHandle), ImmutableList.of()))
                    .transform(subfield -> subfieldExtractor.toRowExpression(subfield, columnTypes.get(subfield.getRootName())));
            RowExpression nonPartitionColumn = rowExpressionService.getDomainTranslator().toPredicate(nonPartitionColumnPredicate);

            // Combine all the rest predicate expressions except predicate on identity partition columns
            LogicalRowExpressions logicalRowExpressions = new LogicalRowExpressions(
                    rowExpressionService.getDeterminismEvaluator(),
                    functionResolution,
                    functionMetadataManager);
            RowExpression remainingFilterExpression = logicalRowExpressions.combineConjuncts(
                    ImmutableList.of(
                            decomposedFilter.getRemainingExpression(),
                            subfieldPredicate,
                            nonPartitionColumn));

            // Simplify call is required because iceberg does not support a large value list for IN predicate
            TupleDomain<IcebergColumnHandle> simplifiedColumnDomain = entireColumnDomain.simplify();
            boolean predicateNotChangedBySimplification = simplifiedColumnDomain.equals(entireColumnDomain);

            IcebergTableHandle oldTableHandle = (IcebergTableHandle) handle.getConnectorHandle();
            IcebergTableHandle newTableHandle = new IcebergTableHandle(
                    oldTableHandle.getSchemaName(),
                    oldTableHandle.getIcebergTableName(),
                    oldTableHandle.isSnapshotSpecified(),
                    simplifiedColumnDomain.intersect(oldTableHandle.getPredicate()),
                    oldTableHandle.getOutputPath(),
                    oldTableHandle.getStorageProperties(),
                    oldTableHandle.getTableSchemaJson(),
                    oldTableHandle.getPartitionSpecId(),
                    oldTableHandle.getEqualityFieldIds());
            TableScanNode newTableScan = new TableScanNode(
                    tableScan.getSourceLocation(),
                    tableScan.getId(),
                    new TableHandle(handle.getConnectorId(), newTableHandle, handle.getTransaction(), handle.getLayout()),
                    tableScan.getOutputVariables(),
                    tableScan.getAssignments(),
                    simplifiedColumnDomain.transform(ColumnHandle.class::cast)
                            .intersect(tableScan.getCurrentConstraint()),
                    predicateNotChangedBySimplification ?
                            identityPartitionColumnPredicate.intersect(tableScan.getEnforcedConstraint()) :
                            tableScan.getEnforcedConstraint());

            if (TRUE_CONSTANT.equals(remainingFilterExpression) && predicateNotChangedBySimplification) {
                return newTableScan;
            }
            else if (predicateNotChangedBySimplification) {
                return new FilterNode(filter.getSourceLocation(), idAllocator.getNextId(), newTableScan, remainingFilterExpression);
            }
            else {
                return new FilterNode(filter.getSourceLocation(), idAllocator.getNextId(), newTableScan, filterPredicate);
            }
        }
    }

    private static Set<IcebergColumnHandle> getIdentityPartitionColumnHandles(Table table,
                                                                              List<IcebergColumnHandle> allColumns)
    {
        Map<Integer, IcebergColumnHandle> idToColumnsMap = allColumns.stream()
                .filter(icebergColumnHandle -> icebergColumnHandle.getColumnType() != SYNTHESIZED)
                .collect(Collectors.toMap(IcebergColumnHandle::getId, identity()));

        // In the case of partition evolution, we must check every partition specs of the table to figure out
        //  whether the predicate on identity partition columns could be enforced by iceberg table itself
        return table.specs().values().stream()
                .map(partitionSpec -> partitionSpec.fields().stream()
                        .filter(field -> field.transform().isIdentity())
                        .map(PartitionField::sourceId)
                        .filter(idToColumnsMap::containsKey)
                        .map(idToColumnsMap::get)
                        .collect(Collectors.toSet()))
                .reduce((columnHandleSet1, columnHandleSet2) -> columnHandleSet1.stream().filter(columnHandleSet2::contains).collect(toImmutableSet()))
                .orElse(ImmutableSet.of());
    }
}
