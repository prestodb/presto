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
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.hive.SubfieldExtractor;
import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergTableHandle;
import com.facebook.presto.iceberg.IcebergTableLayoutHandle;
import com.facebook.presto.iceberg.IcebergTransactionManager;
import com.facebook.presto.iceberg.PartitionTransforms;
import com.facebook.presto.iceberg.PartitionTransforms.ColumnTransform;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
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
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.common.Utils.nativeValueToBlock;
import static com.facebook.presto.expressions.LogicalRowExpressions.FALSE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.toSubfield;
import static com.facebook.presto.iceberg.IcebergPageSink.adjustTimestampForPartitionTransform;
import static com.facebook.presto.iceberg.IcebergSessionProperties.isPushdownFilterEnabled;
import static com.facebook.presto.iceberg.IcebergTableType.DATA;
import static com.facebook.presto.iceberg.IcebergUtil.getAdjacentValue;
import static com.facebook.presto.iceberg.IcebergUtil.getIcebergTable;
import static com.facebook.presto.iceberg.IcebergUtil.getPartitionSpecsIncludingValidData;
import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

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
                    .fromPredicate(session, filterPredicate, new SubfieldExtractor(functionResolution, rowExpressionService.getExpressionOptimizer(session), session).toColumnExtractor());

            // Only pushdown the range filters which apply to entire columns, because iceberg does not accept the filters on the subfields in nested structures
            TupleDomain<IcebergColumnHandle> entireColumnDomain = decomposedFilter.getTupleDomain()
                    .transform(subfield -> subfield.getPath().isEmpty() ? subfield.getRootName() : null)
                    .transform(nameToColumnHandlesMapping::get);

            TableHandle handle = tableScan.getTable();
            IcebergTableHandle tableHandle = (IcebergTableHandle) handle.getConnectorHandle();
            IcebergAbstractMetadata metadata = (IcebergAbstractMetadata) transactionManager.get(handle.getTransaction());
            Table icebergTable = getIcebergTable(metadata, session, tableHandle.getSchemaTableName());

            // Get predicate expression on subfield
            SubfieldExtractor subfieldExtractor = new SubfieldExtractor(functionResolution, rowExpressionService.getExpressionOptimizer(session), session);
            Map<String, Type> columnTypes = nameToColumnHandlesMapping.entrySet().stream()
                    .collect(toImmutableMap(entry -> entry.getKey(), entry -> entry.getValue().getType()));
            TupleDomain<RowExpression> subfieldTupleDomain = decomposedFilter.getTupleDomain()
                    .transform(subfield -> subfield.getPath().isEmpty() ? null : subfield)
                    .transform(subfield -> subfieldExtractor.toRowExpression(subfield, columnTypes.get(subfield.getRootName())));
            RowExpression subfieldPredicate = rowExpressionService.getDomainTranslator().toPredicate(subfieldTupleDomain);

            // Get partition specs that really need to be checked
            Set<Integer> partitionSpecIds = getPartitionSpecsIncludingValidData(icebergTable, tableHandle.getIcebergTableName().getSnapshotId());
            Set<IcebergColumnHandle> enforcedColumns = getEnforcedColumns(icebergTable,
                    partitionSpecIds,
                    entireColumnDomain,
                    session);
            // Get predicate tuple domain on the columns that could be enforced by iceberg table itself
            TupleDomain<ColumnHandle> identityPartitionColumnPredicate = TupleDomain.withColumnDomains(
                    Maps.filterKeys(
                            entireColumnDomain.transform(icebergColumnHandle -> (ColumnHandle) icebergColumnHandle)
                                    .getDomains().get(),
                            Predicates.in(enforcedColumns)));

            // Get predicate expression on entire columns that could not be enforced by iceberg table
            TupleDomain<RowExpression> nonPartitionColumnPredicate = TupleDomain.withColumnDomains(
                            Maps.filterKeys(
                                    entireColumnDomain.transform(icebergColumnHandle -> (ColumnHandle) icebergColumnHandle)
                                            .getDomains().get(),
                                    Predicates.not(Predicates.in(enforcedColumns))))
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

            Optional<ConnectorTableLayoutHandle> newConnectorTableLayoutHandle = handle.getLayout().map(IcebergTableLayoutHandle.class::cast)
                    .map(icebergTableLayoutHandle -> new IcebergTableLayoutHandle(
                            icebergTableLayoutHandle.getPartitionColumns().stream()
                                    .map(IcebergColumnHandle.class::cast).collect(toList()),
                            icebergTableLayoutHandle.getDataColumns(),
                            simplifiedColumnDomain.transform(columnHandle -> toSubfield(columnHandle))
                                    .intersect(icebergTableLayoutHandle.getDomainPredicate()),
                            icebergTableLayoutHandle.getRemainingPredicate(),
                            icebergTableLayoutHandle.getPredicateColumns(),
                            icebergTableLayoutHandle.getRequestedColumns(),
                            icebergTableLayoutHandle.isPushdownFilterEnabled(),
                            identityPartitionColumnPredicate.simplify()
                                    .intersect(icebergTableLayoutHandle.getPartitionColumnPredicate()),
                            icebergTableLayoutHandle.getPartitions(),
                            icebergTableLayoutHandle.getTable()));
            TableScanNode newTableScan = new TableScanNode(
                    tableScan.getSourceLocation(),
                    tableScan.getId(),
                    new TableHandle(handle.getConnectorId(), handle.getConnectorHandle(), handle.getTransaction(), newConnectorTableLayoutHandle),
                    tableScan.getOutputVariables(),
                    tableScan.getAssignments(),
                    simplifiedColumnDomain.transform(ColumnHandle.class::cast)
                            .intersect(tableScan.getCurrentConstraint()),
                    predicateNotChangedBySimplification ?
                            identityPartitionColumnPredicate.intersect(tableScan.getEnforcedConstraint()) :
                            tableScan.getEnforcedConstraint(),
                    tableScan.getCteMaterializationInfo());

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

    public static Set<IcebergColumnHandle> getEnforcedColumns(
            Table table,
            Set<Integer> partitionSpecIds,
            TupleDomain<IcebergColumnHandle> entireColumnDomain,
            ConnectorSession session)
    {
        ImmutableSet.Builder<IcebergColumnHandle> result = ImmutableSet.builder();
        Map<IcebergColumnHandle, Domain> domains = entireColumnDomain.getDomains().orElseThrow(() -> new VerifyException("No domains"));
        domains.forEach((columnHandle, domain) -> {
            if (canEnforceColumnConstraintInSpecs(table, partitionSpecIds, columnHandle, domain, session)) {
                result.add(columnHandle);
            }
        });
        return result.build();
    }

    public static boolean canEnforceColumnConstraintInSpecs(
            Table table,
            Set<Integer> partitionSpecIds,
            IcebergColumnHandle columnHandle,
            Domain domain,
            ConnectorSession session)
    {
        return table.specs().values().stream()
                .filter(partitionSpec -> partitionSpecIds.contains(partitionSpec.specId()))
                .allMatch(spec -> canEnforceConstraintWithinPartitioningSpec(spec, columnHandle, domain, session));
    }

    private static boolean canEnforceConstraintWithinPartitioningSpec(
            PartitionSpec spec,
            IcebergColumnHandle column,
            Domain domain,
            ConnectorSession session)
    {
        for (PartitionField field : spec.getFieldsBySourceId(column.getId())) {
            if (canEnforceConstraintWithPartitionField(field, column, domain, session)) {
                return true;
            }
        }
        return false;
    }

    private static boolean canEnforceConstraintWithPartitionField(
            PartitionField field,
            IcebergColumnHandle column,
            Domain domain,
            ConnectorSession session)
    {
        if (field.transform().isVoid()) {
            // Useless for filtering.
            return false;
        }
        if (field.transform().isIdentity()) {
            // A predicate on an identity partitioning column can always be enforced.
            return true;
        }

        ColumnTransform transform = PartitionTransforms.getColumnTransform(field, column.getType());
        ValueSet domainValues = domain.getValues();

        boolean canEnforce = domainValues.getValuesProcessor().transform(
                ranges -> {
                    for (Range range : ranges.getOrderedRanges()) {
                        if (!canEnforceRangeWithPartitioningField(field, transform, range, session)) {
                            return false;
                        }
                    }
                    return true;
                },
                discreteValues -> false,
                allOrNone -> true);
        return canEnforce;
    }

    private static boolean canEnforceRangeWithPartitioningField(
            PartitionField field,
            ColumnTransform transform,
            Range range,
            ConnectorSession session)
    {
        if (transform.getTransformName().startsWith("bucket")) {
            // bucketing transform could not be enforced
            return false;
        }
        Type type = range.getType();
        if (!type.isOrderable()) {
            return false;
        }
        if (!range.isLowUnbounded()) {
            Object boundedValue = range.getLowBoundedValue();
            Optional<Object> adjacentValue = getAdjacentValue(type, boundedValue, range.isLowInclusive());
            if (!adjacentValue.isPresent() || yieldSamePartitioningValue(field, transform, type, boundedValue, adjacentValue.get(), session)) {
                return false;
            }
        }
        if (!range.isHighUnbounded()) {
            Object boundedValue = range.getHighBoundedValue();
            Optional<Object> adjacentValue = getAdjacentValue(type, boundedValue, !range.isHighInclusive());
            if (!adjacentValue.isPresent() || yieldSamePartitioningValue(field, transform, type, boundedValue, adjacentValue.get(), session)) {
                return false;
            }
        }
        return true;
    }

    private static boolean yieldSamePartitioningValue(
            PartitionField field,
            ColumnTransform transform,
            Type sourceType,
            Object first,
            Object second,
            ConnectorSession session)
    {
        requireNonNull(first, "first is null");
        requireNonNull(second, "second is null");

        if (sourceType instanceof TimestampType &&
                session.getSqlFunctionProperties().isLegacyTimestamp() &&
                !field.transform().isIdentity()) {
            TimestampType timestampType = (TimestampType) sourceType;
            first = adjustTimestampForPartitionTransform(
                    session.getSqlFunctionProperties(),
                    timestampType,
                    first);
            second = adjustTimestampForPartitionTransform(
                    session.getSqlFunctionProperties(),
                    timestampType,
                    second);
        }
        Object firstTransformed = transform.getValueTransform().apply(nativeValueToBlock(sourceType, first), 0);
        Object secondTransformed = transform.getValueTransform().apply(nativeValueToBlock(sourceType, second), 0);
        // The pushdown logic assumes NULLs and non-NULLs are segregated, so that we have to think about non-null values only.
        verify(firstTransformed != null && secondTransformed != null, "Transform for %s returned null for non-null input", field);
        try {
            return Objects.equals(firstTransformed, secondTransformed);
        }
        catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }
}
