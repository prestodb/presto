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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.hive.BaseHiveColumnHandle;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.iceberg.ColumnIdentity;
import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergTableHandle;
import com.facebook.presto.iceberg.IcebergTableLayoutHandle;
import com.facebook.presto.iceberg.IcebergTableProperties;
import com.facebook.presto.iceberg.derivedColumn.DerivedColumnArgumentSpec;
import com.facebook.presto.iceberg.derivedColumn.DerivedColumnRef;
import com.facebook.presto.iceberg.derivedColumn.DerivedColumnUDFSpec;
import com.facebook.presto.iceberg.transaction.IcebergTransactionManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.expressions.LogicalRowExpressions.FALSE_CONSTANT;
import static com.facebook.presto.iceberg.IcebergTableType.DATA;
import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class IcebergDerivedColumnOptimizer
        implements ConnectorPlanOptimizer
{
    private static final Logger LOG = Logger.get(IcebergDerivedColumnOptimizer.class);
    private final IcebergTableProperties tableProperties;
    private final IcebergTransactionManager transactionManager;
    private StandardFunctionResolution functionResolution;
    private RowExpressionService rowExpressionService;
    private FunctionMetadataManager functionMetadataManager;

    public IcebergDerivedColumnOptimizer(IcebergTableProperties tableProperties, IcebergTransactionManager transactionManager, StandardFunctionResolution functionResolution, RowExpressionService rowExpressionService, FunctionMetadataManager functionMetadataManager)
    {
        this.tableProperties = tableProperties;
        this.transactionManager = transactionManager;
        this.functionResolution = functionResolution;
        this.rowExpressionService = rowExpressionService;
        this.functionMetadataManager = functionMetadataManager;
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        return rewriteWith(new DerivedColumnRewriter(tableProperties, functionResolution, rowExpressionService, functionMetadataManager,
                transactionManager, idAllocator, session), maxSubplan);
    }

    private static class DerivedColumnRewriter
            extends ConnectorPlanRewriter<Void>
    {
        private final ConnectorSession session;
        private final RowExpressionService rowExpressionService;
        private final IcebergTableProperties tableProperties1;
        private final StandardFunctionResolution functionResolution;
        private final FunctionMetadataManager functionMetadataManager;
        private final PlanNodeIdAllocator idAllocator;
        private final IcebergTransactionManager transactionManager;

        public DerivedColumnRewriter(
                IcebergTableProperties tableProperties,
                StandardFunctionResolution functionResolution,
                RowExpressionService rowExpressionService,
                FunctionMetadataManager functionMetadataManager,
                IcebergTransactionManager transactionManager,
                PlanNodeIdAllocator idAllocator,
                ConnectorSession session)
        {
            this.tableProperties1 = tableProperties;
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
            TableHandle handle = tableScan.getTable();
            IcebergAbstractMetadata metadata = (IcebergAbstractMetadata) transactionManager.get(handle.getTransaction());

            IcebergTableHandle tableHandle = (IcebergTableHandle) handle.getConnectorHandle();
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);
            List<String> derivedColumns = tableProperties1.getDerivedColumns(tableMetadata.getProperties());
            if (derivedColumns.isEmpty()) {
                return filter;
            }
            Map<String, ColumnMetadata> columnsMap = tableMetadata.getColumns().stream().filter(col -> !col.isHidden())
                    .collect(Collectors.toMap(ColumnMetadata::getName, col -> col));
            checkState(columnsMap.keySet().containsAll(derivedColumns),
                    format("Incorrect derived column definition, configured derived columns: %s does not exist in table: %s", Joiner.on(',').join(derivedColumns),
                            tableHandle.getIcebergTableName()));

            List<DerivedColumnUDFSpec> derivedColumnUDFSpecs = tableProperties1.getDerivedColumnUDFSpec(tableMetadata.getProperties()).getUdfSpecList();
            Multimap<FunctionHandle, DerivedColumnUDFSpec> derivedColumnUDFSpecMap = ArrayListMultimap.create();

            derivedColumnUDFSpecs.forEach(udfSpec -> {
                FunctionHandle functionHandle = functionResolution.lookupFunction(
                        udfSpec.getCatalog(),
                        udfSpec.getSchema(),
                        udfSpec.getFunctionName(),
                        udfSpec.getParameterTypes().stream().map(this::sqlTypeToPrestoType).toList());
                derivedColumnUDFSpecMap.put(functionHandle, udfSpec);
            });

            RowExpression filterPredicate = filter.getPredicate();
            checkArgument(!filterPredicate.equals(FALSE_CONSTANT), "Filter expression 'FALSE' should not be left to handle here");
            RowExpression filterPredicateRewritten = filterPredicate;

            TableScanNode newTableScan = tableScan;
            if (filterPredicate instanceof CallExpression || filterPredicate instanceof SpecialFormExpression) {
                RewrittenFilter rewrittenFilter = rewriteFilter(tableScan, filterPredicate, derivedColumnUDFSpecMap, columnsMap);
                filterPredicateRewritten = rewrittenFilter.filter;
                newTableScan = rewrittenFilter.tableScanNode;
            }

            return new FilterNode(filter.getSourceLocation(), idAllocator.getNextId(), filter.getStatsEquivalentPlanNode(), newTableScan, filterPredicateRewritten);
        }

        private RewrittenFilter rewriteFilter(
                TableScanNode tableScan,
                RowExpression filterPredicate,
                Multimap<FunctionHandle, DerivedColumnUDFSpec> derivedColumnUDFSpecMap,
                Map<String, ColumnMetadata> columnsMap)
        {
            RowExpression filterPredicateRewritten;
            Set<VariableReferenceExpression> outputVariables = new HashSet<>(tableScan.getOutputVariables());
            Map<VariableReferenceExpression, ColumnHandle> tableAssignments = new HashMap<>(tableScan.getAssignments());
            TableHandle handle = tableScan.getTable();
            ImmutableMap<String, Integer> columnIndexMap = getColumnIndexMap(handle);

            RewrittenCallExp rewrittenCallExp = filterPredicateRewrite(filterPredicate, derivedColumnUDFSpecMap);
            filterPredicateRewritten = rewrittenCallExp.rewrittenPredicate;
            Function<VariableReferenceExpression, IcebergColumnHandle> derivedColumnHandle = varRef -> new IcebergColumnHandle(
                    new ColumnIdentity(columnIndexMap.getOrDefault(varRef.getName(), -2), varRef.getName(), ColumnIdentity.TypeCategory.PRIMITIVE, List.of()),
                    columnsMap.get(varRef.getName()).getType(),
                    Optional.of("derived column"),
                    BaseHiveColumnHandle.ColumnType.REGULAR);
            if (!outputVariables.containsAll(rewrittenCallExp.derivedColumns)) {
                outputVariables.addAll(rewrittenCallExp.derivedColumns);
                tableAssignments.putAll(rewrittenCallExp.derivedColumns.stream()
                        .collect(Collectors.toMap(k -> k, derivedColumnHandle)));
            }
            Optional<ConnectorTableLayoutHandle> newConnectorTableLayoutHandle = handle.getLayout().map(IcebergTableLayoutHandle.class::cast)
                    .map(icebergTableLayoutHandle -> new IcebergTableLayoutHandle(
                            icebergTableLayoutHandle.getPartitionColumns().stream()
                                    .map(IcebergColumnHandle.class::cast).collect(toList()),
                            icebergTableLayoutHandle.getDataColumns(),
                            icebergTableLayoutHandle.getDomainPredicate(),
                            icebergTableLayoutHandle.getRemainingPredicate(),
                            icebergTableLayoutHandle.getPredicateColumns(),
                            Optional.of(ImmutableSet.<IcebergColumnHandle>builder().addAll(icebergTableLayoutHandle.getRequestedColumns().orElse(ImmutableSet.of()))
                                    .addAll(rewrittenCallExp.derivedColumns.stream().map(derivedColumnHandle).collect(Collectors.toSet())).build()),
                            icebergTableLayoutHandle.isPushdownFilterEnabled(),
                            icebergTableLayoutHandle.getPartitionColumnPredicate(),
                            icebergTableLayoutHandle.getPartitions(),
                            icebergTableLayoutHandle.getTable()));

            TableScanNode newTableScan = new TableScanNode(
                    tableScan.getSourceLocation(),
                    tableScan.getId(),
                    new TableHandle(handle.getConnectorId(), handle.getConnectorHandle(), handle.getTransaction(), newConnectorTableLayoutHandle),
                    outputVariables.stream().toList(),
                    tableAssignments,
                    tableScan.getCurrentConstraint(),
                    tableScan.getEnforcedConstraint(),
                    tableScan.getCteMaterializationInfo());
            return new RewrittenFilter(newTableScan, filterPredicateRewritten);
        }

        private static ImmutableMap<String, Integer> getColumnIndexMap(TableHandle handle)
        {
            ImmutableMap<String, Integer> columnIndexMap = ImmutableMap.of();
            if (handle.getLayout().isPresent()) {
                List<Column> dataColumns = ((IcebergTableLayoutHandle) handle.getLayoutHandle()).getDataColumns();
                ImmutableMap.Builder<String, Integer> columnIndexMapBuilder = ImmutableMap.builder();
                for (int i = 0; i < dataColumns.size(); i++) {
                    columnIndexMapBuilder.put(dataColumns.get(i).getName(), i + 1);
                }
                columnIndexMap = columnIndexMapBuilder.build();
            }
            return columnIndexMap;
        }

        private RewrittenCallExp filterPredicateRewrite(
                RowExpression filterPredicate,
                Multimap<FunctionHandle, DerivedColumnUDFSpec> derivedColumnUDFSpecMap)
        {
            List<RowExpression> rewrittenArguments = new LinkedList<>();
            List<VariableReferenceExpression> derivedColumnsAdded = new ArrayList<>();
            List<RowExpression> arguments = getArguments(filterPredicate);
            for (RowExpression filterPredicateArg : arguments) {
                // Step 1. Check arguments of filterPredicate has UDF to be replaced by a derived column.
                if (filterPredicateArg instanceof CallExpression) {
                    CallExpression filterPredicateArgCexp = (CallExpression) filterPredicateArg;
                    // case : If a udf calls another UDF: i.e. filterPredicateArgCexp's arg is also a call expression.
                    if (filterPredicateArgCexp.getArguments().stream().anyMatch(x -> x instanceof CallExpression)) {
                        RewrittenCallExp rewrittenCallExp = filterPredicateRewrite(filterPredicateArgCexp, derivedColumnUDFSpecMap);
                        filterPredicateArgCexp = (CallExpression) rewrittenCallExp.rewrittenPredicate;
                        derivedColumnsAdded.addAll(rewrittenCallExp.derivedColumns);
                    }
                    FunctionHandle functionHandleArg = filterPredicateArgCexp.getFunctionHandle();
                    if (derivedColumnUDFSpecMap.containsKey(functionHandleArg)) { // Possible match !
                        Collection<DerivedColumnUDFSpec> derivedColumnUDFSpecs = derivedColumnUDFSpecMap.get(functionHandleArg);
                        List<DerivedColumnArgumentSpec> argumentSpecList = getDerivedColumnArgumentSpecs(filterPredicateArgCexp);
                        // Next we search for a derived column spec, which exactly matches (including arguments') the call expression (i.e. UDF)
                        Set<DerivedColumnUDFSpec> matchingUDFSpec =
                                derivedColumnUDFSpecs.stream().filter(derivedColumnSpec ->
                                        matchTwoArgumentsList(derivedColumnSpec.getArguments(), argumentSpecList)).collect(Collectors.toSet());
                        // We can either have a exact match or no match, if we get more than one match - that indicates duplicate(redundant) entries in udf spec.
                        checkState(matchingUDFSpec.size() < 2,
                                format("derived-columns: A duplicate UDF configuration found in udf specs, for : %s ", Joiner.on(",").join(matchingUDFSpec)));
                        if (!matchingUDFSpec.isEmpty()) {
                            // Finally swap call expression with variable ref exp, i.e. UDF -> derived column.
                            DerivedColumnUDFSpec derivedColumnUDFSpec = matchingUDFSpec.stream().findFirst().get();
                            String derivedColumnName = derivedColumnUDFSpec.getDerivedColumnName();
                            VariableReferenceExpression derivedColumn = new VariableReferenceExpression(Optional.empty(), derivedColumnName,
                                    sqlTypeToPrestoType(derivedColumnUDFSpec.getReturnType()));
                            rewrittenArguments.add(derivedColumn);
                            derivedColumnsAdded.add(derivedColumn);
                        }
                        else {
                            rewrittenArguments.add(filterPredicateArgCexp);
                        }
                    }
                    else {
                        rewrittenArguments.add(filterPredicateArgCexp);
                    }
                }
                else {
                    rewrittenArguments.add(filterPredicateArg);
                }
            }
            checkState(rewrittenArguments.size() == arguments.size(), "Error rewriting plan, mismatch in rewritten arguments.");
            RowExpression rewrittenExpression = getRewrittenExpression(filterPredicate, rewrittenArguments);
            return new RewrittenCallExp(rewrittenExpression, derivedColumnsAdded);
        }

        private List<DerivedColumnArgumentSpec> getDerivedColumnArgumentSpecs(CallExpression filterPredicateArgCexp)
        {
            // reconstruct the call expressions' arguments as DerivedColumnArgumentSpec for matching
            List<DerivedColumnArgumentSpec> argumentSpecList = new ArrayList<>();
            Integer index = 0;
            for (RowExpression arg : filterPredicateArgCexp.getArguments()) {
                if (arg instanceof VariableReferenceExpression) {
                    VariableReferenceExpression variableReferenceExpression = (VariableReferenceExpression) arg;
                    argumentSpecList.add(new DerivedColumnArgumentSpec(index, variableReferenceExpression.getType().getDisplayName(),
                            variableReferenceExpression.getName(), DerivedColumnRef.COLUMN));
                }
                else if (arg instanceof ConstantExpression) {
                    ConstantExpression constantExpression = (ConstantExpression) arg;
                    argumentSpecList.add(new DerivedColumnArgumentSpec(index, constantExpression.getType().getDisplayName(),
                            constantExpression.getValue().toString(), DerivedColumnRef.CONSTANT));
                }
                else {
                    if (arg instanceof CallExpression) {
                        // if the argument UDF does not have a derived column, then the calling UDF cannot have it either.
                        LOG.debug("We found a function argument a Call expression for which derived col is not found.");
                    }
                    else {
                        LOG.warn("Unexpected: We found a function argument other than a Call, variableReference and a constant expression.");
                    }
                    return ImmutableList.of();
                }
                index++;
            }
            return argumentSpecList;
        }

        private static List<RowExpression> getArguments(RowExpression filterPredicate)
        {
            List<RowExpression> arguments = List.of();
            if (filterPredicate instanceof CallExpression) {
                arguments = ((CallExpression) filterPredicate).getArguments();
            }
            else if (filterPredicate instanceof SpecialFormExpression) {
                arguments = ((SpecialFormExpression) filterPredicate).getArguments();
            }
            return arguments;
        }

        private static RowExpression getRewrittenExpression(RowExpression filterPredicate, List<RowExpression> rewrittenArguments)
        {
            RowExpression rewrittenExpression = filterPredicate;
            if (filterPredicate instanceof CallExpression) {
                rewrittenExpression = new CallExpression(
                        filterPredicate.getSourceLocation(),
                        ((CallExpression) filterPredicate).getDisplayName(),
                        ((CallExpression) filterPredicate).getFunctionHandle(),
                        filterPredicate.getType(),
                        rewrittenArguments);
            }
            else if (filterPredicate instanceof SpecialFormExpression) {
                rewrittenExpression = new SpecialFormExpression(
                        filterPredicate.getSourceLocation(),
                        ((SpecialFormExpression) filterPredicate).getForm(),
                        filterPredicate.getType(),
                        rewrittenArguments);
            }
            return rewrittenExpression;
        }

        private Boolean matchTwoArgumentsList(List<DerivedColumnArgumentSpec> list1, List<DerivedColumnArgumentSpec> list2)
        {
            if (list1.size() != list2.size()) {
                return false;
            }
            for (int i = 0; i < list1.size(); i++) {
                if (!list1.get(i).equals(list2.get(i))) {
                    return false;
                }
            }
            return true;
        }

        private Type sqlTypeToPrestoType(String typeName)
        {
            switch (typeName.toLowerCase()) {
                case "varchar":
                    return VarcharType.createUnboundedVarcharType();
                case "decimal":
                    return DecimalType.createDecimalType();
                case "bigint":
                    return BigintType.BIGINT;
                case "integer":
                    return IntegerType.INTEGER;
                case "smallint":
                    return SmallintType.SMALLINT;
                case "tinyint":
                    return TinyintType.TINYINT;
                case "char":
                    return CharType.createCharType(CharType.MAX_LENGTH);
                default:
                    LOG.error("Unsupported derived column type: " + typeName);
                    throw new IllegalArgumentException("Unsupported derived column type: " + typeName);
            }
        }

        private record RewrittenFilter(TableScanNode tableScanNode, RowExpression filter) {}

        private record RewrittenCallExp(RowExpression rewrittenPredicate, List<VariableReferenceExpression> derivedColumns) {}
    }
}
