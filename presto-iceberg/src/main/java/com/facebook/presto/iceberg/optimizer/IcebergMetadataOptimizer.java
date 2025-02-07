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

import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.TupleDomain.ColumnDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergTransactionManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.DiscretePredicates;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.ExpressionOptimizer.Level;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getRowsForMetadataOptimizationThreshold;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class IcebergMetadataOptimizer
        implements ConnectorPlanOptimizer
{
    // Min/Max could be folded into LEAST/GREATEST
    private static final Map<String, String> AGGREGATION_SCALAR_MAPPING = ImmutableMap.of(
            "max", "greatest",
            "min", "least");

    private final FunctionMetadataManager functionMetadataManager;
    private final TypeManager typeManager;
    private final IcebergTransactionManager icebergTransactionManager;
    private final RowExpressionService rowExpressionService;
    private final StandardFunctionResolution functionResolution;

    public IcebergMetadataOptimizer(FunctionMetadataManager functionMetadataManager,
            TypeManager typeManager,
            IcebergTransactionManager icebergTransactionManager,
            RowExpressionService rowExpressionService,
            StandardFunctionResolution functionResolution)
    {
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.icebergTransactionManager = requireNonNull(icebergTransactionManager, "icebergTransactionManager is null");
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        int rowsForMetadataOptimizationThreshold = getRowsForMetadataOptimizationThreshold(session);
        Optimizer optimizer = new Optimizer(session, idAllocator,
                functionMetadataManager,
                typeManager,
                icebergTransactionManager,
                rowExpressionService,
                functionResolution,
                rowsForMetadataOptimizationThreshold);
        PlanNode rewrittenPlan = ConnectorPlanRewriter.rewriteWith(optimizer, maxSubplan, null);
        return rewrittenPlan;
    }

    private static class Optimizer
            extends ConnectorPlanRewriter<Void>
    {
        private final ConnectorSession connectorSession;
        private final PlanNodeIdAllocator idAllocator;
        private final FunctionMetadataManager functionMetadataManager;
        private final TypeManager typeManager;
        private final IcebergTransactionManager icebergTransactionManager;
        private final RowExpressionService rowExpressionService;
        private final StandardFunctionResolution functionResolution;
        private final int rowsForMetadataOptimizationThreshold;
        private final List<Predicate<FunctionHandle>> allowedFunctionsPredicates;

        private Optimizer(ConnectorSession connectorSession,
                PlanNodeIdAllocator idAllocator,
                FunctionMetadataManager functionMetadataManager,
                TypeManager typeManager,
                IcebergTransactionManager icebergTransactionManager,
                RowExpressionService rowExpressionService,
                StandardFunctionResolution functionResolution,
                int rowsForMetadataOptimizationThreshold)
        {
            checkArgument(rowsForMetadataOptimizationThreshold >= 0, "The value of `rowsForMetadataOptimizationThreshold` should not less than 0");
            this.connectorSession = connectorSession;
            this.idAllocator = idAllocator;
            this.functionMetadataManager = functionMetadataManager;
            this.icebergTransactionManager = icebergTransactionManager;
            this.rowExpressionService = rowExpressionService;
            this.functionResolution = functionResolution;
            this.typeManager = typeManager;
            this.rowsForMetadataOptimizationThreshold = rowsForMetadataOptimizationThreshold;
            this.allowedFunctionsPredicates = ImmutableList.of(
                    functionResolution::isMaxFunction,
                    functionResolution::isMinFunction,
                    functionResolution::isApproximateCountDistinctFunction);
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            // supported functions are only MIN/MAX/APPROX_DISTINCT or distinct aggregates
            for (Aggregation aggregation : node.getAggregations().values()) {
                if (allowedFunctionsPredicates.stream().noneMatch(
                        pred -> pred.test(aggregation.getFunctionHandle())) && !aggregation.isDistinct()) {
                    return context.defaultRewrite(node);
                }
            }

            Optional<TableScanNode> result = findTableScan(node.getSource(), rowExpressionService.getDeterminismEvaluator());
            if (!result.isPresent()) {
                return context.defaultRewrite(node);
            }

            // verify all outputs of table scan are partition keys
            TableScanNode tableScan = result.get();

            ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> columnBuilder = ImmutableMap.builder();

            List<VariableReferenceExpression> inputs = tableScan.getOutputVariables();
            for (VariableReferenceExpression variable : inputs) {
                ColumnHandle column = tableScan.getAssignments().get(variable);
                columnBuilder.put(variable, column);
            }

            Map<VariableReferenceExpression, ColumnHandle> columns = columnBuilder.build();

            // Materialize the list of partitions and replace the TableScan node
            // with a Values node
            ConnectorTableLayout layout;
            if (!tableScan.getTable().getLayout().isPresent()) {
                layout = getConnectorMetadata(tableScan.getTable()).getTableLayoutForConstraint(connectorSession, tableScan.getTable().getConnectorHandle(), Constraint.alwaysTrue(), Optional.empty()).getTableLayout();
            }
            else {
                layout = getConnectorMetadata(tableScan.getTable()).getTableLayout(connectorSession, tableScan.getTable().getLayout().get());
            }

            if (!layout.getDiscretePredicates().isPresent()) {
                return context.defaultRewrite(node);
            }

            DiscretePredicates discretePredicates = layout.getDiscretePredicates().get();

            // the optimization is only valid if there is no filter on non-partition columns
            if (layout.getPredicate().getColumnDomains().isPresent()) {
                List<ColumnHandle> predicateColumns = layout.getPredicate().getColumnDomains().get().stream()
                        .map(ColumnDomain::getColumn)
                        .collect(toImmutableList());
                if (!discretePredicates.getColumns().containsAll(predicateColumns)) {
                    return context.defaultRewrite(node);
                }
            }

            // Remaining predicate after tuple domain pushdown in getTableLayout(). This doesn't have overlap with discretePredicates.
            // So it only references non-partition columns. Disable the optimization in this case.
            Optional<RowExpression> remainingPredicate = layout.getRemainingPredicate();
            if (remainingPredicate.isPresent() && !remainingPredicate.get().equals(TRUE_CONSTANT)) {
                return context.defaultRewrite(node);
            }

            // the optimization is only valid if the aggregation node only relies on partition keys
            if (!discretePredicates.getColumns().containsAll(columns.values())) {
                return context.defaultRewrite(node);
            }

            if (isReducible(node, inputs)) {
                // Fold min/max aggregations to a constant value
                return reduce(node, inputs, columns, context, discretePredicates);
            }

            // When `rowsForMetadataOptimizationThreshold == 0`, or partitions number exceeds the threshold, skip the optimization
            if (rowsForMetadataOptimizationThreshold == 0 || Iterables.size(discretePredicates.getPredicates()) > rowsForMetadataOptimizationThreshold) {
                return context.defaultRewrite(node);
            }

            ImmutableList.Builder<List<RowExpression>> rowsBuilder = ImmutableList.builder();
            for (TupleDomain<ColumnHandle> domain : discretePredicates.getPredicates()) {
                if (domain.isNone()) {
                    continue;
                }
                Map<ColumnHandle, NullableValue> entries = TupleDomain.extractFixedValues(domain).get();

                ImmutableList.Builder<RowExpression> rowBuilder = ImmutableList.builder();
                // for each input column, add a literal expression using the entry value
                for (VariableReferenceExpression input : inputs) {
                    ColumnHandle column = columns.get(input);
                    NullableValue value = entries.get(column);
                    if (value == null) {
                        // partition key does not have a single value, so bail out to be safe
                        return context.defaultRewrite(node);
                    }
                    else {
                        rowBuilder.add(new ConstantExpression(Optional.empty(), value.getValue(), input.getType()));
                    }
                }
                rowsBuilder.add(rowBuilder.build());
            }

            // replace the tablescan node with a values node
            return ConnectorPlanRewriter.rewriteWith(new Replacer(new ValuesNode(node.getSourceLocation(), idAllocator.getNextId(), inputs, rowsBuilder.build(), Optional.empty())), node);
        }

        private boolean isReducible(AggregationNode node, List<VariableReferenceExpression> inputs)
        {
            // The aggregation is reducible when there is no group by key
            if (node.getAggregations().isEmpty() || !node.getGroupingKeys().isEmpty() || !(node.getSource() instanceof TableScanNode)) {
                return false;
            }
            for (Aggregation aggregation : node.getAggregations().values()) {
                FunctionMetadata functionMetadata = functionMetadataManager.getFunctionMetadata(aggregation.getFunctionHandle());
                if (!AGGREGATION_SCALAR_MAPPING.containsKey(functionMetadata.getName().getObjectName()) ||
                        functionMetadata.getArgumentTypes().size() > 1 ||
                        !inputs.containsAll(aggregation.getCall().getArguments())) {
                    return false;
                }
            }
            return true;
        }

        private PlanNode reduce(
                AggregationNode node,
                List<VariableReferenceExpression> inputs,
                Map<VariableReferenceExpression, ColumnHandle> columns,
                RewriteContext<Void> context,
                DiscretePredicates predicates)
        {
            // Fold min/max aggregations to a constant value
            ImmutableMap.Builder<VariableReferenceExpression, List<RowExpression>> inputColumnValuesBuilder = ImmutableMap.builder();
            // For each input partition column, we keep one tuple domain for each constant value. When we get the resulting value, we will get the corresponding tuple domain and
            // check if the partition stats can be trusted.
            ImmutableMap.Builder<VariableReferenceExpression, Map<RowExpression, TupleDomain<ColumnHandle>>> inputValueToDomainBuilder = ImmutableMap.builder();
            for (VariableReferenceExpression input : inputs) {
                ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();
                Map<RowExpression, TupleDomain<ColumnHandle>> valueToDomain = new HashMap<>();
                ColumnHandle column = columns.get(input);
                // for each input column, add a literal expression using the entry value
                for (TupleDomain<ColumnHandle> domain : predicates.getPredicates()) {
                    if (domain.isNone()) {
                        continue;
                    }
                    Map<ColumnHandle, NullableValue> entries = TupleDomain.extractFixedValues(domain).get();
                    NullableValue value = entries.get(column);
                    if (value == null) {
                        // partition key does not have a single value, so bail out to be safe
                        return context.defaultRewrite(node);
                    }
                    // min/max ignores null value
                    else if (value.getValue() != null) {
                        Type type = input.getType();
                        ConstantExpression constantExpression = new ConstantExpression(Optional.empty(), value.getValue(), type);
                        arguments.add(constantExpression);
                        valueToDomain.putIfAbsent(constantExpression, domain);
                    }
                }
                inputColumnValuesBuilder.put(input, arguments.build());
                inputValueToDomainBuilder.put(input, valueToDomain);
            }
            Map<VariableReferenceExpression, List<RowExpression>> inputColumnValues = inputColumnValuesBuilder.build();

            Assignments.Builder assignmentsBuilder = Assignments.builder();
            for (VariableReferenceExpression outputVariable : node.getOutputVariables()) {
                Aggregation aggregation = node.getAggregations().get(outputVariable);
                RowExpression inputVariable = getOnlyElement(aggregation.getArguments());
                RowExpression result = evaluateMinMax(
                        functionMetadataManager.getFunctionMetadata(node.getAggregations().get(outputVariable).getFunctionHandle()),
                        inputColumnValues.get(inputVariable));
                assignmentsBuilder.put(outputVariable, result);
            }
            Assignments assignments = assignmentsBuilder.build();
            ValuesNode valuesNode = new ValuesNode(node.getSourceLocation(), idAllocator.getNextId(), node.getOutputVariables(), ImmutableList.of(new ArrayList<>(assignments.getExpressions())), Optional.empty());
            return new ProjectNode(node.getSourceLocation(), idAllocator.getNextId(), valuesNode, assignments, LOCAL);
        }

        private RowExpression evaluateMinMax(FunctionMetadata aggregationFunctionMetadata, List<RowExpression> arguments)
        {
            Type returnType = typeManager.getType(aggregationFunctionMetadata.getReturnType());
            if (arguments.isEmpty()) {
                return new ConstantExpression(Optional.empty(), null, returnType);
            }

            String scalarFunctionName = AGGREGATION_SCALAR_MAPPING.get(aggregationFunctionMetadata.getName().getObjectName());
            while (arguments.size() > 1) {
                List<RowExpression> reducedArguments = new ArrayList<>();
                // We fold for every 100 values because GREATEST/LEAST has argument count limit
                for (List<RowExpression> partitionedArguments : Lists.partition(arguments, 100)) {
                    FunctionHandle functionHandle;
                    if (scalarFunctionName.equals("greatest")) {
                        functionHandle = functionResolution.greatestFunction(partitionedArguments.stream().map(RowExpression::getType).collect(toImmutableList()));
                    }
                    else if (scalarFunctionName.equals("least")) {
                        functionHandle = functionResolution.leastFunction(partitionedArguments.stream().map(RowExpression::getType).collect(toImmutableList()));
                    }
                    else {
                        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "unsupported function: " + scalarFunctionName);
                    }

                    RowExpression reducedValue = rowExpressionService.getExpressionOptimizer(connectorSession).optimize(
                            new CallExpression(
                                    Optional.empty(),
                                    scalarFunctionName,
                                    functionHandle,
                                    returnType,
                                    partitionedArguments),
                            Level.EVALUATED,
                            connectorSession,
                            variableReferenceExpression -> null);
                    checkArgument(reducedValue instanceof ConstantExpression, "unexpected expression type: %s", reducedValue.getClass().getSimpleName());
                    reducedArguments.add(reducedValue);
                }
                arguments = reducedArguments;
            }
            return getOnlyElement(arguments);
        }

        private static Optional<TableScanNode> findTableScan(PlanNode source, DeterminismEvaluator determinismEvaluator)
        {
            while (true) {
                // allow any chain of linear transformations
                if (source instanceof MarkDistinctNode ||
                        source instanceof FilterNode ||
                        source instanceof SortNode) {
                    source = source.getSources().get(0);
                }
                else if (source instanceof ProjectNode) {
                    // verify projections are deterministic
                    ProjectNode project = (ProjectNode) source;
                    if (!Iterables.all(project.getAssignments().getExpressions(), determinismEvaluator::isDeterministic)) {
                        return Optional.empty();
                    }
                    source = project.getSource();
                }
                else if (source instanceof TableScanNode) {
                    return Optional.of((TableScanNode) source);
                }
                else {
                    return Optional.empty();
                }
            }
        }

        private ConnectorMetadata getConnectorMetadata(TableHandle tableHandle)
        {
            requireNonNull(icebergTransactionManager, "icebergTransactionManager is null");
            ConnectorMetadata metadata = icebergTransactionManager.get(tableHandle.getTransaction());
            checkState(metadata instanceof IcebergAbstractMetadata, "metadata must be IcebergAbstractMetadata");
            return metadata;
        }
    }

    private static class Replacer
            extends ConnectorPlanRewriter<Void>
    {
        private final ValuesNode replacement;

        private Replacer(ValuesNode replacement)
        {
            this.replacement = replacement;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            return replacement;
        }
    }
}
