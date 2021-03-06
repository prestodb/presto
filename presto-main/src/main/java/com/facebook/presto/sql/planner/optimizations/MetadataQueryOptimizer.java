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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.TupleDomain.ColumnDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.DiscretePredicates;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.sql.planner.RowExpressionInterpreter.evaluateConstantRowExpression;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

/**
 * Converts cardinality-insensitive aggregations (max, min, "distinct") over partition keys
 * into simple metadata queries
 */
public class MetadataQueryOptimizer
        implements PlanOptimizer
{
    private static final Set<QualifiedObjectName> ALLOWED_FUNCTIONS = ImmutableSet.of(
            QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "max"),
            QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "min"),
            QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "approx_distinct"));

    // Min/Max could be folded into LEAST/GREATEST
    private static final Map<QualifiedObjectName, QualifiedObjectName> AGGREGATION_SCALAR_MAPPING = ImmutableMap.of(
            QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "max"), QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "greatest"),
            QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "min"), QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "least"));

    private final Metadata metadata;

    public MetadataQueryOptimizer(Metadata metadata)
    {
        requireNonNull(metadata, "metadata is null");

        this.metadata = metadata;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (!SystemSessionProperties.isOptimizeMetadataQueries(session)) {
            return plan;
        }
        return SimplePlanRewriter.rewriteWith(new Optimizer(session, metadata, idAllocator), plan, null);
    }

    private static class Optimizer
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Session session;
        private final Metadata metadata;
        private final RowExpressionDeterminismEvaluator determinismEvaluator;

        private Optimizer(Session session, Metadata metadata, PlanNodeIdAllocator idAllocator)
        {
            this.session = session;
            this.metadata = metadata;
            this.idAllocator = idAllocator;
            this.determinismEvaluator = new RowExpressionDeterminismEvaluator(metadata);
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            // supported functions are only MIN/MAX/APPROX_DISTINCT or distinct aggregates
            for (Aggregation aggregation : node.getAggregations().values()) {
                QualifiedObjectName functionName = metadata.getFunctionAndTypeManager().getFunctionMetadata(aggregation.getFunctionHandle()).getName();
                if (!ALLOWED_FUNCTIONS.contains(functionName) && !aggregation.isDistinct()) {
                    return context.defaultRewrite(node);
                }
            }

            Optional<TableScanNode> result = findTableScan(node.getSource(), determinismEvaluator);
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
            TableLayout layout;
            if (!tableScan.getTable().getLayout().isPresent()) {
                layout = metadata.getLayout(session, tableScan.getTable(), Constraint.alwaysTrue(), Optional.empty()).getLayout();
            }
            else {
                layout = metadata.getLayout(session, tableScan.getTable());
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

            // the optimization is only valid if the aggregation node only relies on partition keys
            if (!discretePredicates.getColumns().containsAll(columns.values())) {
                return context.defaultRewrite(node);
            }

            if (isReducible(node, inputs)) {
                // Fold min/max aggregations to a constant value
                return reduce(node, inputs, columns, context, discretePredicates);
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
                        rowBuilder.add(constant(value.getValue(), input.getType()));
                    }
                }
                rowsBuilder.add(rowBuilder.build());
            }

            // replace the tablescan node with a values node
            return SimplePlanRewriter.rewriteWith(new Replacer(new ValuesNode(idAllocator.getNextId(), inputs, rowsBuilder.build())), node);
        }

        private boolean isReducible(AggregationNode node, List<VariableReferenceExpression> inputs)
        {
            // The aggregation is reducible when there is no group by key
            if (node.getAggregations().isEmpty() || !node.getGroupingKeys().isEmpty() || !(node.getSource() instanceof TableScanNode)) {
                return false;
            }
            for (Aggregation aggregation : node.getAggregations().values()) {
                FunctionMetadata functionMetadata = metadata.getFunctionAndTypeManager().getFunctionMetadata(aggregation.getFunctionHandle());
                if (!AGGREGATION_SCALAR_MAPPING.containsKey(functionMetadata.getName()) ||
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
            for (VariableReferenceExpression input : inputs) {
                ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();
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
                        arguments.add(constant(value.getValue(), type));
                    }
                }
                inputColumnValuesBuilder.put(input, arguments.build());
            }
            Map<VariableReferenceExpression, List<RowExpression>> inputColumnValues = inputColumnValuesBuilder.build();

            Assignments.Builder assignmentsBuilder = Assignments.builder();
            for (VariableReferenceExpression outputVariable : node.getOutputVariables()) {
                Aggregation aggregation = node.getAggregations().get(outputVariable);
                assignmentsBuilder.put(
                        outputVariable,
                        evaluateMinMax(
                                metadata.getFunctionAndTypeManager().getFunctionMetadata(node.getAggregations().get(outputVariable).getFunctionHandle()),
                                inputColumnValues.get(getOnlyElement(aggregation.getArguments()))));
            }
            Assignments assignments = assignmentsBuilder.build();
            ValuesNode valuesNode = new ValuesNode(idAllocator.getNextId(), node.getOutputVariables(), ImmutableList.of(new ArrayList<>(assignments.getExpressions())));
            return new ProjectNode(idAllocator.getNextId(), valuesNode, assignments, LOCAL);
        }

        private RowExpression evaluateMinMax(FunctionMetadata aggregationFunctionMetadata, List<RowExpression> arguments)
        {
            Type returnType = metadata.getFunctionAndTypeManager().getType(aggregationFunctionMetadata.getReturnType());
            if (arguments.isEmpty()) {
                return constant(null, returnType);
            }

            String scalarFunctionName = AGGREGATION_SCALAR_MAPPING.get(aggregationFunctionMetadata.getName()).getObjectName();
            ConnectorSession connectorSession = session.toConnectorSession();
            while (arguments.size() > 1) {
                List<RowExpression> reducedArguments = new ArrayList<>();
                // We fold for every 100 values because GREATEST/LEAST has argument count limit
                for (List<RowExpression> partitionedArguments : Lists.partition(arguments, 100)) {
                    Object reducedValue = evaluateConstantRowExpression(
                            call(
                                    metadata.getFunctionAndTypeManager(),
                                    scalarFunctionName,
                                    returnType,
                                    partitionedArguments),
                            metadata,
                            connectorSession);
                    reducedArguments.add(constant(reducedValue, returnType));
                }
                arguments = reducedArguments;
            }
            return getOnlyElement(arguments);
        }

        private static Optional<TableScanNode> findTableScan(PlanNode source, RowExpressionDeterminismEvaluator determinismEvaluator)
        {
            while (true) {
                // allow any chain of linear transformations
                if (source instanceof MarkDistinctNode ||
                        source instanceof FilterNode ||
                        source instanceof LimitNode ||
                        source instanceof TopNNode ||
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
    }

    private static class Replacer
            extends SimplePlanRewriter<Void>
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
