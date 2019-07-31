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
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.DiscretePredicates;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.ExpressionDeterminismEvaluator;
import com.facebook.presto.sql.planner.LiteralEncoder;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.relational.OriginalExpressionUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.relational.Expressions.constant;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Converts cardinality-insensitive aggregations (max, min, "distinct") over partition keys
 * into simple metadata queries
 */
public class MetadataQueryOptimizer
        implements PlanOptimizer
{
    private static final Set<String> ALLOWED_FUNCTIONS = ImmutableSet.of("max", "min", "approx_distinct");

    private final Metadata metadata;
    private final LiteralEncoder literalEncoder;

    public MetadataQueryOptimizer(Metadata metadata)
    {
        requireNonNull(metadata, "metadata is null");

        this.metadata = metadata;
        this.literalEncoder = new LiteralEncoder(metadata.getBlockEncodingSerde());
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (!SystemSessionProperties.isOptimizeMetadataQueries(session)) {
            return plan;
        }
        return SimplePlanRewriter.rewriteWith(new Optimizer(session, metadata, literalEncoder, idAllocator), plan, null);
    }

    private static class Optimizer
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Session session;
        private final Metadata metadata;
        private final LiteralEncoder literalEncoder;

        private Optimizer(Session session, Metadata metadata, LiteralEncoder literalEncoder, PlanNodeIdAllocator idAllocator)
        {
            this.session = session;
            this.metadata = metadata;
            this.literalEncoder = literalEncoder;
            this.idAllocator = idAllocator;
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            // supported functions are only MIN/MAX/APPROX_DISTINCT or distinct aggregates
            for (Aggregation aggregation : node.getAggregations().values()) {
                String functionName = metadata.getFunctionManager().getFunctionMetadata(aggregation.getFunctionHandle()).getName();
                if (!ALLOWED_FUNCTIONS.contains(functionName) && !aggregation.isDistinct()) {
                    return context.defaultRewrite(node);
                }
            }

            Optional<TableScanNode> result = findTableScan(node.getSource());
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
            DiscretePredicates predicates = layout.getDiscretePredicates().get();

            // the optimization is only valid if the aggregation node only relies on partition keys
            if (!predicates.getColumns().containsAll(columns.values())) {
                return context.defaultRewrite(node);
            }

            ImmutableList.Builder<List<RowExpression>> rowsBuilder = ImmutableList.builder();
            for (TupleDomain<ColumnHandle> domain : predicates.getPredicates()) {
                if (!domain.isNone()) {
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
            }

            // replace the tablescan node with a values node
            ValuesNode valuesNode = new ValuesNode(idAllocator.getNextId(), inputs, rowsBuilder.build());
            return SimplePlanRewriter.rewriteWith(new Replacer(valuesNode), node);
        }

        private static Optional<TableScanNode> findTableScan(PlanNode source)
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
                    if (!Iterables.all(project.getAssignments().getExpressions().stream().map(OriginalExpressionUtils::castToExpression).collect(toList()), ExpressionDeterminismEvaluator::isDeterministic)) {
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
