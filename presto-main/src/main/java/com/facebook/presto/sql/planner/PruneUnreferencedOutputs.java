package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.compiler.Symbol;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.FunctionHandle;
import com.facebook.presto.sql.compiler.Type;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Iterables.concat;

/**
 * Removes all computation that does is not referenced transitively from the root of the plan
 *
 * E.g.,
 *
 * Output[$0] -> Project[$0 := $1 + $2, $3 = $4 / $5] -> ...
 *
 * gets rewritten as
 *
 * Output[$0] -> Project[$0 := $1 + $2] -> ...
 */
public class PruneUnreferencedOutputs
        extends PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Map<Symbol, Type> types)
    {
        return plan.accept(new Visitor(types), ImmutableSet.<Symbol>of());
    }

    private static class Visitor
            extends PlanVisitor<Set<Symbol>, PlanNode>
    {
        private final Map<Symbol, Type> types;

        public Visitor(Map<Symbol, Type> types)
        {
            this.types = types;
        }

        @Override
        protected PlanNode visitPlan(PlanNode node, Set<Symbol> expectedOutputs)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        @Override
        public PlanNode visitJoin(JoinNode node, Set<Symbol> expectedOutputs)
        {
            Set<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                    .addAll(expectedOutputs)
                    .addAll(new DependencyExtractor().extract(node.getCriteria()))
                    .build();

            PlanNode left = node.getLeft().accept(this, expectedInputs);
            PlanNode right = node.getRight().accept(this, expectedInputs);

            return new JoinNode(left, right, node.getCriteria());
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, Set<Symbol> expectedOutputs)
        {
            ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                    .addAll(node.getGroupBy());

            ImmutableMap.Builder<Symbol, FunctionHandle> functions = ImmutableMap.builder();
            ImmutableMap.Builder<Symbol, FunctionCall> functionCalls = ImmutableMap.builder();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();

                if (expectedOutputs.contains(symbol)) {
                    FunctionCall call = entry.getValue();
                    expectedInputs.addAll(new DependencyExtractor().extract(call));

                    functionCalls.put(symbol, call);
                    functions.put(symbol, node.getFunctions().get(symbol));
                }
            }

            PlanNode source = node.getSource().accept(this, expectedInputs.build());

            return new AggregationNode(source, node.getGroupBy(), functionCalls.build(), functions.build());
        }

        @Override
        public PlanNode visitTableScan(TableScan node, Set<Symbol> expectedOutputs)
        {
            Map<Symbol, ColumnHandle> assignments = new HashMap<>();
            for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
                Symbol symbol = entry.getKey();

                if (expectedOutputs.contains(symbol) || expectedOutputs.isEmpty() && Type.isNumeric(types.get(symbol))) {
                    assignments.put(symbol, entry.getValue());

                    if (expectedOutputs.isEmpty()) {
                        break;
                    }
                }
            }

            if (assignments.isEmpty()) {
                Map.Entry<Symbol, ColumnHandle> first = Iterables.getFirst(node.getAssignments().entrySet(), null);
                assignments.put(first.getKey(), first.getValue());
            }

            return new TableScan(node.getTable(), assignments);
        }

        @Override
        public PlanNode visitFilter(FilterNode node, Set<Symbol> expectedOutputs)
        {
            Set<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                    .addAll(new DependencyExtractor().extract(node.getPredicate()))
                    .addAll(expectedOutputs)
                    .build();

            PlanNode source = node.getSource().accept(this, expectedInputs);

            List<Symbol> outputs = ImmutableList.copyOf(Iterables.filter(source.getOutputSymbols(), in(expectedOutputs)));

            // TODO: remove once filterandproject supports empty projections
            if (expectedOutputs.isEmpty()) {
                outputs = ImmutableList.copyOf(Iterables.limit(source.getOutputSymbols(), 1));
            }

            return new FilterNode(source, node.getPredicate(), outputs);
        }

        @Override
        public PlanNode visitProject(ProjectNode node, Set<Symbol> expectedOutputs)
        {
            ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.builder();

            ImmutableMap.Builder<Symbol, Expression> builder = ImmutableMap.builder();
            for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                Symbol output = node.getOutputSymbols().get(i);
                Expression expression = node.getExpressions().get(i);

                if (expectedOutputs.contains(output)) {
                    expectedInputs.addAll(new DependencyExtractor().extract(expression));
                    builder.put(output, expression);
                }
            }

            PlanNode source = node.getSource().accept(this, expectedInputs.build());

            return new ProjectNode(source, builder.build());
        }

        @Override
        public PlanNode visitOutput(OutputPlan node, Set<Symbol> expectedOutputs)
        {
            Set<Symbol> expectedInputs = ImmutableSet.copyOf(node.getAssignments().values());
            PlanNode source = node.getSource().accept(this, expectedInputs);
            return new OutputPlan(source, node.getColumnNames(), node.getAssignments());
        }

        @Override
        public PlanNode visitLimit(LimitNode node, Set<Symbol> expectedOutputs)
        {
            PlanNode source = node.getSource().accept(this, expectedOutputs);
            return new LimitNode(source, node.getCount());
        }

        @Override
        public PlanNode visitTopN(TopNNode node, Set<Symbol> expectedOutputs)
        {
            Set<Symbol> expectedInputs = ImmutableSet.copyOf(concat(expectedOutputs, node.getOrderBy()));

            PlanNode source = node.getSource().accept(this, expectedInputs);

            return new TopNNode(source, node.getCount(), node.getOrderBy(), node.getOrderings());
        }
    }
}
