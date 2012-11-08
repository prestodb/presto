package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.sql.compiler.Slot;
import com.facebook.presto.sql.compiler.Type;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

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
    public PlanNode optimize(PlanNode plan)
    {
        return plan.accept(new Visitor(), ImmutableSet.<Slot>of());
    }

    private static class Visitor
            extends PlanVisitor<Set<Slot>, PlanNode>
    {
        @Override
        protected PlanNode visitPlan(PlanNode node, Set<Slot> expectedOutputs)
        {
            throw new UnsupportedOperationException("not yet implemented: " + getClass().getName());
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, Set<Slot> expectedOutputs)
        {
            ImmutableSet.Builder<Slot> expectedInputs = ImmutableSet.<Slot>builder()
                    .addAll(node.getGroupBy());

            ImmutableMap.Builder<Slot, FunctionInfo> functionInfos = ImmutableMap.builder();
            ImmutableMap.Builder<Slot, FunctionCall> functionCalls = ImmutableMap.builder();
            for (Map.Entry<Slot, FunctionCall> entry : node.getAggregations().entrySet()) {
                Slot slot = entry.getKey();

                if (expectedOutputs.contains(slot)) {
                    FunctionCall call = entry.getValue();
                    expectedInputs.addAll(new DependencyExtractor().extract(call));

                    functionCalls.put(slot, call);
                    functionInfos.put(slot, node.getFunctionInfos().get(slot));
                }
            }

            PlanNode source = node.getSource().accept(this, expectedInputs.build());

            return new AggregationNode(source, node.getGroupBy(), functionCalls.build(), functionInfos.build());
        }

        @Override
        public PlanNode visitAlign(AlignNode node, Set<Slot> expectedOutputs)
        {
            ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();
            ImmutableList.Builder<Slot> outputs = ImmutableList.builder();
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNode source = node.getSources().get(i);
                Slot slot = node.getOutputs().get(i);

                if (expectedOutputs.contains(slot) || expectedOutputs.isEmpty() && (slot.getType() == Type.LONG || slot.getType() == Type.DOUBLE)) {
                    sources.add(source);
                    outputs.add(slot);

                    if (expectedOutputs.isEmpty()) {
                        break;
                    }
                }
            }

            List<PlanNode> sourceNodes = sources.build();
            List<Slot> outputSlots = outputs.build();

            if (sourceNodes.isEmpty()) {
                sourceNodes = ImmutableList.of(node.getSources().get(0));
                outputSlots = ImmutableList.of(node.getOutputs().get(0));
            }

            return new AlignNode(sourceNodes, outputSlots);
        }

        @Override
        public PlanNode visitColumnScan(ColumnScan node, Set<Slot> expectedOutputs)
        {
            Preconditions.checkArgument(expectedOutputs.equals(expectedOutputs), "ColumnScan outputs should match expected outputs");
            return node;
        }

        @Override
        public PlanNode visitFilter(FilterNode node, Set<Slot> expectedOutputs)
        {
            Set<Slot> expectedInputs = ImmutableSet.<Slot>builder()
                    .addAll(new DependencyExtractor().extract(node.getPredicate()))
                    .addAll(expectedOutputs)
                    .build();

            PlanNode source = node.getSource().accept(this, expectedInputs);

            List<Slot> outputs = ImmutableList.copyOf(Iterables.filter(source.getOutputs(), in(expectedOutputs)));

            // TODO: remove once filterandproject supports empty projections
            if (expectedOutputs.isEmpty()) {
                outputs = ImmutableList.copyOf(Iterables.limit(source.getOutputs(), 1));
            }

            return new FilterNode(source, node.getPredicate(), outputs);
        }

        @Override
        public PlanNode visitProject(ProjectNode node, Set<Slot> expectedOutputs)
        {
            ImmutableSet.Builder<Slot> expectedInputs = ImmutableSet.builder();

            ImmutableMap.Builder<Slot, Expression> builder = ImmutableMap.<Slot, Expression>builder();
            for (int i = 0; i < node.getOutputs().size(); i++) {
                Slot output = node.getOutputs().get(i);
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
        public PlanNode visitOutput(OutputPlan node, Set<Slot> expectedOutputs)
        {
            ImmutableSet<Slot> expectedInputs = ImmutableSet.copyOf(node.getAssignments().values());
            PlanNode source = node.getSource().accept(this, expectedInputs);
            return new OutputPlan(source, node.getColumnNames(), node.getAssignments());
        }

        @Override
        public PlanNode visitLimit(LimitNode node, Set<Slot> expectedOutputs)
        {
            PlanNode source = node.getSource().accept(this, expectedOutputs);
            return new LimitNode(source, node.getCount());
        }

        @Override
        public PlanNode visitTopN(TopNNode node, Set<Slot> expectedOutputs)
        {
            Set<Slot> expectedInputs = ImmutableSet.copyOf(concat(expectedOutputs, node.getOrderBy()));

            PlanNode source = node.getSource().accept(this, expectedInputs);

            return new TopNNode(source, node.getCount(), node.getOrderBy(), node.getOrderings());
        }
    }
}
