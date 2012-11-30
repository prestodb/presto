package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.FunctionHandle;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.sql.compiler.SessionMetadata;
import com.facebook.presto.sql.compiler.Symbol;
import com.facebook.presto.sql.compiler.SymbolAllocator;
import com.facebook.presto.sql.compiler.Type;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.planner.AggregationNode.Step.FINAL;
import static com.facebook.presto.sql.planner.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.AggregationNode.Step.SINGLE;

/**
 * Splits a logical plan into fragments that can be shipped and executed on distributed nodes
 */
public class FragmentPlanner
{
    private final SessionMetadata metadata;

    public FragmentPlanner(SessionMetadata metadata)
    {
        this.metadata = metadata;
    }

    public List<PlanFragment> createFragments(PlanNode plan, SymbolAllocator allocator, boolean createSingleNodePlan)
    {
        Visitor visitor = new Visitor(allocator, createSingleNodePlan);
        plan.accept(visitor, null);

        return Lists.transform(visitor.getFragments(), PlanFragmentBuilder.buildFragmentFunction(allocator.getTypes()));
    }

    private class Visitor
            extends PlanVisitor<Void, PlanFragmentBuilder>
    {
        private int fragmentId = 0;
        private final LinkedList<PlanFragmentBuilder> fragments = new LinkedList<>();

        private final SymbolAllocator allocator;
        private final boolean createSingleNodePlan;

        public Visitor(SymbolAllocator allocator, boolean createSingleNodePlan)
        {
            this.allocator = allocator;
            this.createSingleNodePlan = createSingleNodePlan;
        }

        @Override
        public PlanFragmentBuilder visitAggregation(AggregationNode node, Void context)
        {
            PlanFragmentBuilder current = node.getSource().accept(this, context);

            if (!current.isPartitioned()) {
                // add the aggregation node as the root of the current fragment
                current.setRoot(new AggregationNode(current.getRoot(), node.getGroupBy(), node.getAggregations(), node.getFunctions(), SINGLE));
                return current;
            }

            Map<Symbol, FunctionCall> finalCalls = new HashMap<>();
            Map<Symbol, FunctionCall> intermediateCalls = new HashMap<>();
            Map<Symbol, FunctionHandle> intermediateFunctions = new HashMap<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                FunctionHandle functionHandle = node.getFunctions().get(entry.getKey());
                FunctionInfo function = metadata.getFunction(functionHandle);

                Symbol intermediateSymbol = allocator.newSymbol(function.getName().getSuffix(), Type.fromRaw(function.getIntermediateType()));
                intermediateCalls.put(intermediateSymbol, entry.getValue());
                intermediateFunctions.put(intermediateSymbol, functionHandle);

                // rewrite final aggregation in terms of intermediate function
                finalCalls.put(entry.getKey(), new FunctionCall(function.getName(), ImmutableList.<Expression>of(new QualifiedNameReference(intermediateSymbol.toQualifiedName()))));
            }

            current.setRoot(new AggregationNode(current.getRoot(), node.getGroupBy(), intermediateCalls, intermediateFunctions, PARTIAL));

            // create merge + aggregation plan
            ExchangeNode source = new ExchangeNode(current.getId(), current.getRoot().getOutputSymbols());
            AggregationNode merged = new AggregationNode(source, node.getGroupBy(), finalCalls, node.getFunctions(), FINAL);
            current = newPlanFragment(merged, false);

            return current;
        }

        @Override
        public PlanFragmentBuilder visitFilter(FilterNode node, Void context)
        {
            PlanFragmentBuilder current = node.getSource().accept(this, context);
            current.setRoot(new FilterNode(current.getRoot(), node.getPredicate(), node.getOutputSymbols()));
            return current;
        }

        @Override
        public PlanFragmentBuilder visitProject(ProjectNode node, Void context)
        {
            PlanFragmentBuilder current = node.getSource().accept(this, context);
            current.setRoot(new ProjectNode(current.getRoot(), node.getOutputMap()));
            return current;
        }

        @Override
        public PlanFragmentBuilder visitTopN(TopNNode node, Void context)
        {
            PlanFragmentBuilder current = node.getSource().accept(this, context);

            current.setRoot(new TopNNode(current.getRoot(), node.getCount(), node.getOrderBy(), node.getOrderings()));

            if (current.isPartitioned()) {
                // create merge plan fragment
                PlanNode source = new ExchangeNode(current.getId(), current.getRoot().getOutputSymbols());
                TopNNode merge = new TopNNode(source, node.getCount(), node.getOrderBy(), node.getOrderings());
                current = newPlanFragment(merge, false);
            }

            return current;
        }

        @Override
        public PlanFragmentBuilder visitOutput(OutputPlan node, Void context)
        {
            PlanFragmentBuilder current = node.getSource().accept(this, context);

            if (current.isPartitioned()) {
                // create a new non-partitioned fragment
                current = newPlanFragment(new ExchangeNode(current.getId(), current.getRoot().getOutputSymbols()), false);
            }

            current.setRoot(new OutputPlan(current.getRoot(), node.getColumnNames(), node.getAssignments()));

            return current;
        }

        @Override
        public PlanFragmentBuilder visitLimit(LimitNode node, Void context)
        {
            PlanFragmentBuilder current = node.getSource().accept(this, context);

            current.setRoot(new LimitNode(current.getRoot(), node.getCount()));

            if (current.isPartitioned()) {
                // create merge plan fragment
                PlanNode source = new ExchangeNode(current.getId(), current.getRoot().getOutputSymbols());
                LimitNode merge = new LimitNode(source, node.getCount());
                current = newPlanFragment(merge, false);
            }

            return current;
        }

        @Override
        public PlanFragmentBuilder visitTableScan(TableScan node, Void context)
        {
            return newPlanFragment(node, !createSingleNodePlan);
        }

        @Override
        protected PlanFragmentBuilder visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        private PlanFragmentBuilder newPlanFragment(PlanNode root, boolean partitioned)
        {
            PlanFragmentBuilder builder = new PlanFragmentBuilder(fragmentId++)
                    .setRoot(root)
                    .setPartitioned(partitioned);

            fragments.add(builder);

            return builder;
        }

        private List<PlanFragmentBuilder> getFragments()
        {
            return fragments;
        }
    }

}
