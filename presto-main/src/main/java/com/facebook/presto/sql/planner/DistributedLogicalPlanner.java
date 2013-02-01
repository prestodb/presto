package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.FunctionHandle;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.analyzer.SymbolAllocator;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SinkNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.SINGLE;

/**
 * Splits a logical plan into fragments that can be shipped and executed on distributed nodes
 */
public class DistributedLogicalPlanner
{
    private final Metadata metadata;
    private final PlanNodeIdAllocator idAllocator;

    public DistributedLogicalPlanner(Metadata metadata, PlanNodeIdAllocator idAllocator)
    {
        this.metadata = metadata;
        this.idAllocator = idAllocator;
    }

    public SubPlan createSubplans(PlanNode plan, SymbolAllocator allocator, boolean createSingleNodePlan)
    {
        Visitor visitor = new Visitor(allocator, createSingleNodePlan);
        SubPlanBuilder builder = plan.accept(visitor, null);

        SubPlan subplan = builder.build();
        subplan.sanityCheck();

        return subplan;
    }

    private class Visitor
            extends PlanVisitor<Void, SubPlanBuilder>
    {
        private int nextFragmentId = 0;

        private final SymbolAllocator allocator;
        private final boolean createSingleNodePlan;

        public Visitor(SymbolAllocator allocator, boolean createSingleNodePlan)
        {
            this.allocator = allocator;
            this.createSingleNodePlan = createSingleNodePlan;
        }

        @Override
        public SubPlanBuilder visitAggregation(AggregationNode node, Void context)
        {
            SubPlanBuilder current = node.getSource().accept(this, context);

            if (!current.isPartitioned()) {
                // add the aggregation node as the root of the current fragment
                current.setRoot(new AggregationNode(node.getId(), current.getRoot(), node.getGroupBy(), node.getAggregations(), node.getFunctions(), SINGLE));
                return current;
            }

            // else, we need to "close" the current fragment and create an unpartitioned fragment for the final aggregation
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

            AggregationNode aggregation = new AggregationNode(idAllocator.getNextId(), current.getRoot(), node.getGroupBy(), intermediateCalls, intermediateFunctions, PARTIAL);
            current.setRoot(new SinkNode(idAllocator.getNextId(), aggregation));

            // create merge + aggregation plan
            ExchangeNode source = new ExchangeNode(idAllocator.getNextId(), current.getId(), current.getRoot().getOutputSymbols());
            AggregationNode merged = new AggregationNode(idAllocator.getNextId(), source, node.getGroupBy(), finalCalls, node.getFunctions(), FINAL);

            return newSubPlan(merged)
                    .setPartitioned(false)
                    .addChild(current.build());
        }

        @Override
        public SubPlanBuilder visitFilter(FilterNode node, Void context)
        {
            SubPlanBuilder current = node.getSource().accept(this, context);
            current.setRoot(new FilterNode(node.getId(), current.getRoot(), node.getPredicate()));
            return current;
        }

        @Override
        public SubPlanBuilder visitProject(ProjectNode node, Void context)
        {
            SubPlanBuilder current = node.getSource().accept(this, context);
            current.setRoot(new ProjectNode(node.getId(), current.getRoot(), node.getOutputMap()));
            return current;
        }

        @Override
        public SubPlanBuilder visitTopN(TopNNode node, Void context)
        {
            SubPlanBuilder current = node.getSource().accept(this, context);

            current.setRoot(new TopNNode(node.getId(), current.getRoot(), node.getCount(), node.getOrderBy(), node.getOrderings()));

            if (current.isPartitioned()) {
                current.setRoot(new SinkNode(idAllocator.getNextId(), current.getRoot()));

                // create merge plan fragment
                PlanNode source = new ExchangeNode(idAllocator.getNextId(), current.getId(), current.getRoot().getOutputSymbols());
                TopNNode merge = new TopNNode(idAllocator.getNextId(), source, node.getCount(), node.getOrderBy(), node.getOrderings());
                current = newSubPlan(merge)
                        .setPartitioned(false)
                        .addChild(current.build());
            }

            return current;
        }

        @Override
        public SubPlanBuilder visitSort(SortNode node, Void context)
        {
            SubPlanBuilder current = node.getSource().accept(this, context);

            if (current.isPartitioned()) {
                current.setRoot(new SinkNode(idAllocator.getNextId(), current.getRoot()));

                // create a new non-partitioned fragment
                current = newSubPlan(new ExchangeNode(idAllocator.getNextId(), current.getId(), current.getRoot().getOutputSymbols()))
                        .setPartitioned(false)
                        .addChild(current.build());
            }

            current.setRoot(new SortNode(node.getId(), current.getRoot(), node.getOrderBy(), node.getOrderings()));

            return current;
        }


        @Override
        public SubPlanBuilder visitOutput(OutputNode node, Void context)
        {
            SubPlanBuilder current = node.getSource().accept(this, context);

            if (current.isPartitioned()) {
                current.setRoot(new SinkNode(idAllocator.getNextId(), current.getRoot()));

                // create a new non-partitioned fragment
                current = newSubPlan(new ExchangeNode(idAllocator.getNextId(), current.getId(), current.getRoot().getOutputSymbols()))
                        .setPartitioned(false)
                        .addChild(current.build());
            }

            current.setRoot(new OutputNode(node.getId(), current.getRoot(), node.getColumnNames(), node.getAssignments()));

            return current;
        }

        @Override
        public SubPlanBuilder visitLimit(LimitNode node, Void context)
        {
            SubPlanBuilder current = node.getSource().accept(this, context);

            current.setRoot(new LimitNode(node.getId(), current.getRoot(), node.getCount()));

            if (current.isPartitioned()) {
                current.setRoot(new SinkNode(idAllocator.getNextId(), current.getRoot()));

                // create merge plan fragment
                PlanNode source = new ExchangeNode(idAllocator.getNextId(), current.getId(), current.getRoot().getOutputSymbols());
                LimitNode merge = new LimitNode(idAllocator.getNextId(), source, node.getCount());
                current = newSubPlan(merge)
                        .setPartitioned(false)
                        .addChild(current.build());
            }

            return current;
        }

        @Override
        public SubPlanBuilder visitTableScan(TableScanNode node, Void context)
        {
            return newSubPlan(node)
                    .setPartitioned(!createSingleNodePlan);
        }

        @Override
        public SubPlanBuilder visitJoin(JoinNode node, Void context)
        {
            SubPlanBuilder left = node.getLeft().accept(this, context);
            SubPlanBuilder right = node.getRight().accept(this, context);

            if (left.isPartitioned() || right.isPartitioned()) {
                right.setRoot(new SinkNode(idAllocator.getNextId(), right.getRoot()));

                ExchangeNode exchange = new ExchangeNode(idAllocator.getNextId(), right.getId(), right.getRoot().getOutputSymbols());
                JoinNode join = new JoinNode(node.getId(), left.getRoot(), exchange, node.getCriteria());
                left.setRoot(join)
                    .addChild(right.build());

                return left;
            }
            else {
                JoinNode join = new JoinNode(node.getId(), left.getRoot(), right.getRoot(), node.getCriteria());
                return newSubPlan(join)
                        .setPartitioned(false)
                        .setChildren(Iterables.concat(left.getChildren(), right.getChildren()));
            }
        }

        @Override
        protected SubPlanBuilder visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        private SubPlanBuilder newSubPlan(PlanNode root)
        {
            return new SubPlanBuilder(new PlanFragmentId(String.valueOf(nextFragmentId++)), allocator, root);
        }
    }

}
