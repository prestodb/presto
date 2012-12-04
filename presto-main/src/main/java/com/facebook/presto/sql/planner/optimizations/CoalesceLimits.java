package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;

import java.util.Map;

/**
 * Merges successive LIMIT operators into a single LIMIT that's the minimum of the entire chain
 */
public class CoalesceLimits
        extends PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Map<Symbol, Type> types)
    {
        Visitor visitor = new Visitor();
        return plan.accept(visitor, null);
    }

    private static class Visitor
        extends PlanVisitor<Void, PlanNode>
    {
        @Override
        protected PlanNode visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        public PlanNode visitLimit(LimitNode node, Void context)
        {
            PlanNode child = node.getSource().accept(this, context);
            if (child instanceof LimitNode) {
                return new LimitNode(((LimitNode) child).getSource(), Math.min(node.getCount(), ((LimitNode) child).getCount()));
            }

            return node;
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);
            return new AggregationNode(source, node.getGroupBy(), node.getAggregations(), node.getFunctions());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, Void context)
        {
            return node;
        }

        @Override
        public PlanNode visitFilter(FilterNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);
            return new FilterNode(source, node.getPredicate(), node.getOutputSymbols());
        }

        @Override
        public PlanNode visitProject(ProjectNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);
            return new ProjectNode(source, node.getOutputMap());
        }

        @Override
        public PlanNode visitOutput(OutputNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);
            return new OutputNode(source, node.getColumnNames(), node.getAssignments());
        }

        @Override
        public PlanNode visitTopN(TopNNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);
            return new TopNNode(source, node.getCount(), node.getOrderBy(), node.getOrderings());
        }

        @Override
        public PlanNode visitJoin(JoinNode node, Void context)
        {
            PlanNode left = node.getLeft().accept(this, context);
            PlanNode right = node.getRight().accept(this, context);

            return new JoinNode(left, right, node.getCriteria());
        }
    }
}
