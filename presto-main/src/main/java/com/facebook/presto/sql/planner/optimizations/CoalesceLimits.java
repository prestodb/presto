package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.compiler.Symbol;
import com.facebook.presto.sql.compiler.Type;
import com.facebook.presto.sql.planner.AggregationNode;
import com.facebook.presto.sql.planner.FilterNode;
import com.facebook.presto.sql.planner.JoinNode;
import com.facebook.presto.sql.planner.LimitNode;
import com.facebook.presto.sql.planner.OutputPlan;
import com.facebook.presto.sql.planner.PlanNode;
import com.facebook.presto.sql.planner.PlanVisitor;
import com.facebook.presto.sql.planner.ProjectNode;
import com.facebook.presto.sql.planner.TableScan;
import com.facebook.presto.sql.planner.TopNNode;

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
        public PlanNode visitTableScan(TableScan node, Void context)
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
        public PlanNode visitOutput(OutputPlan node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);
            return new OutputPlan(source, node.getColumnNames(), node.getAssignments());
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
