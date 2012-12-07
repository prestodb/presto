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
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedNameReference;

import java.util.Map;

/**
 * Removes pure identity projections (e.g., Project $0 := $0, $1 := $1, ...)
 */
public class PruneRedundantProjections
        extends PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Map<Symbol, Type> types)
    {
        return plan.accept(new Visitor(), null);
    }

    private static class Visitor
            extends PlanVisitor<Void, PlanNode>
    {
        @Override
        public PlanNode visitProject(ProjectNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);

            if (node.getOutputSymbols().size() != source.getOutputSymbols().size()) {
                // Can't get rid of this projection. It constrains the output tuple from the underlying operator
                return new ProjectNode(source, node.getOutputMap());
            }

            boolean canElide = true;
            for (Map.Entry<Symbol, Expression> entry : node.getOutputMap().entrySet()) {
                Expression expression = entry.getValue();
                Symbol symbol = entry.getKey();
                if (!(expression instanceof QualifiedNameReference && ((QualifiedNameReference) expression).getName().equals(symbol.toQualifiedName()))) {
                    canElide = false;
                    break;
                }
            }

            if (canElide) {
                return source;
            }

            return new ProjectNode(source, node.getOutputMap());
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
        public PlanNode visitOutput(OutputNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);
            return new OutputNode(source, node.getColumnNames(), node.getAssignments());
        }

        @Override
        public PlanNode visitLimit(LimitNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);
            return new LimitNode(source, node.getCount());
        }

        @Override
        public PlanNode visitTopN(TopNNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);
            return new TopNNode(source, node.getCount(), node.getOrderBy(), node.getOrderings());
        }

        @Override
        public PlanNode visitSort(SortNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);
            return new SortNode(source, node.getOrderBy(), node.getOrderings());
        }

        @Override
        public PlanNode visitJoin(JoinNode node, Void context)
        {
            PlanNode left = node.getLeft().accept(this, context);
            PlanNode right = node.getRight().accept(this, context);

            return new JoinNode(left, right, node.getCriteria());
        }

        @Override
        protected PlanNode visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + getClass().getName());
        }
    }
}
