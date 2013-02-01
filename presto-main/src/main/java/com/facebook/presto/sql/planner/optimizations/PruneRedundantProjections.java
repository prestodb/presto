package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeRewriter;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
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
        return PlanRewriter.rewriteWith(new Rewriter(), plan);
    }

    private static class Rewriter
            extends PlanNodeRewriter<Void>
    {
        @Override
        public PlanNode rewriteProject(ProjectNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), context);

            if (node.getOutputSymbols().size() != source.getOutputSymbols().size()) {
                // Can't get rid of this projection. It constrains the output tuple from the underlying operator
                return new ProjectNode(node.getId(), source, node.getOutputMap());
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

            return new ProjectNode(node.getId(), source, node.getOutputMap());
        }
    }
}
