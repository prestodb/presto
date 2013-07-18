package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeRewriter;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedNameReference;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Removes pure identity projections (e.g., Project $0 := $0, $1 := $1, ...)
 */
public class PruneRedundantProjections
        extends PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        checkNotNull(plan, "plan is null");
        checkNotNull(session, "session is null");
        checkNotNull(types, "types is null");
        checkNotNull(symbolAllocator, "symbolAllocator is null");
        checkNotNull(idAllocator, "idAllocator is null");

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
