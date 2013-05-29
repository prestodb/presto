package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeRewriter;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRewriter;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Merges chains of consecutive projections
 */
public class MergeProjections
        extends PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types)
    {
        checkNotNull(plan, "plan is null");
        checkNotNull(session, "session is null");
        checkNotNull(types, "types is null");

        return PlanRewriter.rewriteWith(new Rewriter(), plan);
    }

    private static class Rewriter
            extends PlanNodeRewriter<Void>
    {
        @Override
        public PlanNode rewriteProject(ProjectNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), context);

            if (source instanceof ProjectNode) {
                ImmutableMap.Builder<Symbol, Expression> projections = ImmutableMap.builder();
                for (Map.Entry<Symbol, Expression> projection : node.getOutputMap().entrySet()) {
                    Expression inlined = TreeRewriter.rewriteWith(new Inliner(((ProjectNode) source).getOutputMap()), projection.getValue());
                    projections.put(projection.getKey(), inlined);
                }

                return new ProjectNode(node.getId(), ((ProjectNode) source).getSource(), projections.build());
            }

            return new ProjectNode(node.getId(), source, node.getOutputMap());
        }
    }

    private static class Inliner
            extends NodeRewriter<Void>
    {
        private final Map<Symbol, Expression> mappings;

        public Inliner(Map<Symbol, Expression> mappings)
        {
            this.mappings = mappings;
        }

        @Override
        public Node rewriteQualifiedNameReference(QualifiedNameReference node, Void context, TreeRewriter<Void> treeRewriter)
        {
            return mappings.get(Symbol.fromQualifiedName(node.getName()));
        }
    }
}
