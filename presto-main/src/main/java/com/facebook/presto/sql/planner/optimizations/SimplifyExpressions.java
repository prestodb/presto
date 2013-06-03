package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolResolver;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeRewriter;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class SimplifyExpressions
        extends PlanOptimizer
{
    private final Metadata metadata;

    public SimplifyExpressions(Metadata metadata)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, PlanNodeIdAllocator idAllocator)
    {
        checkNotNull(plan, "plan is null");
        checkNotNull(session, "session is null");
        checkNotNull(types, "types is null");
        checkNotNull(idAllocator, "idAllocator is null");

        ExpressionInterpreter interpreter = ExpressionInterpreter.expressionOptimizer(new SymbolResolver()
        {
            @Override
            public Object getValue(Symbol symbol)
            {
                return new QualifiedNameReference(symbol.toQualifiedName());
            }
        }, metadata, session);

        return PlanRewriter.rewriteWith(new Rewriter(interpreter), plan);
    }

    private static class Rewriter
            extends PlanNodeRewriter<Void>
    {
        private final ExpressionInterpreter interpreter;

        private Rewriter(ExpressionInterpreter interpreter)
        {
            this.interpreter = interpreter;
        }

        @Override
        public PlanNode rewriteProject(ProjectNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), context);
            Map<Symbol, Expression> assignments = ImmutableMap.copyOf(Maps.transformValues(node.getOutputMap(), simplifyExpressionFunction()));
            return new ProjectNode(node.getId(), source, assignments);
        }

        @Override
        public PlanNode rewriteFilter(FilterNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), context);
            return new FilterNode(node.getId(), source, simplifyExpression(node.getPredicate()));
        }

        private Function<Expression, Expression> simplifyExpressionFunction()
        {
            return new Function<Expression, Expression>()
            {
                @Override
                public Expression apply(Expression input)
                {
                    return simplifyExpression(input);
                }
            };
        }

        private Expression simplifyExpression(Expression input)
        {
            return ExpressionInterpreter.toExpression(interpreter.process(input, null));
        }
    }

}
