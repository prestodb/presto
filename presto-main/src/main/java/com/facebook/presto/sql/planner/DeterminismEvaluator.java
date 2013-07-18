package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Determines whether a given Expression is deterministic
 */
public class DeterminismEvaluator
{
    public static boolean isDeterministic(Expression expression)
    {
        Preconditions.checkNotNull(expression, "expression is null");

        AtomicBoolean deterministic = new AtomicBoolean(true);
        new Visitor().process(expression, deterministic);
        return deterministic.get();
    }

    public static Predicate<Expression> deterministic()
    {
        return new Predicate<Expression>()
        {
            @Override
            public boolean apply(Expression expression)
            {
                return isDeterministic(expression);
            }
        };
    }

    private static class Visitor
            extends DefaultTraversalVisitor<Void, AtomicBoolean>
    {
        @Override
        protected Void visitFunctionCall(FunctionCall node, AtomicBoolean deterministic)
        {
            // TODO: total hack to figure out if a function is deterministic. martint should fix this when he refactors the planning code
            if (node.getName().equals(new QualifiedName("rand")) || node.getName().equals(new QualifiedName("random"))) {
                deterministic.set(false);
            }
            return super.visitFunctionCall(node, deterministic);
        }
    }
}
