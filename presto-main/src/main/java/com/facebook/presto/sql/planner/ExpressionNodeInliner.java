package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;

import java.util.Map;

public class ExpressionNodeInliner
        extends ExpressionRewriter<Void>
{
    private final Map<? extends Expression, ? extends Expression> mappings;

    public ExpressionNodeInliner(Map<? extends Expression, ? extends Expression> mappings)
    {
        this.mappings = mappings;
    }

    @Override
    public Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        return mappings.get(node);
    }
}
