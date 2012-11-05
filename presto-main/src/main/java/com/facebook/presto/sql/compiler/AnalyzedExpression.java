package com.facebook.presto.sql.compiler;

import com.facebook.presto.sql.tree.Expression;

import java.util.Set;

public class AnalyzedExpression
{
    private final Type type;
    private final Expression rewritten;

    public AnalyzedExpression(Type type, Expression rewritten)
    {
        this.type = type;
        this.rewritten = rewritten;
    }

    public Expression getRewrittenExpression()
    {
        return rewritten;
    }

    public Type getType()
    {
        return type;
    }
}
