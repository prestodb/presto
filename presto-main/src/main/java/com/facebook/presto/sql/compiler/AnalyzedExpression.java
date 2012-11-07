package com.facebook.presto.sql.compiler;

import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Preconditions;

import java.util.Set;

public class AnalyzedExpression
{
    private final Type type;
    private final Expression rewritten;

    public AnalyzedExpression(Type type, Expression rewritten)
    {
        Preconditions.checkNotNull(type, "type is null");
        Preconditions.checkNotNull(rewritten, "rewritten is null");

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
