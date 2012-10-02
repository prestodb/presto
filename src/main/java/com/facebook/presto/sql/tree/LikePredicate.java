package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class LikePredicate
        extends Expression
{
    private final Expression value;
    private final Expression pattern;
    private final Expression escape;

    public LikePredicate(Expression value, Expression pattern, Expression escape) {
        this.value = value;
        this.pattern = pattern;
        this.escape = escape;
    }

    public Expression getValue()
    {
        return value;
    }

    public Expression getPattern()
    {
        return pattern;
    }

    public Expression getEscape()
    {
        return escape;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitLikePredicate(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("value", value)
                .add("pattern", pattern)
                .add("escape", escape)
                .omitNullValues()
                .toString();
    }
}
