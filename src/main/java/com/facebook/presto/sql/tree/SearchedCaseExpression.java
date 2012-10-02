package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class SearchedCaseExpression
        extends Expression
{
    private final List<WhenClause> whenClauses;
    private final Expression defaultValue;

    public SearchedCaseExpression(List<WhenClause> whenClauses, Expression defaultValue)
    {
        this.whenClauses = ImmutableList.copyOf(whenClauses);
        this.defaultValue = defaultValue;
    }

    public List<WhenClause> getWhenClauses()
    {
        return whenClauses;
    }

    public Expression getDefaultValue()
    {
        return defaultValue;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSearchedCaseExpression(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("whenClauses", whenClauses)
                .add("defaultValue", defaultValue)
                .toString();
    }
}
