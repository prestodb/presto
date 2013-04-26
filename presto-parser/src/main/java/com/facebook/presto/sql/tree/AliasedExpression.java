package com.facebook.presto.sql.tree;

import com.google.common.base.Function;

public class AliasedExpression
        extends Expression
{
    private final Expression expression;
    private final String alias;

    public AliasedExpression(Expression expression, String alias)
    {
        this.expression = expression;
        this.alias = alias;
    }

    public Expression getExpression()
    {
        return expression;
    }

    public String getAlias()
    {
        return alias;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitAliasedExpression(this, context);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AliasedExpression that = (AliasedExpression) o;

        if (!alias.equals(that.alias)) {
            return false;
        }
        if (!expression.equals(that.expression)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = expression.hashCode();
        result = 31 * result + alias.hashCode();
        return result;
    }

    public static Function<AliasedExpression, String> aliasGetter()
    {
        return new Function<AliasedExpression, String>()
        {
            @Override
            public String apply(AliasedExpression input)
            {
                return input.getAlias();
            }
        };
    }

}
