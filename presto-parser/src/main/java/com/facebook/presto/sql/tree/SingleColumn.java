package com.facebook.presto.sql.tree;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

public class SingleColumn
        extends SelectItem
{
    private final Optional<String> alias;
    private final Expression expression;

    public SingleColumn(Expression expression, Optional<String> alias)
    {
        Preconditions.checkNotNull(expression, "expression is null");
        Preconditions.checkNotNull(alias, "alias is null");

        this.expression = expression;
        this.alias = alias;
    }

    public SingleColumn(Expression expression, String alias)
    {
        this(expression, Optional.of(alias));
    }

    public SingleColumn(Expression expression)
    {
        this(expression, Optional.<String>absent());
    }

    public Optional<String> getAlias()
    {
        return alias;
    }

    public Expression getExpression()
    {
        return expression;
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

        SingleColumn that = (SingleColumn) o;

        if (!alias.equals(that.alias)) {
            return false;
        }
        if (!expression.equals(that.expression)) {
            return false;
        }

        return true;
    }

    @Override
    public String toString()
    {
        if (alias.isPresent()) {
            return expression.toString() + " " + alias.get();
        }

        return expression.toString();
    }

    @Override
    public int hashCode()
    {
        int result = alias.hashCode();
        result = 31 * result + expression.hashCode();
        return result;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSingleColumn(this, context);
    }
}
