package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
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
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final SingleColumn other = (SingleColumn) obj;
        return Objects.equal(this.alias, other.alias) && Objects.equal(this.expression, other.expression);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(alias, expression);
    }

    public String toString()
    {
        if (alias.isPresent()) {
            return expression.toString() + " " + alias.get();
        }

        return expression.toString();
    }


    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSingleColumn(this, context);
    }
}
