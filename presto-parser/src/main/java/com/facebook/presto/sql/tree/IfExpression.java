package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.base.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * IF(v1,v2[,v3]): CASE WHEN v1 THEN v2 [ELSE v3] END
 */
public class IfExpression
        extends Expression
{
    private final Expression condition;
    private final Expression trueValue;
    private final Optional<Expression> falseValue;

    public IfExpression(Expression condition, Expression trueValue, Expression falseValue)
    {
        this.condition = checkNotNull(condition, "condition is null");
        this.trueValue = checkNotNull(trueValue, "trueValue is null");
        this.falseValue = Optional.fromNullable(falseValue);
    }

    public Expression getCondition()
    {
        return condition;
    }

    public Expression getTrueValue()
    {
        return trueValue;
    }

    public Optional<Expression> getFalseValue()
    {
        return falseValue;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitIfExpression(this, context);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        IfExpression o = (IfExpression) obj;
        return Objects.equal(condition, o.condition) &&
                Objects.equal(trueValue, o.trueValue) &&
                Objects.equal(falseValue, o.falseValue);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(condition, trueValue, falseValue);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("condition", condition)
                .add("trueValue", trueValue)
                .add("falseValue", falseValue.orNull())
                .omitNullValues()
                .toString();
    }
}
