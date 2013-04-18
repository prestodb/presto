package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents an expression or a direct field reference. The latter is used, for
 * instance, when expanding "*" in SELECT * FROM ....
 */
public class FieldOrExpression
{
    private final Optional<Field> field;
    private final Optional<Expression> expression;

    public FieldOrExpression(Field field)
    {
        checkNotNull(field, "field is null");

        this.field = Optional.of(field);
        this.expression = Optional.absent();
    }

    public FieldOrExpression(Expression expression)
    {
        Preconditions.checkNotNull(expression, "expression is null");

        this.field = Optional.absent();
        this.expression = Optional.of(expression);
    }

    public Optional<Field> getField()
    {
        return field;
    }

    public Optional<Expression> getExpression()
    {
        return expression;
    }

    @Override
    public String toString()
    {
        if (field.isPresent()) {
            return field.get().toString();
        }

        return expression.get().toString();
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

        FieldOrExpression that = (FieldOrExpression) o;

        if (!expression.equals(that.expression)) {
            return false;
        }
        if (!field.equals(that.field)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = field.hashCode();
        result = 31 * result + expression.hashCode();
        return result;
    }

    public static Function<FieldOrExpression, Optional<Field>> fieldGetter()
    {
        return new Function<FieldOrExpression, Optional<Field>>()
        {
            @Override
            public Optional<Field> apply(FieldOrExpression input)
            {
                return input.getField();
            }
        };
    }

    public static Function<FieldOrExpression, Optional<Expression>> expressionGetter()
    {
        return new Function<FieldOrExpression, Optional<Expression>>()
        {
            @Override
            public Optional<Expression> apply(FieldOrExpression input)
            {
                return input.getExpression();
            }
        };
    }
}
