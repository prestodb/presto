package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AnalyzedExpression that = (AnalyzedExpression) o;

        if (!rewritten.equals(that.rewritten)) {
            return false;
        }
        if (type != that.type) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = type.hashCode();
        result = 31 * result + rewritten.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("type", type)
                .add("rewritten", rewritten)
                .toString();
    }

    public static Function<AnalyzedExpression, Expression> rewrittenExpressionGetter()
    {
        return new Function<AnalyzedExpression, Expression>()
        {
            @Override
            public Expression apply(AnalyzedExpression input)
            {
                return input.getRewrittenExpression();
            }
        };
    }

    public static Function<AnalyzedExpression, Type> typeGetter()
    {
        return new Function<AnalyzedExpression, Type>()
        {
            @Override
            public Type apply(AnalyzedExpression input)
            {
                return input.getType();
            }
        };
    }
}
