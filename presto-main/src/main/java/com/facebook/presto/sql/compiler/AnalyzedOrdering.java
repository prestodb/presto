package com.facebook.presto.sql.compiler;

import com.facebook.presto.sql.tree.SortItem;
import com.google.common.base.Function;

import javax.annotation.Nullable;

public class AnalyzedOrdering
{
    private final AnalyzedExpression expression;
    private final SortItem.Ordering ordering;

    public AnalyzedOrdering(AnalyzedExpression expression, SortItem.Ordering ordering)
    {
        this.expression = expression;
        this.ordering = ordering;
    }

    public AnalyzedExpression getExpression()
    {
        return expression;
    }

    public SortItem.Ordering getOrdering()
    {
        return ordering;
    }

    public static Function<AnalyzedOrdering, AnalyzedExpression> expressionGetter()
    {
        return new Function<AnalyzedOrdering, AnalyzedExpression>()
        {
            @Override
            public AnalyzedExpression apply(AnalyzedOrdering input)
            {
                return input.getExpression();
            }
        };
    }
}
