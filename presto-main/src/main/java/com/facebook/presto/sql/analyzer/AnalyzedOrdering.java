package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.SortItem;
import com.google.common.base.Function;
import com.google.common.base.Objects;

public class AnalyzedOrdering
{
    private final AnalyzedExpression expression;
    private final SortItem.Ordering ordering;
    private final SortItem node;

    public AnalyzedOrdering(AnalyzedExpression expression, SortItem.Ordering ordering, SortItem node)
    {
        this.expression = expression;
        this.ordering = ordering;
        this.node = node;
    }

    public AnalyzedExpression getExpression()
    {
        return expression;
    }

    public SortItem.Ordering getOrdering()
    {
        return ordering;
    }

    public SortItem getNode()
    {
        return node;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("expression", expression)
                .add("ordering", ordering)
                .add("node", node)
                .toString();
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

    public static Function<AnalyzedOrdering, SortItem> nodeGetter()
    {
        return new Function<AnalyzedOrdering, SortItem>()
        {
            @Override
            public SortItem apply(AnalyzedOrdering input)
            {
                return input.getNode();
            }
        };
    }
}
