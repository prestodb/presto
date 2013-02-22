package com.facebook.presto.sql.analyzer;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class AnalyzedWindow
{
    private final List<AnalyzedExpression> partitionBy;
    private final List<AnalyzedOrdering> orderBy;

    public AnalyzedWindow(List<AnalyzedExpression> partitionBy, List<AnalyzedOrdering> orderBy)
    {
        this.partitionBy = checkNotNull(partitionBy, "partitionBy is null");
        this.orderBy = checkNotNull(orderBy, "orderBy is null");
    }

    public List<AnalyzedExpression> getPartitionBy()
    {
        return partitionBy;
    }

    public List<AnalyzedOrdering> getOrderBy()
    {
        return orderBy;
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
        AnalyzedWindow o = (AnalyzedWindow) obj;
        return Objects.equal(partitionBy, o.partitionBy) &&
                Objects.equal(orderBy, o.orderBy);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(partitionBy, orderBy);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("partitionBy", partitionBy)
                .add("orderBy", orderBy)
                .toString();
    }

    public List<AnalyzedExpression> getExpressions()
    {
        ImmutableList.Builder<AnalyzedExpression> list = ImmutableList.builder();
        list.addAll(partitionBy);
        list.addAll(Iterables.transform(orderBy, AnalyzedOrdering.expressionGetter()));
        return list.build();
    }

    public static Function<AnalyzedWindow, List<AnalyzedExpression>> expressionGetter()
    {
        return new Function<AnalyzedWindow, List<AnalyzedExpression>>()
        {
            @Override
            public List<AnalyzedExpression> apply(AnalyzedWindow input)
            {
                return input.getExpressions();
            }
        };
    }
}
