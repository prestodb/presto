package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class InPredicate
        extends Expression
{
    private final Expression value;
    private final Expression valueList;

    public InPredicate(Expression value, Expression valueList)
    {
        this.value = value;
        this.valueList = valueList;
    }

    public Expression getValue()
    {
        return value;
    }

    public Expression getValueList()
    {
        return valueList;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("value", value)
                .add("valueList", valueList)
                .toString();
    }
}
