package com.facebook.presto.operator;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;

import java.util.List;

public class ForwardingOperator
        implements Operator
{
    private final Operator operator;

    public ForwardingOperator(Operator operator)
    {
        this.operator = Preconditions.checkNotNull(operator, "operator is null");
    }

    @Override
    public int getChannelCount()
    {
        return operator.getChannelCount();
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return operator.getTupleInfos();
    }

    @Override
    public PageIterator iterator(OperatorStats operatorStats)
    {
        return operator.iterator(operatorStats);
    }
}
