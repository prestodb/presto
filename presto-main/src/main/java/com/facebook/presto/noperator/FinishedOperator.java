package com.facebook.presto.noperator;

import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class FinishedOperator
        implements NewOperator
{
    private final OperatorContext operatorContext;
    private final List<TupleInfo> tupleInfos;

    public FinishedOperator(OperatorContext operatorContext, List<TupleInfo> tupleInfos)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.tupleInfos = checkNotNull(tupleInfos, "tupleInfos is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public void finish()
    {
    }

    @Override
    public boolean isFinished()
    {
        return true;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
