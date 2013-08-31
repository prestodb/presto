package com.facebook.presto.noperator;

import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class InMemoryExchangeSinkOperator
        implements NewOperator
{
    private final InMemoryExchange inMemoryExchange;
    private boolean finished;

    InMemoryExchangeSinkOperator(InMemoryExchange inMemoryExchange)
    {
        this.inMemoryExchange = inMemoryExchange;
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return inMemoryExchange.getTupleInfos();
    }

    @Override
    public void finish()
    {
        if (!finished) {
            finished = true;
            inMemoryExchange.sinkFinished();
        }
    }

    @Override
    public boolean isFinished()
    {
        if (!finished) {
            finished = inMemoryExchange.isFinishing();
        }
        return finished;
    }

    @Override
    public boolean needsInput()
    {
        return !isFinished();
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");
        checkState(!finished, "Already finished");
        inMemoryExchange.addPage(page);
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
