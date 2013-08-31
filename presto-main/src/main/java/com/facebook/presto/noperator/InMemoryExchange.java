package com.facebook.presto.noperator;

import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class InMemoryExchange
{
    private final List<TupleInfo> tupleInfos;
    private final Queue<Page> buffer;
    private boolean finishing;
    private boolean noMoreSinkFactories;
    private int sinkFactories;
    private int sinks;

    public InMemoryExchange(List<TupleInfo> tupleInfos)
    {
        this.tupleInfos = ImmutableList.copyOf(checkNotNull(tupleInfos, "tupleInfos is null"));
        this.buffer = new ConcurrentLinkedQueue<>();
    }

    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    public synchronized NewOperatorFactory createSinkFactory()
    {
        sinkFactories++;
        return new InMemoryExchangeSinkOperatorFactory();
    }

    private synchronized void addSink()
    {
        checkState(sinkFactories > 0, "All sink factories already closed");
        sinks++;
    }

    public synchronized void sinkFinished()
    {
        checkState(sinks != 0, "All sinks are already complete");
        sinks--;
        updateState();
    }

    public synchronized void noMoreSinkFactories()
    {
        this.noMoreSinkFactories = true;
        updateState();
    }

    private synchronized void sinkFactoryClosed()
    {
        checkState(sinkFactories != 0, "All sinks factories are already closed");
        sinkFactories--;
        updateState();
    }

    private void updateState()
    {
        if (noMoreSinkFactories && sinkFactories == 0 && sinks == 0) {
            finishing = true;
        }
    }

    public synchronized boolean isFinishing()
    {
        return finishing;
    }

    public synchronized void finish()
    {
        finishing = true;
    }

    public synchronized boolean isFinished()
    {
        return finishing && buffer.isEmpty();
    }

    public synchronized void addPage(Page page)
    {
        if (finishing) {
            return;
        }
        buffer.add(page);
    }

    public synchronized Page removePage()
    {
        return buffer.poll();
    }

    private class InMemoryExchangeSinkOperatorFactory
            implements NewOperatorFactory
    {
        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        @Override
        public NewOperator createOperator(OperatorStats operatorStats, TaskMemoryManager taskMemoryManager)
        {
            addSink();
            return new InMemoryExchangeSinkOperator(InMemoryExchange.this);
        }

        @Override
        public void close()
        {
            sinkFactoryClosed();
        }
    }
}
