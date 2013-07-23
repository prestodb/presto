package com.facebook.presto.operator;

import com.google.common.base.Preconditions;

import java.util.concurrent.atomic.AtomicBoolean;

public class StoppableOperator
        extends ForwardingOperator
{
    private final AtomicBoolean closed = new AtomicBoolean();

    public StoppableOperator(Operator operator)
    {
        super(operator);
    }

    public void stopIterators()
    {
        closed.set(true);
    }

    @Override
    public PageIterator iterator(OperatorStats operatorStats)
    {
        Preconditions.checkNotNull(operatorStats, "operatorStats is null");
        return new StoppablePageIterator(super.iterator(operatorStats));
    }

    private class StoppablePageIterator
            extends AbstractPageIterator
    {
        private final PageIterator iterator;

        private StoppablePageIterator(PageIterator iterator)
        {
            super(iterator.getTupleInfos());
            this.iterator = iterator;
        }

        @Override
        protected Page computeNext()
        {
            if (closed.get() || !iterator.hasNext()) {
                return endOfData();
            }
            return iterator.next();
        }

        @Override
        protected void doClose()
        {
            iterator.close();
        }
    }
}
