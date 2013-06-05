package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.ingest.RecordProjectOperator.RecordProjectionIterator;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.tuple.TupleInfo;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractFilterAndProjectOperator
        implements Operator
{
    private final Operator source;
    private final List<TupleInfo> tupleInfos;

    public AbstractFilterAndProjectOperator(List<TupleInfo> tupleInfos, Operator source)
    {
        this.source = checkNotNull(source, "source is null");
        this.tupleInfos = checkNotNull(tupleInfos, "tupleInfos is null");
    }

    @Override
    public int getChannelCount()
    {
        return tupleInfos.size();
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public PageIterator iterator(OperatorStats operatorStats)
    {
        return iterator(source.iterator(operatorStats));
    }

    protected abstract PageIterator iterator(PageIterator source);

    public abstract static class AbstractFilterAndProjectIterator
            extends AbstractPageIterator
    {
        protected final PageIterator pageIterator;
        protected final RecordCursor cursor;
        protected final PageBuilder pageBuilder;

        public AbstractFilterAndProjectIterator(Iterable<TupleInfo> tupleInfos, PageIterator pageIterator)
        {
            super(tupleInfos);
            this.pageIterator = pageIterator;
            if (pageIterator instanceof RecordProjectionIterator) {
                this.cursor = ((RecordProjectionIterator) pageIterator).getCursor();
            }
            else {
                this.cursor = null;
            }
            this.pageBuilder = new PageBuilder(getTupleInfos());
        }

        @Override
        protected Page computeNext()
        {
            if (cursor == null) {
                return iteratorComputeNext();
            } else {
                return cursorComputeNext();
            }
        }

        protected Page iteratorComputeNext()
        {
            pageBuilder.reset();
            while (!pageBuilder.isFull() && pageIterator.hasNext()) {
                Page page = pageIterator.next();
                Block[] blocks = page.getBlocks();
                filterAndProjectRowOriented(blocks, pageBuilder);
            }

            if (pageBuilder.isEmpty()) {
                return endOfData();
            }

            Page page = pageBuilder.build();
            return page;
        }

        protected Page cursorComputeNext()
        {
            pageBuilder.reset();
            filterAndProjectRowOriented(cursor, pageBuilder);
            if (pageBuilder.isEmpty()) {
                return endOfData();
            }

            Page page = pageBuilder.build();
            return page;
        }

        protected abstract void filterAndProjectRowOriented(Block[] blocks, PageBuilder pageBuilder);

        protected abstract void filterAndProjectRowOriented(RecordCursor cursor, PageBuilder pageBuilder);

        @Override
        protected void doClose()
        {
            pageIterator.close();
        }
    }
}
