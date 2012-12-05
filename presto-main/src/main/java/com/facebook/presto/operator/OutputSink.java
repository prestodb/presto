/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class OutputSink
        implements Operator
{
    private final Operator source;
    private final OutputSinkHandler handler;

    public OutputSink(Operator source, OutputSinkHandler handler)
    {
        this.source = checkNotNull(source, "source is null");
        this.handler = checkNotNull(handler, "handler is null");
    }

    @Override
    public int getChannelCount()
    {
        return source.getChannelCount();
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return source.getTupleInfos();
    }

    @Override
    public PageIterator iterator()
    {
        return new OutputSinkPageIterator(source.iterator(), handler);
    }

    public interface OutputSinkHandler
    {
        public void process(Tuple... tuples);
    }

    private static class OutputSinkPageIterator
            extends AbstractPageIterator
    {
        private final PageIterator source;
        private final OutputSinkHandler handler;
        private final BlockCursor[] cursors;
        private final Tuple[] tuples;

        public OutputSinkPageIterator(PageIterator source, OutputSinkHandler handler)
        {
            super(source.getTupleInfos());
            this.source = source;
            this.handler = handler;
            cursors = new BlockCursor[source.getChannelCount()];
            tuples = new Tuple[source.getChannelCount()];
        }

        @Override
        protected Page computeNext()
        {
            if (!source.hasNext()) {
                return endOfData();
            }
            Page page = source.next();
            for (int i = 0; i < cursors.length; i++) {
                cursors[i] = page.getBlock(i).cursor();
            }

            for (int position = 0; position < page.getPositionCount(); position++) {
                for (int i = 0; i < cursors.length; i++) {
                    checkState(cursors[i].advanceNextPosition());
                    tuples[i] = cursors[i].getTuple();
                }

                handler.process(tuples);
            }

            for (BlockCursor cursor : cursors) {
                checkState(!cursor.advanceNextPosition());
            }
            return page;
        }

        @Override
        protected void doClose()
        {
            source.close();
        }
    }
}
