/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.asList;

public final class PageIterators
{
    private PageIterators()
    {
    }

    public static PageIterator emptyIterator(Iterable<TupleInfo> tupleInfos)
    {
        return new EmptyPageIterator(tupleInfos);
    }

    private static class EmptyPageIterator
            implements PageIterator
    {

        private final ImmutableList<TupleInfo> tupleInfos;

        public EmptyPageIterator(Iterable<TupleInfo> tupleInfos)
        {
            this.tupleInfos = ImmutableList.copyOf(tupleInfos);
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
        public void close()
        {
        }

        @Override
        public boolean hasNext()
        {
            return false;
        }

        @Override
        public Page next()
        {
            throw new NoSuchElementException();
        }

        @Override
        public Page peek()
        {
            throw new NoSuchElementException();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    public static PageIterator singletonIterator(Page page)
    {
        return new SingletonPageIterator(page);
    }

    private static class SingletonPageIterator
            extends AbstractPageIterator
    {
        private boolean used;
        private final Page page;

        public SingletonPageIterator(Page page)
        {
            super(toTupleInfo(page));
            this.page = page;
        }

        @Override
        protected void doClose()
        {
            used = true;
        }

        @Override
        protected Page computeNext()
        {
            if (used) {
                return endOfData();
            }
            used = true;
            return page;
        }
    }

    public static PageIterator createPageIterator(Page firstPage, Page... additionalPages)
    {
        return createPageIterator(asList(firstPage, additionalPages));
    }

    public static PageIterator createPageIterator(Iterable<? extends Page> pages)
    {
        return createPageIterator(pages, new OperatorStats());
    }

    public static PageIterator createPageIterator(Iterable<? extends Page> pages, OperatorStats operatorStats)
    {
        Preconditions.checkNotNull(pages, "pages is null");
        Preconditions.checkArgument(!Iterables.isEmpty(pages), "pages is empty");
        return new PageIteratorAdapter(operatorStats, pages);
    }

    private static class PageIteratorAdapter extends AbstractPageIterator
    {
        private final Iterator<? extends Page> iterator;
        private final OperatorStats operatorStats;

        public PageIteratorAdapter(OperatorStats operatorStats, Iterable<? extends Page> pages)
        {
            super(toTupleInfo(pages.iterator().next()));
            this.operatorStats = operatorStats;
            iterator = pages.iterator();
        }

        @Override
        protected Page computeNext()
        {
            if (!iterator.hasNext()) {
                return endOfData();
            }

            Page page = iterator.next();
            operatorStats.addCompletedPositions(page.getPositionCount());
            operatorStats.addCompletedDataSize(page.getDataSize().toBytes());

            return page;
        }

        @Override
        protected void doClose()
        {
            // advance to end of stream
            while (iterator.hasNext()) {
                iterator.next();
            }
        }
    }

    private static Iterable<TupleInfo> toTupleInfo(Page page)
    {
        return transform(ImmutableList.copyOf(page.getBlocks()), new Function<Block, TupleInfo>()
        {
            @Override
            public TupleInfo apply(Block block)
            {
                return block.getTupleInfo();
            }
        });
    }

}
