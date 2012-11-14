package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.AbstractIterator;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class LimitOperator
        implements Operator
{
    private final Operator source;
    private final long limit;

    public LimitOperator(Operator source, long limit)
    {
        checkNotNull(source, "source is null");
        checkArgument(limit > 0, "limit must be at least 1");

        this.source = source;
        this.limit = limit;
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
    public Iterator<Page> iterator()
    {
        return new LimitIterator(source.iterator(), limit);
    }

    private static class LimitIterator
            extends AbstractIterator<Page>
    {
        private final Iterator<Page> sourceIterator;
        private long remainingLimit;

        private LimitIterator(Iterator<Page> sourceIterator, long limit)
        {
            this.sourceIterator = sourceIterator;
            this.remainingLimit = limit;
        }

        @Override
        protected Page computeNext()
        {
            if (!sourceIterator.hasNext() || remainingLimit <= 0) {
                return endOfData();
            }

            Page page = sourceIterator.next();
            if (page.getPositionCount() <= remainingLimit) {
                remainingLimit -= page.getPositionCount();
                return page;
            }
            else {
                Block[] blocks = new Block[page.getChannelCount()];
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    Block block = page.getBlock(channel);
                    blocks[channel] = block.getRegion(0, (int) remainingLimit);
                }
                remainingLimit = 0;
                return new Page(blocks);
            }
        }
    }
}
