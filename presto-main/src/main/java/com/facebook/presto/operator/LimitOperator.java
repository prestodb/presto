package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.tuple.TupleInfo;

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
        checkArgument(limit >= 0, "limit must be at least zero");

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
    public PageIterator iterator(OperatorStats operatorStats)
    {
        return new LimitIterator(source.iterator(operatorStats), limit);
    }

    private static class LimitIterator
            extends AbstractPageIterator
    {
        private final PageIterator sourceIterator;
        private long remainingLimit;

        private LimitIterator(PageIterator sourceIterator, long limit)
        {
            super(sourceIterator.getTupleInfos());
            this.sourceIterator = sourceIterator;
            this.remainingLimit = limit;
        }

        @Override
        protected Page computeNext()
        {
            if (remainingLimit <= 0 || !sourceIterator.hasNext()) {
                return endOfData();
            }

            Page page = sourceIterator.next();
            if (page.getPositionCount() <= remainingLimit) {
                remainingLimit -= page.getPositionCount();
                if (remainingLimit == 0) {
                    sourceIterator.close();
                }
                return page;
            }
            else {
                Block[] blocks = new Block[page.getChannelCount()];
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    Block block = page.getBlock(channel);
                    blocks[channel] = block.getRegion(0, (int) remainingLimit);
                }
                remainingLimit = 0;
                sourceIterator.close();
                return new Page(blocks);
            }
        }

        @Override
        protected void doClose()
        {
            sourceIterator.close();
        }
    }
}
