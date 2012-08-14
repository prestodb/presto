package com.facebook.presto;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;

import java.util.Iterator;

public class PairsIterator
        extends AbstractIterator<Pair>
{
    private final Iterator<ValueBlock> blockIterator;
    private PeekingIterator<Pair> currentBlock;

    public PairsIterator(Iterator<ValueBlock> blockIterator)
    {
        this.blockIterator = blockIterator;
    }

    @Override
    protected Pair computeNext()
    {
        if (!advance()) {
            endOfData();
            return null;
        }
        return currentBlock.next();
    }

    private boolean advance()
    {
        // does current block iterator have more data?
        if (currentBlock != null && currentBlock.hasNext()) {
            return true;
        }

        // are there more blocks?
        if (!blockIterator.hasNext()) {
            return false;
        }

        // advance to next block and open an iterator
        currentBlock = blockIterator.next().pairIterator();
        return true;
    }

}
