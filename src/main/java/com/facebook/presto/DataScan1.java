package com.facebook.presto;

import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;

import java.util.Iterator;

public class DataScan1
        extends AbstractIterator<PositionBlock>
{
    private final Iterator<ValueBlock> source;
    private final Predicate<Tuple> predicate;

    public DataScan1(Iterator<ValueBlock> source, Predicate<Tuple> predicate)
    {
        this.predicate = predicate;
        this.source = source;
    }

    @Override
    protected PositionBlock computeNext()
    {
        while (source.hasNext()) {
            PositionBlock block = source.next().selectPositions(predicate);
            if (!block.isEmpty()) {
                return block;
            }
        }

        endOfData();
        return null;
    }
}
