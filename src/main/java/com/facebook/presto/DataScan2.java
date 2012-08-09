package com.facebook.presto;

import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;

import java.util.Iterator;

public class DataScan2
        extends AbstractIterator<ValueBlock>
{
    private final Iterator<ValueBlock> source;
    private final Predicate<Object> predicate;

    public DataScan2(Iterator<ValueBlock> source, Predicate<Object> predicate)
    {
        this.predicate = predicate;
        this.source = source;
    }

    @Override
    protected ValueBlock computeNext()
    {
        while (source.hasNext()) {
            ValueBlock block = source.next().selectPairs(predicate);
            if (!block.isEmpty()) {
                return block;
            }
        }

        endOfData();
        return null;
    }
}
