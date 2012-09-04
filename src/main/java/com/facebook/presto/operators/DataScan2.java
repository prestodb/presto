package com.facebook.presto.operators;

import com.facebook.presto.Tuple;
import com.facebook.presto.ValueBlock;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;

import java.util.Iterator;

public class DataScan2
        extends AbstractIterator<ValueBlock>
{
    private final Iterator<ValueBlock> source;
    private final Predicate<Tuple> predicate;

    public DataScan2(Iterator<ValueBlock> source, Predicate<Tuple> predicate)
    {
        this.predicate = predicate;
        this.source = source;
    }

    @Override
    protected ValueBlock computeNext()
    {
        while (source.hasNext()) {
            Optional<ValueBlock> block = source.next().selectPairs(predicate);
            if (block.isPresent()) {
                return block.get();
            }
        }

        endOfData();
        return null;
    }
}
