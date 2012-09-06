package com.facebook.presto.operators;

import com.facebook.presto.BlockStream;
import com.facebook.presto.Cursor;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.ValueBlock;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;

import java.util.Iterator;

public class DataScan2
        implements BlockStream<ValueBlock>
{
    private final BlockStream<?> source;
    private final Predicate<Tuple> predicate;

    public DataScan2(BlockStream<?> source, Predicate<Tuple> predicate)
    {
        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkNotNull(predicate, "predicate is null");
        this.source = source;
        this.predicate = predicate;
    }

    @Override
    public Iterator<ValueBlock> iterator()
    {
        return new AbstractIterator<ValueBlock>()
        {
            private final Iterator<? extends ValueBlock> blockIterator = source.iterator();

            @Override
            protected ValueBlock computeNext()
            {
                while (blockIterator.hasNext()) {
                    Optional<ValueBlock> block = blockIterator.next().selectPairs(predicate);
                    if (block.isPresent()) {
                        return block.get();
                    }
                }

                endOfData();
                return null;
            }
        };
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return source.getTupleInfo();
    }

    @Override
    public Cursor cursor()
    {
        return new ValueCursor(getTupleInfo(), iterator());
    }

}
