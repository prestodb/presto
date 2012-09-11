package com.facebook.presto.operators;

import com.facebook.presto.BlockStream;
import com.facebook.presto.Cursor;
import com.facebook.presto.PositionsBlock;
import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.ValueBlock;
import com.facebook.presto.block.cursor.BlockCursor;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;

public class DataScan1
        implements BlockStream<ValueBlock>
{
    private static final TupleInfo INFO = new TupleInfo();

    private final BlockStream<? extends ValueBlock> source;
    private final Predicate<BlockCursor> predicate;

    public DataScan1(BlockStream<? extends ValueBlock> source, Predicate<BlockCursor> predicate)
    {
        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkNotNull(predicate, "predicate is null");
        this.source = source;
        this.predicate = predicate;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return INFO;
    }

    @Override
    public Iterator<ValueBlock> iterator()
    {
        return new AbstractIterator<ValueBlock>()
        {
            Iterator<? extends ValueBlock> sourceIterator = source.iterator();

            @Override
            protected ValueBlock computeNext()
            {
                int rangesCount = 0;
                ImmutableList.Builder<Range> ranges = ImmutableList.builder();
                while (rangesCount < 100 && sourceIterator.hasNext()) {
                    BlockCursor blockCursor = sourceIterator.next().blockCursor();
                    while (blockCursor.hasNextValue()) {
                        blockCursor.advanceNextValue();
                        if (predicate.apply(blockCursor)) {
                            ranges.add(new Range(blockCursor.getPosition(), blockCursor.getValuePositionEnd()));
                            rangesCount++;
                        }
                    }
                }
                if (rangesCount == 0) {
                    endOfData();
                    return null;
                }
                return new PositionsBlock(ranges.build());
            }
        };
    }

    @Override
    public Cursor cursor()
    {
        return new ValueCursor(INFO, source.iterator());
    }
}
