package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.position.PositionsBlock;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;

public class DataScan1
        implements TupleStream
{
    private static final int RANGES_PER_BLOCK = 100;
    private static final TupleInfo INFO = new TupleInfo();

    private final TupleStream source;
    private final Predicate<Cursor> predicate;

    public DataScan1(TupleStream source, Predicate<Cursor> predicate)
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
    public Range getRange()
    {
        return Range.ALL;
    }

    public Iterator<TupleStream> iterator()
    {
        return new AbstractIterator<TupleStream>()
        {
            Cursor cursor = source.cursor();

            @Override
            protected TupleStream computeNext()
            {
                int rangesCount = 0;
                ImmutableList.Builder<Range> ranges = ImmutableList.builder();
                while (rangesCount < RANGES_PER_BLOCK && cursor.advanceNextValue()) {
                    if (predicate.apply(cursor)) {
                        ranges.add(new Range(cursor.getPosition(), cursor.getCurrentValueEndPosition()));
                        rangesCount++;
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
        return new GenericCursor(INFO, iterator());
    }
}
