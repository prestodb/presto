package com.facebook.presto.operators;

import com.facebook.presto.BlockStream;
import com.facebook.presto.Cursor;
import com.facebook.presto.Range;
import com.facebook.presto.RunLengthEncodedBlock;
import com.facebook.presto.RunLengthEncodedCursor;
import com.facebook.presto.Slice;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.ValueBlock;
import com.google.common.collect.AbstractIterator;

import java.util.Iterator;

/**
 * Group input data and produce a single block for each sequence of identical values.
 */
public class GroupByBlockStream
    implements BlockStream<RunLengthEncodedBlock>
{
    private final BlockStream<? extends ValueBlock> source;

    public GroupByBlockStream(BlockStream<? extends ValueBlock> keySource)
    {
        this.source = keySource;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return source.getTupleInfo();
    }

    @Override
    public Cursor cursor()
    {
        return new RunLengthEncodedCursor(getTupleInfo(), iterator());
    }

    @Override
    public Iterator<RunLengthEncodedBlock> iterator()
    {
        final Cursor cursor = source.cursor();
        cursor.advanceNextValue();

        return new AbstractIterator<RunLengthEncodedBlock>()
        {
            @Override
            protected RunLengthEncodedBlock computeNext()
            {
                // if no more data, return null
                if (!cursor.hasNextValue()) {
                    endOfData();
                    return null;
                }

                Slice key = cursor.getSlice(0);

                long startPosition = cursor.getPosition();

                if (!cursor.hasNextValue()) {
                    Range range = Range.create(startPosition, startPosition);
                    return new RunLengthEncodedBlock(new Tuple(key, getTupleInfo()), range);
                }

                do {
                    cursor.advanceNextValue();
                }
                while (cursor.equals(0, key) && cursor.hasNextValue());


                long endPosition = cursor.getPosition() - 1;
                Range range = Range.create(startPosition, endPosition);

                return new RunLengthEncodedBlock(new Tuple(key, getTupleInfo()), range);
            }
        };
    }
}
