package com.facebook.presto.operators;

import com.facebook.presto.BlockStream;
import com.facebook.presto.Cursor;
import com.facebook.presto.Range;
import com.facebook.presto.RunLengthEncodedBlock;
import com.facebook.presto.RunLengthEncodedCursor;
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
    private boolean done;

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
                if (done) {
                    endOfData();
                    return null;
                }

                Tuple key = cursor.getTuple();

                long startPosition = cursor.getPosition();

                if (!cursor.hasNextValue()) {
                    done = true;
                    Range range = Range.create(startPosition, startPosition);
                    return new RunLengthEncodedBlock(key, range);
                }

                long lastPosition;
                do {
                    lastPosition = cursor.getPosition();
                    cursor.advanceNextValue();
                }
                while (cursor.equals(key) && cursor.hasNextValue());

                if (cursor.equals(key) && !cursor.hasNextValue()) {
                    // last element of the stream is part of the current range
                    done = true;
                    Range range = Range.create(startPosition, cursor.getPosition());
                    return new RunLengthEncodedBlock(key, range);
                }
                else {
                    // range does not include the current element
                    Range range = Range.create(startPosition, lastPosition);
                    return new RunLengthEncodedBlock(key, range);
                }
            }
        };
    }
}
