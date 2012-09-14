package com.facebook.presto.operator;

import com.facebook.presto.block.BlockStream;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.Range;
import com.facebook.presto.block.rle.RunLengthEncodedBlock;
import com.facebook.presto.block.rle.RunLengthEncodedCursor;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.google.common.collect.AbstractIterator;

import java.util.Iterator;

/**
 * Group input data and produce a single block for each sequence of identical values.
 */
public class GroupByBlockStream
        implements BlockStream, Iterable<RunLengthEncodedBlock>
{
    private final BlockStream source;

    public GroupByBlockStream(BlockStream keySource)
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

        return new AbstractIterator<RunLengthEncodedBlock>()
        {
            private boolean done;

            {
                // advance to first position
                done = !cursor.advanceNextPosition();
            }

            @Override
            protected RunLengthEncodedBlock computeNext()
            {
                // if no more data, return null
                if (done) {
                    endOfData();
                    return null;
                }

                // get starting key and position
                Tuple key = cursor.getTuple();
                long startPosition = cursor.getPosition();
                long endPosition = cursor.getCurrentValueEndPosition();

                // advance while the next value equals the current value
                while (cursor.advanceNextValue() && cursor.currentTupleEquals(key)) {
                    endPosition = cursor.getCurrentValueEndPosition();
                }

                if (cursor.isFinished()) {
                    // no more data
                    done = true;
                }

                // range does not include the current element
                Range range = Range.create(startPosition, endPosition);
                return new RunLengthEncodedBlock(key, range);
            }
        };
    }
}
