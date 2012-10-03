package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.AbstractBlockIterator;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.block.BlockIterator;
import com.facebook.presto.block.BlockIterators;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.Cursor.AdvanceResult;
import com.facebook.presto.block.Cursors;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.rle.RunLengthEncodedBlock;
import com.facebook.presto.block.rle.RunLengthEncodedCursor;

import static com.facebook.presto.block.Cursor.AdvanceResult.FINISHED;
import static com.facebook.presto.block.Cursor.AdvanceResult.MUST_YIELD;

/**
 * Group input data and produce a single block for each sequence of identical values.
 */
public class GroupByOperator
        implements TupleStream, BlockIterable<RunLengthEncodedBlock>
{
    private final TupleStream source;

    public GroupByOperator(TupleStream keySource)
    {
        this.source = keySource;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return source.getTupleInfo();
    }

    @Override
    public Range getRange()
    {
        return Range.ALL;
    }

    @Override
    public Cursor cursor()
    {
        return new RunLengthEncodedCursor(getTupleInfo(), iterator());
    }

    @Override
    public BlockIterator<RunLengthEncodedBlock> iterator()
    {
        final Cursor cursor = source.cursor();
        if (!Cursors.advanceNextPositionNoYield(cursor)) {
            return BlockIterators.emptyIterator();
        }

        return new AbstractBlockIterator<RunLengthEncodedBlock>()
        {
            private Tuple currentKey;
            private long currentKeyStartPosition;

            @Override
            protected RunLengthEncodedBlock computeNext()
            {
                if (cursor.isFinished()) {
                    return endOfData();
                }

                // get starting key and position, if we don't already have one from a prior yielded loop
                if (currentKey == null) {
                    currentKey = cursor.getTuple();
                    currentKeyStartPosition = cursor.getPosition();
                }

                // advance while the next value equals the current value
                long endPosition;
                do {
                    endPosition = cursor.getCurrentValueEndPosition();

                    AdvanceResult result = cursor.advanceNextValue();
                    if (result == MUST_YIELD) {
                        return setMustYield();
                    } else if (result == FINISHED) {
                        break;
                    }
                } while (cursor.currentTupleEquals(currentKey));

                // range does not include the current element
                Range range = Range.create(currentKeyStartPosition, endPosition);
                RunLengthEncodedBlock block = new RunLengthEncodedBlock(currentKey, range);

                // reset for next iteration
                currentKey = null;
                currentKeyStartPosition = -1;

                return block;
            }
        };
    }
}
