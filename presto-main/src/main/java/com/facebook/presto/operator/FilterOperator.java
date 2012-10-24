package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.AbstractYieldingIterator;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.Cursor.AdvanceResult;
import com.facebook.presto.block.Cursors;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.YieldingIterable;
import com.facebook.presto.block.YieldingIterator;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.google.common.base.Preconditions;

import static com.facebook.presto.block.Cursor.AdvanceResult.MUST_YIELD;
import static com.facebook.presto.block.Cursor.AdvanceResult.SUCCESS;

public class FilterOperator
        implements TupleStream, YieldingIterable<UncompressedBlock>
{
    private final TupleInfo tupleInfo;
    private final TupleStream source;
    private final TupleStream positions;

    public FilterOperator(TupleInfo tupleInfo, TupleStream source, TupleStream positions)
    {
        this.tupleInfo = tupleInfo;
        this.source = source;
        this.positions = positions;
    }

    @Override
    public Range getRange()
    {
        return Range.ALL;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    public YieldingIterator<UncompressedBlock> iterator(final QuerySession session)
    {
        Preconditions.checkNotNull(session, "session is null");
        final Cursor valueCursor = source.cursor(session);
        final Cursor positionsCursor = positions.cursor(session);

        return new AbstractYieldingIterator<UncompressedBlock>()
        {
            private long position;

            @Override
            protected UncompressedBlock computeNext()
            {
                if (valueCursor.isFinished() || positionsCursor.isFinished()) {
                    return endOfData();
                }

                BlockBuilder blockBuilder = new BlockBuilder(position, tupleInfo);
                AdvanceResult result;
                do {
                    // get the next valid position
                    // if position cursor is before value cursor, advance it to the current value cursor
                    // this can happen when the value cursor is advanced to a position it doesn't have
                    if (positionsCursor.isValid() && valueCursor.isValid() && positionsCursor.getPosition() < valueCursor.getPosition()) {
                        result = positionsCursor.advanceToPosition(valueCursor.getPosition());
                    } else {
                        result = positionsCursor.advanceNextPosition();
                    }
                    if (result != SUCCESS) {
                        break;
                    }
                    long nextPosition = positionsCursor.getPosition();

                    // advance value cursor to the position
                    result = valueCursor.advanceToPosition(nextPosition);
                    if (result != SUCCESS) {
                        break;
                    }

                    // if value cursor has the position, add it to the output block
                    if (nextPosition == valueCursor.getPosition()) {
                        Cursors.appendCurrentTupleToBlockBuilder(valueCursor, blockBuilder);
                    }
                } while (!blockBuilder.isFull());

                if (!blockBuilder.isEmpty()) {
                    UncompressedBlock block = blockBuilder.build();
                    position += block.getCount();
                    return block;
                }
                if (result == MUST_YIELD) {
                    return setMustYield();
                }
                return endOfData();
            }
        };
    }

    @Override
    public Cursor cursor(QuerySession session)
    {
        Preconditions.checkNotNull(session, "session is null");
        return new GenericCursor(session, getTupleInfo(), iterator(session));
    }
}
