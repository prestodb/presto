package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.AbstractBlockIterator;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.block.BlockIterator;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.rle.RunLengthEncodedBlock;
import com.facebook.presto.block.rle.RunLengthEncodedCursor;
import com.facebook.presto.operation.BinaryOperation;
import com.google.common.base.Preconditions;

/**
 * A binary operator that produces RLE blocks
 */
public class RunLengthBinaryOperator
        implements TupleStream, BlockIterable<RunLengthEncodedBlock>
{
    private final TupleStream leftSource;
    private final TupleStream rightSource;
    private final BinaryOperation operation;

    public RunLengthBinaryOperator(TupleStream leftSource, TupleStream rightSource, BinaryOperation operation)
    {
        this.leftSource = leftSource;
        this.rightSource = rightSource;
        this.operation = operation;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return operation.getTupleInfo();
    }

    @Override
    public Range getRange()
    {
        return Range.ALL;
    }

    @Override
    public Cursor cursor(QuerySession session)
    {
        Preconditions.checkNotNull(session, "session is null");
        return new RunLengthEncodedCursor(getTupleInfo(), iterator(session));
    }

    @Override
    public BlockIterator<RunLengthEncodedBlock> iterator(QuerySession session)
    {
        Preconditions.checkNotNull(session, "session is null");
        final Cursor left = leftSource.cursor(session);
        final Cursor right = rightSource.cursor(session);

        left.advanceNextPosition();
        right.advanceNextPosition();

        Preconditions.checkState(!left.isFinished() && !right.isFinished(), "Source cursors are empty"); // TODO: we should be able to handle this case

        return new AbstractBlockIterator<RunLengthEncodedBlock>()
        {
            @Override
            protected RunLengthEncodedBlock computeNext()
            {
                Preconditions.checkState(left.isFinished() && right.isFinished() || !left.isFinished() && !right.isFinished(), "Left and right streams don't have the same cardinality");

                if (left.isFinished() || right.isFinished()) {
                    endOfData();
                    return null;
                }

                Preconditions.checkState(left.getPosition() == right.getPosition(), "Left and right streams are out of sync");

                long endPosition = Math.min(left.getCurrentValueEndPosition(), right.getCurrentValueEndPosition());

                Tuple value = operation.evaluate(left, right);
                RunLengthEncodedBlock result = new RunLengthEncodedBlock(value, Range.create(left.getPosition(), endPosition));

                // skip ahead to the next position after the common range for the next round
                left.advanceToPosition(endPosition + 1);
                right.advanceToPosition(endPosition + 1);

                return result;
            }
        };

    }
}
