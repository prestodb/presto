package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.AbstractYieldingIterator;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.YieldingIterable;
import com.facebook.presto.block.YieldingIterator;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.Cursor.AdvanceResult;
import com.facebook.presto.block.Cursors;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.block.uncompressed.UncompressedCursor;
import com.facebook.presto.operation.BinaryOperation;
import com.google.common.base.Preconditions;

import static com.facebook.presto.block.Cursor.AdvanceResult.FINISHED;
import static com.facebook.presto.block.Cursor.AdvanceResult.MUST_YIELD;
import static com.facebook.presto.block.Cursor.AdvanceResult.SUCCESS;

/**
 * A binary operator that produces uncompressed blocks
 */
public class UncompressedBinaryOperator
        implements TupleStream, YieldingIterable<UncompressedBlock>
{
    private final TupleStream leftOperandSource;
    private final TupleStream rightOperandSource;
    private final BinaryOperation operation;

    public UncompressedBinaryOperator(TupleStream leftOperandSource, TupleStream rightOperandSource, BinaryOperation operation)
    {
        this.leftOperandSource = leftOperandSource;
        this.rightOperandSource = rightOperandSource;
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
        return new UncompressedCursor(getTupleInfo(), iterator(session));
    }

    @Override
    public YieldingIterator<UncompressedBlock> iterator(QuerySession session)
    {
        Preconditions.checkNotNull(session, "session is null");
        final Cursor left = leftOperandSource.cursor(session);
        final Cursor right = rightOperandSource.cursor(session);

        // todo advance cursors to first position - moving to the first position is not allowed to cause a yield
        boolean advancedLeft = Cursors.advanceNextPositionNoYield(left);
        boolean advancedRight = Cursors.advanceNextPositionNoYield(right);

        Preconditions.checkState(advancedLeft && advancedRight, "Empty source cursor"); // TODO: we should be able to support this scenario

        // todo add code to advance to watermark position
        return new AbstractYieldingIterator<UncompressedBlock>()
        {
            @Override
            protected UncompressedBlock computeNext()
            {
                // we assume, left is always advanced first, and left and right have the exact same positions
                if (left.isFinished()) {
                    return endOfData();
                }
                if (right.getPosition() < left.getPosition()) {
                    if (right.advanceNextPosition() == MUST_YIELD) {
                        return setMustYield();
                    }
                    Preconditions.checkState(right.getPosition() == left.getPosition());
                }

                BlockBuilder builder = new BlockBuilder(left.getPosition(), getTupleInfo());
                do {
                    long endPosition = Math.min(left.getCurrentValueEndPosition(), right.getCurrentValueEndPosition());

                    // TODO: arbitrary types
                    long value = operation.evaluateAsLong(left, right);
                    while (left.getPosition() <= endPosition && !builder.isFull()) {
                        builder.append(value);

                        // always advance left and then right
                        AdvanceResult result = left.advanceNextPosition();
                        if (result == SUCCESS) {
                            result = right.advanceNextPosition();
                        }

                        if (result != SUCCESS) {
                            if (!builder.isEmpty()) {
                                return builder.build();
                            }
                            if (result == MUST_YIELD) {
                                return setMustYield();
                            }
                            if (result == FINISHED) {
                                return endOfData();
                            }
                        }
                    }
                }
                while (!builder.isFull());

                return builder.build();
            }
        };
    }
}
