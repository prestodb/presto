package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.SizeOf;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.AbstractYieldingIterator;
import com.facebook.presto.block.YieldingIterable;
import com.facebook.presto.block.YieldingIterator;
import com.facebook.presto.block.YieldingIterators;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.Cursor.AdvanceResult;
import com.facebook.presto.block.Cursors;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.position.UncompressedPositionBlock;
import com.facebook.presto.operation.ComparisonOperation;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import static com.facebook.presto.block.Cursor.AdvanceResult.FINISHED;
import static com.facebook.presto.block.Cursor.AdvanceResult.MUST_YIELD;
import static com.facebook.presto.block.Cursor.AdvanceResult.SUCCESS;

public class ComparisonOperator
        implements TupleStream, YieldingIterable<UncompressedPositionBlock>
{
    private static final int MAX_POSITIONS_PER_BLOCK = Ints.checkedCast(65536 / SizeOf.SIZE_OF_LONG);

    private final TupleStream leftSource;
    private final TupleStream rightSource;
    private final ComparisonOperation operation;

    public ComparisonOperator(TupleStream left, TupleStream right, ComparisonOperation operation)
    {
        Preconditions.checkNotNull(left, "left is null");
        Preconditions.checkNotNull(right, "right is null");
        Preconditions.checkNotNull(operation, "operation is null");

        this.leftSource = left;
        this.rightSource = right;
        this.operation = operation;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return TupleInfo.EMPTY;
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
        return new GenericCursor(session, TupleInfo.EMPTY, iterator(session));
    }

    @Override
    public YieldingIterator<UncompressedPositionBlock> iterator(QuerySession session)
    {
        Preconditions.checkNotNull(session, "session is null");
        final Cursor left = leftSource.cursor(session);
        final Cursor right = rightSource.cursor(session);

        boolean advancedLeft = Cursors.advanceNextPositionNoYield(left);
        boolean advancedRight = Cursors.advanceNextPositionNoYield(right);

        Preconditions.checkState(advancedLeft && advancedRight || !advancedLeft && !advancedRight, "Left and right don't have the same cardinality");

        if (!advancedLeft || !advancedRight) {
            return YieldingIterators.emptyIterator();
        }

        return new AbstractYieldingIterator<UncompressedPositionBlock>()
        {
            @Override
            protected UncompressedPositionBlock computeNext()
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

                ImmutableList.Builder<Long> positions = ImmutableList.builder();
                int count = 0;

                do {
                    long endPosition = Math.min(left.getCurrentValueEndPosition(), right.getCurrentValueEndPosition());

                    if (operation.evaluate(left, right)) {
                        // add all the positions for the current match
                        while (left.getPosition() <= endPosition && count < MAX_POSITIONS_PER_BLOCK) {
                            positions.add(left.getPosition());
                            ++count;

                            // always advance left and then right
                            AdvanceResult result = left.advanceNextPosition();
                            if (result == SUCCESS) {
                                result = right.advanceNextPosition();
                            }

                            if (result != SUCCESS) {
                                if (count != 0) {
                                    return new UncompressedPositionBlock(positions.build());
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
                    else {
                        // skip ahead to the next position after the common range for the next round
                        // always advance left and then right
                        AdvanceResult result = left.advanceToPosition(endPosition + 1);
                        if (result == SUCCESS) {
                            result = right.advanceToPosition(endPosition + 1);
                        }

                        if (result != SUCCESS) {
                            if (count != 0) {
                                return new UncompressedPositionBlock(positions.build());
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
                while (count < MAX_POSITIONS_PER_BLOCK);

                if (count == 0) {
                    endOfData();
                    return null;
                }

                return new UncompressedPositionBlock(positions.build());
            }
        };

    }
}
