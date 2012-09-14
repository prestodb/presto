package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.SizeOf;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.position.UncompressedPositionBlock;
import com.facebook.presto.operation.ComparisonOperation;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;

import java.util.Iterator;

public class ComparisonOperator
        implements TupleStream, Iterable<UncompressedPositionBlock>
{
    private static final TupleInfo INFO = new TupleInfo();
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
        return INFO;
    }

    @Override
    public Range getRange()
    {
        return Range.ALL;
    }

    @Override
    public Cursor cursor()
    {
        return new GenericCursor(INFO, iterator());
    }

    @Override
    public Iterator<UncompressedPositionBlock> iterator()
    {
        final Cursor left = leftSource.cursor();
        final Cursor right = rightSource.cursor();

        boolean advancedLeft = left.advanceNextPosition();
        boolean advancedRight = right.advanceNextPosition();

        Preconditions.checkState(advancedLeft && advancedRight || !advancedLeft && !advancedRight, "Left and right don't have the same cardinality");

        if (!advancedLeft || !advancedRight) {
            return Iterators.emptyIterator();
        }

        return new AbstractIterator<UncompressedPositionBlock>()
        {
            private boolean done = false;

            @Override
            protected UncompressedPositionBlock computeNext()
            {
                if (done) {
                    endOfData();
                    return null;
                }

                ImmutableList.Builder<Long> positions = ImmutableList.builder();
                int count = 0;

                do {
                    long endPosition = Math.min(left.getCurrentValueEndPosition(), right.getCurrentValueEndPosition());

                    if (operation.evaluate(left, right)) {
                        // add all the positions for the current match
                        while (!done && left.getPosition() <= endPosition && count < MAX_POSITIONS_PER_BLOCK) {
                            positions.add(left.getPosition());
                            ++count;

                            done = !left.advanceNextPosition() || !right.advanceNextPosition();
                        }
                    }
                    else {
                        // skip ahead to the next position after the common range for the next round
                        done = !left.advanceToPosition(endPosition + 1) || !right.advanceToPosition(endPosition + 1);
                    }
                }
                while (!done && count < MAX_POSITIONS_PER_BLOCK);

                if (count == 0) {
                    endOfData();
                    return null;
                }

                return new UncompressedPositionBlock(positions.build());
            }
        };

    }
}
