package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.block.uncompressed.UncompressedCursor;
import com.facebook.presto.operation.BinaryOperation;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

import java.util.Iterator;

/**
 * A binary operator that produces uncompressed blocks
 */
public class UncompressedBinaryOperator
        implements TupleStream, Iterable<UncompressedBlock>
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
    public Cursor cursor()
    {
        return new UncompressedCursor(getTupleInfo(), iterator());
    }

    @Override
    public Iterator<UncompressedBlock> iterator()
    {
        final Cursor left = leftOperandSource.cursor();
        final Cursor right = rightOperandSource.cursor();

        boolean advancedLeft = left.advanceNextPosition();
        boolean advancedRight = right.advanceNextPosition();

        Preconditions.checkState(advancedLeft && advancedRight, "Empty source cursor"); // TODO: we should be able to support this scenario

        return new AbstractIterator<UncompressedBlock>()
        {
            private boolean done = false;

            @Override
            protected UncompressedBlock computeNext()
            {
                if (done) {
                    endOfData();
                    return null;
                }

                BlockBuilder builder = new BlockBuilder(left.getPosition(), getTupleInfo());
                do {
                    long endPosition = Math.min(left.getCurrentValueEndPosition(), right.getCurrentValueEndPosition());

                    // TODO: arbitrary types
                    long value = operation.evaluateAsLong(left, right);

                    while (!done && left.getPosition() <= endPosition && !builder.isFull()) {
                        builder.append(value);

                        done = !left.advanceNextPosition() || !right.advanceNextPosition();
                    }
                }
                while (!done && !builder.isFull());

                return builder.build();
            }
        };
    }
}
