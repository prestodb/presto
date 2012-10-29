package com.facebook.presto.block.rle;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.Cursors;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.Iterator;

import static com.facebook.presto.block.Cursor.AdvanceResult.FINISHED;
import static com.facebook.presto.block.Cursor.AdvanceResult.SUCCESS;

public class RunLengthEncodedCursor
        implements Cursor
{
    private final TupleInfo info;
    private final PeekingIterator<RunLengthEncodedBlock> iterator;
    private final Range totalRange;
    private RunLengthEncodedBlock block;
    private long position;

    public RunLengthEncodedCursor(TupleInfo info, Iterator<RunLengthEncodedBlock> iterator)
    {
        this(info, iterator, Range.ALL);
    }

    public RunLengthEncodedCursor(TupleInfo info, Iterator<RunLengthEncodedBlock> iterator, Range totalRange)
    {
        Preconditions.checkNotNull(info, "info is null");
        Preconditions.checkArgument(iterator.hasNext(), "iterator is empty");
        Preconditions.checkNotNull(iterator, "iterator is null");

        this.info = info;
        this.iterator = Iterators.peekingIterator(iterator);
        this.totalRange = totalRange;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return info;
    }

    @Override
    public Range getRange()
    {
        return totalRange;
    }

    @Override
    public boolean isValid()
    {
        return block != null && !isFinished();
    }

    @Override
    public boolean isFinished()
    {
        return !iterator.hasNext() && (block == null || position > block.getRange().getEnd());
    }

    @Override
    public AdvanceResult advanceNextValue()
    {
        if (!iterator.hasNext()) {
            block = null;
            return FINISHED;
        }
        block = iterator.next();
        position = block.getRange().getStart();
        return SUCCESS;
    }

    @Override
    public AdvanceResult advanceNextPosition()
    {
        if (block == null || position == block.getRange().getEnd()) {
            return advanceNextValue();
        }
        else {
            position++;
            return SUCCESS;
        }
    }

    @Override
    public AdvanceResult advanceToPosition(long newPosition)
    {
        Preconditions.checkArgument(block == null || newPosition >= getPosition(), "Can't advance backwards");

        if (block == null) {
            if (iterator.hasNext()) {
                block = iterator.next();
            }
            else {
                return FINISHED;
            }
        }

        // skip to block containing requested position
        while (newPosition > block.getRange().getEnd() && iterator.hasNext()) {
            block = iterator.next();
        }

        if (newPosition > block.getRange().getEnd()) {
            block = null;
            return FINISHED;
        }

        this.position = Math.max(newPosition, block.getRange().getStart());
        return SUCCESS;
    }

    @Override
    public Tuple getTuple()
    {
        Cursors.checkReadablePosition(this);
        return block.getSingleValue();
    }

    @Override
    public long getLong(int field)
    {
        Cursors.checkReadablePosition(this);
        return block.getSingleValue().getLong(field);
    }

    @Override
    public double getDouble(int field)
    {
        Cursors.checkReadablePosition(this);
        return block.getSingleValue().getDouble(field);
    }

    @Override
    public Slice getSlice(int field)
    {
        Cursors.checkReadablePosition(this);
        return block.getSingleValue().getSlice(field);
    }

    @Override
    public long getPosition()
    {
        Cursors.checkReadablePosition(this);
        return position;
    }

    @Override
    public long getCurrentValueEndPosition()
    {
        Cursors.checkReadablePosition(this);
        return block.getRange().getEnd();
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        Cursors.checkReadablePosition(this);
        return block.getSingleValue().equals(value);
    }

}
