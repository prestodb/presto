package com.facebook.presto.nblock;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.slice.Slice;

import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Applies a position range bound on a given cursor of arbitrary state.
 * The underlying cursor may be initialized or not, and may be at an arbitrary position.
 * RangeBoundedCursor will provide all valid positions/values within the specified range
 * and guarantees that the underlying cursor will not be advanced further
 * than one valid position beyond the specified range end position.
 * <p/>
 * A single underlying cursor may be passed serially to a contiguous sequence of RangeBoundedCursors
 * to completely process a TupleStream in segments.
 */
public class RangeBoundedBlockCursor
        implements BlockCursor
{
    private final Range validRange;
    private final BlockCursor cursor;
    private boolean initialized = false;

    public RangeBoundedBlockCursor(Range validRange, BlockCursor cursor)
    {
        this.validRange = checkNotNull(validRange, "validRange is null");
        this.cursor = checkNotNull(cursor, "cursor is null");
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return cursor.getTupleInfo();
    }

    @Override
    public Range getRange()
    {
        // TODO: we can improve the range tightness by taking the overlap with the delegate range
        return validRange;
    }

    @Override
    public boolean isValid()
    {
        return initialized && !isFinished();
    }

    @Override
    public boolean isFinished()
    {
        return cursor.isFinished() || (initialized && cursor.getPosition() > validRange.getEnd());
    }

    private void checkReadablePosition()
    {
        if (isFinished()) {
            throw new NoSuchElementException("already finished");
        }
        checkState(isValid(), "cursor not yet advanced");
    }

    /**
     * Attempts to advance the cursor to the initial position
     * <p/>
     * Precondition: cursor must not be finished
     */
    private boolean initializeUnderlyingCursor()
    {
        checkState(!initialized, "cursor is already initialized");
        checkState(!cursor.isFinished(), "cursor is already finished");

        if (!cursor.isValid()) {
            // Underlying cursor not yet initialized, so move to first position
            return cursor.advanceToPosition(validRange.getStart());
        } else if (cursor.getPosition() < validRange.getStart()) {
            // Cursor has not advanced into range yet
            return cursor.advanceToPosition(validRange.getStart());
        }
        return true;
    }

    @Override
    public boolean advanceNextValue()
    {
        if (isFinished()) {
            return false;
        }

        if (!initialized) {
            initializeUnderlyingCursor();
        }
        else {
            cursor.advanceToPosition(getCurrentValueEndPosition() + 1);
        }

        return processResult();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (isFinished()) {
            return false;
        }

        if (!initialized) {
            initializeUnderlyingCursor();
        }
        else {
            cursor.advanceNextPosition();
        }
        return processResult();
    }

    @Override
    public boolean advanceToPosition(long position)
    {
        if (isFinished()) {
            return false;
        }
        checkArgument(position >= validRange.getStart(), "target position is before start");
        if (!initialized) {
            initialized = true;
        }
        cursor.advanceToPosition(Math.min(position, validRange.getEnd() + 1));
        return processResult();
    }

    private boolean processResult()
    {
        initialized = true;

        // INVARIANT: cursor is at least ahead of start
        checkState(cursor.isFinished() || cursor.getPosition() >= validRange.getStart(), "invariant");
        return !cursor.isFinished() && cursor.getPosition() <= validRange.getEnd();
    }

    @Override
    public Tuple getTuple()
    {
        checkReadablePosition();
        return cursor.getTuple();
    }

    @Override
    public long getLong(int field)
    {
        checkReadablePosition();
        return cursor.getLong(field);
    }

    @Override
    public double getDouble(int field)
    {
        checkReadablePosition();
        return cursor.getDouble(field);
    }

    @Override
    public Slice getSlice(int field)
    {
        checkReadablePosition();
        return cursor.getSlice(field);
    }

    @Override
    public long getPosition()
    {
        checkReadablePosition();
        return cursor.getPosition();
    }

    @Override
    public long getCurrentValueEndPosition()
    {
        checkReadablePosition();
        // Should not report an end position beyond the valid range
        return Math.min(cursor.getCurrentValueEndPosition(), validRange.getEnd());
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        checkReadablePosition();
        return cursor.currentTupleEquals(value);
    }
}
