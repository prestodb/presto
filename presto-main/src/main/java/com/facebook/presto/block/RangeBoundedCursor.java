package com.facebook.presto.block;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.slice.Slice;

import static com.facebook.presto.block.Cursor.AdvanceResult.*;
import static com.google.common.base.Preconditions.*;

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
public class RangeBoundedCursor
        implements Cursor
{
    private final Range validRange;
    private final Cursor cursor;
    private boolean initialized = false;

    public RangeBoundedCursor(Range validRange, Cursor cursor)
    {
        this.validRange = checkNotNull(validRange, "validRange is null");
        this.cursor = checkNotNull(cursor, "cursor is null");
    }

    public static RangeBoundedCursor bound(Cursor cursor, Range range)
    {
        return new RangeBoundedCursor(range, cursor);
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

    /**
     * Attempts to advance the cursor to the initial position
     * <p/>
     * Precondition: cursor must not be finished
     */
    private AdvanceResult initializeUnderlyingCursor()
    {
        checkState(!initialized, "cursor is already initialized");
        checkState(!cursor.isFinished(), "cursor is already finished");

        AdvanceResult result = SUCCESS;
        if (!cursor.isValid()) {
            // Underlying cursor not yet initialized, so move to first position
            result = cursor.advanceToPosition(validRange.getStart());
        } else if (cursor.getPosition() < validRange.getStart()) {
            // Cursor has not advanced into range yet
            result = cursor.advanceToPosition(validRange.getStart());
        }
        return result;
    }

    @Override
    public AdvanceResult advanceNextValue()
    {
        if (isFinished()) {
            return FINISHED;
        }

        AdvanceResult result;
        if (!initialized) {
            result = initializeUnderlyingCursor();
        }
        else {
            result = cursor.advanceToPosition(getCurrentValueEndPosition() + 1);
        }

        return processResult(result);
    }

    @Override
    public AdvanceResult advanceNextPosition()
    {
        if (isFinished()) {
            return FINISHED;
        }

        AdvanceResult result;
        if (!initialized) {
            result = initializeUnderlyingCursor();
        }
        else {
            result = cursor.advanceNextPosition();
        }
        return processResult(result);
    }

    @Override
    public AdvanceResult advanceToPosition(long position)
    {
        if (isFinished()) {
            return FINISHED;
        }
        checkArgument(position >= validRange.getStart(), "target position is before start");
        if (!initialized) {
            initialized = true;
        }
        AdvanceResult result = cursor.advanceToPosition(Math.min(position, validRange.getEnd() + 1));
        return processResult(result);
    }

    private AdvanceResult processResult(AdvanceResult result)
    {
        if (result == MUST_YIELD) {
            return MUST_YIELD;
        }

        initialized = true;

        // INVARIANT: cursor is at least ahead of start
        checkState(cursor.isFinished() || cursor.getPosition() >= validRange.getStart(), "invariant");
        return !cursor.isFinished() && cursor.getPosition() <= validRange.getEnd() ? SUCCESS : FINISHED;
    }

    @Override
    public Tuple getTuple()
    {
        Cursors.checkReadablePosition(this);
        return cursor.getTuple();
    }

    @Override
    public long getLong(int field)
    {
        Cursors.checkReadablePosition(this);
        return cursor.getLong(field);
    }

    @Override
    public double getDouble(int field)
    {
        Cursors.checkReadablePosition(this);
        return cursor.getDouble(field);
    }

    @Override
    public Slice getSlice(int field)
    {
        Cursors.checkReadablePosition(this);
        return cursor.getSlice(field);
    }

    @Override
    public long getPosition()
    {
        Cursors.checkReadablePosition(this);
        return cursor.getPosition();
    }

    @Override
    public long getCurrentValueEndPosition()
    {
        Cursors.checkReadablePosition(this);
        // Should not report an end position beyond the valid range
        return Math.min(cursor.getCurrentValueEndPosition(), validRange.getEnd());
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        Cursors.checkReadablePosition(this);
        return cursor.currentTupleEquals(value);
    }
}
