package com.facebook.presto.block;

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
 * than ond valid position beyond the specified range end position.
 *
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

    private void checkDataAccess()
    {
        if (!initialized) {
            throw new IllegalStateException("cursor not yet advanced");
        }
        if (isFinished()) {
            throw new NoSuchElementException("cursor advanced beyond last position");
        }
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
     * Guarantees that after successfully calling this method, the underlying cursor will be
     * initialized and at least with a position greater than or equal to the start position.
     *
     * Precondition: cursor must not be finished
     *
     * @return true if successfully initialized, false otherwise. If false is returned, isFinished() will return
     * true on all future calls.
     */
    private boolean initializeUnderlyingCursor() {
        if (!cursor.isValid()) {
            // Underlying cursor not yet initialized, so move to first position
            if (!cursor.advanceNextPosition()) {
                return false;
            }
        }
        // INVARIANT: underlying cursor isValid at this point
        checkState(cursor.isValid(), "invariant");

        if (cursor.getPosition() < validRange.getStart()) {
            // Cursor has not advanced into range yet
            if (!cursor.advanceToPosition(validRange.getStart())) {
                return false;
            }
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
            initialized = true;
            if (!initializeUnderlyingCursor()) {
                return false;
            }
        }
        else {
            if (!cursor.advanceToPosition(getCurrentValueEndPosition() + 1)) {
                return false;
            }
        }
        // INVARIANT: cursor is at least ahead of start
        checkState(cursor.getPosition() >= validRange.getStart(), "invariant");
        return cursor.getPosition() <= validRange.getEnd();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (isFinished()) {
            return false;
        }
        if (!initialized) {
            initialized = true;
            if (!initializeUnderlyingCursor()) {
                return false;
            }
        }
        else {
            if (!cursor.advanceNextPosition()) {
                return false;
            }
        }
        // INVARIANT: cursor is at least ahead of start
        checkState(cursor.getPosition() >= validRange.getStart(), "invariant");
        return cursor.getPosition() <= validRange.getEnd();
    }

    @Override
    public boolean advanceToPosition(long position)
    {
        if (isFinished()) {
            return false;
        }
        if (!initialized) {
            initialized = true;
        }
        checkArgument(position >= validRange.getStart(), "target position is before start");
        if (!cursor.advanceToPosition(Math.min(position, validRange.getEnd() + 1))) {
            // Advance only as far as one passed the last valid position (to ensure isFinished)
            return false;
        }
        // INVARIANT: cursor is at least ahead of start
        checkState(cursor.getPosition() >= validRange.getStart(), "invariant");
        return cursor.getPosition() <= validRange.getEnd();
    }

    @Override
    public Tuple getTuple()
    {
        checkDataAccess();
        return cursor.getTuple();
    }

    @Override
    public long getLong(int field)
    {
        checkDataAccess();
        return cursor.getLong(field);
    }

    @Override
    public double getDouble(int field)
    {
        checkDataAccess();
        return cursor.getDouble(field);
    }

    @Override
    public Slice getSlice(int field)
    {
        checkDataAccess();
        return cursor.getSlice(field);
    }

    @Override
    public long getPosition()
    {
        checkDataAccess();
        return cursor.getPosition();
    }

    @Override
    public long getCurrentValueEndPosition()
    {
        checkDataAccess();
        // Should not report an end position beyond the valid range
        return Math.min(cursor.getCurrentValueEndPosition(), validRange.getEnd());
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        checkDataAccess();
        return cursor.currentTupleEquals(value);
    }
}
