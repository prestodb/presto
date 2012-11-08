package com.facebook.presto.block;

import com.facebook.presto.tuple.TupleReadable;
import com.facebook.presto.util.Range;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.slice.Slice;

/**
 * Iterate as:
 * <p/>
 * <pre>{@code
 *  Cursor cursor = ...;
 * <p/>
 *  while (cursor.advanceNextValue()) {
 *     long value = cursor.getLong(...);
 *     ...
 *  }
 * }</pre>
 */
public interface BlockCursor
    extends TupleReadable
{
    /**
     * Gets the type of all tuples in this cursor
     */
    @Override
    TupleInfo getTupleInfo();

    /**
     * Gets the upper bound on the range of this cursor
     */
    Range getRange();

    /**
     * Returns true if the current position of the cursor is valid; false if
     * the cursor has not been advanced yet, or if the cursor has advanced
     * beyond the last position.
     * INVARIANT 1: isValid is false if isFinished is true
     * INVARIANT 2: all get* and data access methods will throw java.util.IllegalStateException while isValid is false
     */
    boolean isValid();

    /**
     * Returns true if the cursor has advanced beyond its last position.
     * INVARIANT 1: isFinished will only return true once advance* has returned false.
     * INVARIANT 2: all get* and data access methods will throw java.util.IllegalStateException once isFinished is true
     */
    boolean isFinished();

    /**
     * Attempts to advance to the first position of the next value
     */
    boolean advanceNextValue();

    /**
     * Attempts to advance to the next position in this stream and possibly to the next value if the current position if the last one for the current value
     */
    boolean advanceNextPosition();

    /**
     * Attempts to advance to the requested position or the next immediately available if that position does not exist in this stream (e.g., there's a gap in the sequence)
     */
    boolean advanceToPosition(long position);

    /**
     * Gets the current tuple.
     *
     * @throws IllegalStateException if this cursor is not at a valid position
     */
    @Override
    Tuple getTuple();

    /**
     * Gets a field from the current tuple.
     *
     * @throws IllegalStateException if this cursor is not at a valid position
     */
    @Override
    long getLong(int field);

    /**
     * Gets a field from the current tuple.
     *
     * @throws IllegalStateException if this cursor is not at a valid position
     */
    @Override
    double getDouble(int field);

    /**
     * Gets a field from the current tuple.
     *
     * @throws IllegalStateException if this cursor is not at a valid position
     */
    @Override
    Slice getSlice(int field);

    /**
     * Returns the current position of this cursor
     *
     * @throws IllegalStateException if this cursor is not at a valid position
     */
    long getPosition();

    /**
     * Returns the last position of the current value
     *
     * @throws IllegalStateException if this cursor is not at a valid position
     */
    long getCurrentValueEndPosition();

    /**
     * True if the next tuple equals the specified tuple.
     *
     * @throws IllegalStateException if this cursor is not at a valid position
     */
    boolean currentTupleEquals(Tuple value);
}
