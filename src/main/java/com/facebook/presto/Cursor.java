package com.facebook.presto;

import com.facebook.presto.slice.Slice;

/**
 * Iterate as:
 * <p/>
 * <pre>{@code
 *  Cursor cursor = ...;
 * <p/>
 *  while (cursor.hasNextValue()) {
 *     cursor.advanceNextValue();
 *     long value = cursor.getLong(...);
 *     ...
 *  }
 * }</pre>
 */
public interface Cursor
{
    TupleInfo getTupleInfo();

    /**
     * Is there any more data in the cursor.  When a cursor is finished, the cursor does not have a current value.
     */
    boolean isFinished();

    /**
     * Attempts to advance to the first position of the next value
     *
     * @returns true if the stream had a next value; false if the stream contains no more data
     */
    boolean advanceNextValue();

    /**
     * Attempts to advance to the next position in this stream and possibly to the next value if the current position if the last one for the current value
     *
     * @returns true if the stream had a next position; false if the stream contains no more data
     */
    boolean advanceNextPosition();

    /**
     * Attempts to advance to the requested position or the next immediately available if that position does not exist in this stream (e.g., there's a gap in the sequence)
     *
     * @returns true if the stream advanced to the position; false if the stream contains no more data
     */
    boolean advanceToPosition(long position);

    /**
     * Gets the current tuple.
     *
     * @throws java.util.NoSuchElementException if this cursor has not been advanced yet
     */
    Tuple getTuple();

    /**
     * Gets a field from the current tuple.
     *
     * @throws java.util.NoSuchElementException if this cursor has not been advanced yet
     */
    long getLong(int field);

    /**
     * Gets a field from the current tuple.
     *
     * @throws java.util.NoSuchElementException if this cursor has not been advanced yet
     */
    double getDouble(int field);

    /**
     * Gets a field from the current tuple.
     *
     * @throws java.util.NoSuchElementException if this cursor has not been advanced yet
     *
     */
    Slice getSlice(int field);

    /**
     * Returns the current position of this cursor
     *
     * @throws java.util.NoSuchElementException if this cursor has not been advanced yet
     */
    long getPosition();

    /**
     * Returns the last position of the current value
     *
     * @throws java.util.NoSuchElementException if this cursor has not been advanced yet
     */
    long getCurrentValueEndPosition();

    /**
     * True if the next tuple equals the specified tuple.
     *
     * @throws java.util.NoSuchElementException if this cursor has not been advanced yet
     */
    boolean currentValueEquals(Tuple value);
}
