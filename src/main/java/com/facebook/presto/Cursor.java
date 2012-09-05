package com.facebook.presto;

import com.facebook.presto.slice.Slice;

/**
 * Iterate as:
 *
 * <pre>{@code
 *  Cursor cursor = ...;
 *
 *  while (cursor.hasNextValue()) {
 *     cursor.advanceNextValue();
 *     long value = cursor.getLong(...);
 *     ...
 *  }
 * }</pre>
 *
 */
public interface Cursor
{
    TupleInfo getTupleInfo();

    boolean hasNextValue();

    /**
     * Advances to the first position of the next value
     */
    void advanceNextValue();

    boolean hasNextPosition();

    /**
     * Advances to the next position in this stream and possibly to the next value if the current position if the last one for the current value
     */
    void advanceNextPosition();

    /**
     * Advances to the requested position or the next immediately available if that position does not exist in this stream (e.g., there's a gap in the sequence)
     */
    void advanceToPosition(long position);

    /**
     * Gets the current tuple.
     *
     * @throws java.util.NoSuchElementException if this cursor has not been advanced yet
     *
     */
    Tuple getTuple();

    /**
     * Gets a field from the current tuple.
     *
     * @throws java.util.NoSuchElementException if this cursor has not been advanced yet
     *
     */
    long getLong(int field);

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
     * Returns the first position of the next value if one is available.
     *
     * @throws java.util.NoSuchElementException if this cursor is already at the last value
     */
    long peekNextValuePosition();

    long getCurrentValueEndPosition();

    /**
     * True if the next tuple equals the specified tuple.
     *
     * @throws java.util.NoSuchElementException if this cursor has not been advanced yet
     */
    boolean currentValueEquals(Tuple value);

    /**
     * True if the next tuple equals the specified tuple.
     *
     * @throws java.util.NoSuchElementException if this cursor has not been advanced yet
     */
    boolean nextValueEquals(Tuple value);
}
