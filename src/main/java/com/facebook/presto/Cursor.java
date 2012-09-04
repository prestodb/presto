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

    Tuple getTuple();

    long getLong(int field);
    Slice getSlice(int field);

    boolean equals(Cursor other);

    long getPosition();

    boolean equals(Tuple value);
    boolean equals(int field, Slice value);
}
