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
    void advanceNextValue();

    boolean hasNextPosition();
    void advanceNextPosition();

    Tuple getTuple();

    long getLong(int field);
    Slice getSlice(int field);

    boolean equals(Cursor other);

    long getPosition();

    boolean equals(Tuple value);
    boolean equals(int field, Slice value);
}
