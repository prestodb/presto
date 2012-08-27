package com.facebook.presto;

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

    long getLong(int field);
    Slice getSlice(int field);

    boolean equals(Cursor other);
}
