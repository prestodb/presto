/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.block;

import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import io.airlift.slice.Slice;

/**
 * Iterate as:
 * <p/>
 * <pre>{@code
 *  Cursor cursor = ...;
 * <p/>
 *  while (cursor.advanceNextPosition()) {
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
     * Gets the number of positions remaining in this cursor.
     */
    int getRemainingPositions();

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
     * Attempts to advance to the next position in this stream.
     */
    boolean advanceNextPosition();

    /**
     * Attempts to advance to the requested position.
     */
    boolean advanceToPosition(int position);

    /**
     * Creates a block view port starting at the next position and extending
     * the specified length. If the length is greater than the remaining
     * positions, the length wil be reduced.  This method advances the cursor
     * to the last position in the view port, so repeated calls to this method
     * will result in distinct chunks covering all positions of the cursor.
     * <p/>
     * For example, to get a sequence of regions with 1024 positions, use the
     * following code:
     * <p/>
     * <pre>{@code
     *  Cursor cursor = ...;
     * <p/>
     *  while (cursor.getRemainingPositions() > 0) {
     *     Block block = cursor.getRegionAndAdvance(1024);
     *     ...
     *  }
     * }</pre>
     */
    Block getRegionAndAdvance(int length);

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
    boolean getBoolean(int field);

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
     * Is the specified field null.
     *
     * @throws IllegalStateException if this cursor is not at a valid position
     */
    @Override
    boolean isNull(int field);

    /**
     * Returns the current position of this cursor
     *
     * @throws IllegalStateException if this cursor is not at a valid position
     */
    int getPosition();

    /**
     * True if the next tuple equals the specified tuple.
     *
     * @throws IllegalStateException if this cursor is not at a valid position
     */
    boolean currentTupleEquals(Tuple value);

    int getRawOffset();

    Slice getRawSlice();

    void appendTupleTo(BlockBuilder blockBuilder);
}
