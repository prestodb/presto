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
package com.facebook.presto.spi.type;

import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

public interface VariableWidthType
        extends Type
{
    /**
     * Gets an object representation of the type encoded in the specified slice
     * at the specified offset.  This is the value returned to the user via the
     * REST endpoint and therefore must be JSON serializable.
     */
    Object getObjectValue(Session session, Slice slice, int offset);

    /**
     * Gets the length of the value at the specified offset in the specified slice.
     */
    int getLength(Slice slice, int offset);

    /**
     * Gets the value at the specified offset in the specified slice as a Slice.
     */
    Slice getSlice(Slice slice, int offset);

    /**
     * Writes the Slice value into the specified slice output.
     */
    int writeSlice(SliceOutput sliceOutput, Slice value, int offset, int length);

    /**
     * Are the values in the specified slices at the specified offsets equal?
     */
    boolean equalTo(Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset);

    /**
     * Is the value at the specified offset in the specified slice equal value
     * at the specified cursor?
     */
    boolean equalTo(Slice leftSlice, int leftOffset, BlockCursor rightCursor);

    /**
     * Calculates the hash code of the value at the specified offset in the
     * specified slice.
     */
    int hash(Slice slice, int offset);

    /**
     * Compare the values in the specified slices at the specified offsets equal?
     */
    int compareTo(Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset);

    /**
     * Append the value at the specified offset in the specified slice to the
     * specified block builder
     */
    void appendTo(Slice slice, int offset, BlockBuilder blockBuilder);

    /**
     * Append the value at the specified offset in the specified slice to the
     * specified slice output
     */
    void appendTo(Slice slice, int offset, SliceOutput sliceOutput);
}
