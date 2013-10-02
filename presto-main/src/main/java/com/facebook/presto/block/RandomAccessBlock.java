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

import com.facebook.presto.operator.SortOrder;
import com.facebook.presto.tuple.TupleReadable;
import io.airlift.slice.Slice;

public interface RandomAccessBlock
        extends Block
{
    /**
     * Gets a position from the current tuple.
     *
     * @throws IllegalArgumentException if this position is not valid
     */
    boolean getBoolean(int position);

    /**
     * Gets a position from the current tuple.
     *
     * @throws IllegalArgumentException if this position is not valid
     */
    long getLong(int position);

    /**
     * Gets a position from the current tuple.
     *
     * @throws IllegalArgumentException if this position is not valid
     */
    double getDouble(int position);

    /**
     * Gets a position from the current tuple.
     *
     * @throws IllegalArgumentException if this position is not valid
     */
    Slice getSlice(int position);

    int sliceCompareTo(int leftPosition, Slice rightSlice, int rightOffset, int rightLength);

    boolean sliceEquals(int leftPosition, Slice rightSlice, int rightOffset, int rightLength);

    /**
     * Is the specified position null.
     *
     * @throws IllegalArgumentException if this position is not valid
     */
    boolean isNull(int position);
    boolean equals(int position, RandomAccessBlock right, int rightPosition);
    boolean equals(int position, TupleReadable value);
    int hashCode(int position);
    int compareTo(SortOrder sortOrder, int position, RandomAccessBlock right, int rightPosition);
    void appendTupleTo(int position, BlockBuilder blockBuilder);
}
