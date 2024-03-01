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
package com.facebook.presto.common.block;

import io.airlift.slice.Slice;

/**
 * Accessors for Block which provide for raw indexing into the underlying data structure of the Block.
 * It is intended for hot path use cases where the speed of the boundary checks on Block is a performance
 * bottleneck.  Methods in this case must be used with more care, as they may return invalid data if the
 * indexing is not performed with respect to {@link #getOffsetBase} and {@code getPositionCount()}.
 * Implementations of UncheckedBlock should perform any boundary checks within assert statements so they can
 * be disabled in performance critical deployments by disabling assertions.
 *
 * Example usage:
 *
 * <pre>
 * {@code
 *  UncheckedBlock block = ...
 *  int sum = 0;
 *  for (int i = block.getOffsetBase(); i < block.getOffsetBase() + block.getPositionCount(); i++) {
 *      sum += block.getIntUnchecked(i);
 *  }
 * }
 * </pre>
 *
 * For nested structures, such as dictionaries and RLEs, the indexing is unchecked with respect to the top
 * level block, but not with respect to the inner blocks.  If, for performance reasons, it is desired to
 * use unchecked indexing also for the inner blocks, you may unpeel the blocks using existing utilities
 * such as {@link ColumnarArray}, {@link ColumnarMap}, {@link ColumnarRow} and {@link BlockFlattener}.
 */
public interface UncheckedBlock
{
    /**
     * Gets a byte value at {@code internalPosition - getOffsetBase()}.
     */
    default byte getByteUnchecked(int internalPosition)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Gets a little endian short at value at {@code internalPosition - getOffsetBase()}.
     */
    default short getShortUnchecked(int internalPosition)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Gets a little endian int value at {@code internalPosition - getOffsetBase()}.
     */
    default int getIntUnchecked(int internalPosition)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Gets a little endian long value at {@code internalPosition - getOffsetBase()}.
     */
    default long getLongUnchecked(int internalPosition)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Gets a little endian long at {@code offset} in the value at {@code internalPosition - getOffsetBase()}.
     */
    default long getLongUnchecked(int internalPosition, int offset)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Gets the length of the slice value at {@code internalPosition - getOffsetBase()}.
     * This method must be implemented if {@code getSliceUnchecked} is implemented.
     */
    default int getSliceLengthUnchecked(int internalPosition)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Gets a slice with offset {@code offset} at {@code internalPosition - getOffsetBase()}.
     */
    default Slice getSliceUnchecked(int internalPosition, int offset, int length)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Gets a Block value at {@code internalPosition - getOffsetBase()}.
     */
    default Block getBlockUnchecked(int internalPosition)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * @return true if value at {@code internalPosition - getOffsetBase()} is null
     */
    boolean isNullUnchecked(int internalPosition);

    /**
     * @return the internal offset of the underlying data structure of this block
     */
    int getOffsetBase();
}
