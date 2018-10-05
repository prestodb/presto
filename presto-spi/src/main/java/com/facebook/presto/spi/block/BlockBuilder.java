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
package com.facebook.presto.spi.block;

import io.airlift.slice.Slice;

public abstract class BlockBuilder
        extends Block
{
    /**
     * Write a byte to the current entry;
     */
    public BlockBuilder writeByte(int value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Write a short to the current entry;
     */
    public BlockBuilder writeShort(int value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Write a int to the current entry;
     */
    public BlockBuilder writeInt(int value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Write a long to the current entry;
     */
    public BlockBuilder writeLong(long value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Write a byte sequences to the current entry;
     */
    public BlockBuilder writeBytes(Slice source, int sourceIndex, int length)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Return a writer to the current entry. The caller can operate on the returned caller to incrementally build the object. This is generally more efficient than
     * building the object elsewhere and call writeObject afterwards because a large chunk of memory could potentially be unnecessarily copied in this process.
     */
    public BlockBuilder beginBlockEntry()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Create a new block from the current materialized block by keeping the same elements
     * only with respect to {@code visiblePositions}.
     */
    public Block getPositions(int[] visiblePositions, int offset, int length)
    {
        return build().getPositions(visiblePositions, offset, length);
    }

    /**
     * Write a byte to the current entry;
     */
    public BlockBuilder closeEntry()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Appends a null value to the block.
     */
    public BlockBuilder appendNull()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Append a struct to the block and close the entry.
     */
    public BlockBuilder appendStructure(Block value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Do not use this interface outside block package.
     * Instead, use Block.writePositionTo(BlockBuilder, position)
     */
    public BlockBuilder appendStructureInternal(Block block, int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Builds the block. This method can be called multiple times.
     */
    public Block build()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Creates a new block builder of the same type based on the current usage statistics of this block builder.
     */
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }
}
