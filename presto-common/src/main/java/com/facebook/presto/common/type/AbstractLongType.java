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
package com.facebook.presto.common.type;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockBuilderStatus;
import com.facebook.presto.common.block.LongArrayBlockBuilder;
import com.facebook.presto.common.block.PageBuilderStatus;
import com.facebook.presto.common.block.UncheckedBlock;
import io.airlift.slice.Slice;

import static java.lang.Long.rotateLeft;

/**
 * Base class for fixed-width types whose SPI Java type ({@link #getJavaType()}) is {@code long.class}.
 * The physical block layout may still be wider (e.g. {@code TimestampType} p=7–12 uses a 12-byte
 * Fixed12ArrayBlock); such subclasses may override these non-final methods:
 * <ul>
 *   <li>{@link #getFixedSize()}, {@link #getLong(Block, int)}, {@link #writeLong(BlockBuilder, long)}</li>
 *   <li>{@link #appendTo(Block, int, BlockBuilder)}</li>
 *   <li>{@link #createBlockBuilder} / {@link #createFixedSizeBlockBuilder}</li>
 * </ul>
 *
 * <p>{@link #getLongUnchecked} stays {@code final} and reads only the first {@code long} word;
 * do not rely on it for wider storage.
 * TODO(#27934 Phase 2): {@code TypeUtils.readNativeValue} must check {@code isShort()} before
 * dispatching {@code getJavaType() == long.class} to {@code getLong()} for p=7–12.
 */
public abstract class AbstractLongType
        extends AbstractPrimitiveType
        implements FixedWidthType
{
    public AbstractLongType(TypeSignature signature)
    {
        super(signature, long.class);
    }

    @Override
    public int getFixedSize()
    {
        return Long.BYTES;
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        return true;
    }

    @Override
    public long getLong(Block block, int position)
    {
        return block.getLong(position);
    }

    // TODO(#27934 Phase 3): returns only epochMicros for Fixed12ArrayBlock; fix vectorized operators before p=7–12 is SQL-reachable.
    @Override
    public final long getLongUnchecked(UncheckedBlock block, int internalPosition)
    {
        return block.getLongUnchecked(internalPosition);
    }

    @Override
    public final Slice getSlice(Block block, int position)
    {
        return block.getSlice(position, 0, getFixedSize());
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value)
    {
        blockBuilder.writeLong(value).closeEntry();
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            blockBuilder.writeLong(block.getLong(position)).closeEntry();
        }
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        long leftValue = leftBlock.getLong(leftPosition);
        long rightValue = rightBlock.getLong(rightPosition);
        return leftValue == rightValue;
    }

    @Override
    public long hash(Block block, int position)
    {
        return hash(block.getLong(position));
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        long leftValue = leftBlock.getLong(leftPosition);
        long rightValue = rightBlock.getLong(rightPosition);
        return Long.compare(leftValue, rightValue);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        int maxBlockSizeInBytes;
        if (blockBuilderStatus == null) {
            maxBlockSizeInBytes = PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
        }
        else {
            maxBlockSizeInBytes = blockBuilderStatus.getMaxPageSizeInBytes();
        }
        return new LongArrayBlockBuilder(
                blockBuilderStatus,
                Math.min(expectedEntries, maxBlockSizeInBytes / Long.BYTES));
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, Long.BYTES);
    }

    @Override
    public BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new LongArrayBlockBuilder(null, positionCount);
    }

    public static long hash(long value)
    {
        // xxhash64 mix
        return rotateLeft(value * 0xC2B2AE3D27D4EB4FL, 31) * 0x9E3779B185EBCA87L;
    }
}
