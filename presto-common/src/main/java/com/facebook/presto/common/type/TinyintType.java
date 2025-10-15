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

import com.facebook.presto.common.GenericInternalException;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockBuilderStatus;
import com.facebook.presto.common.block.ByteArrayBlock;
import com.facebook.presto.common.block.ByteArrayBlockBuilder;
import com.facebook.presto.common.block.PageBuilderStatus;
import com.facebook.presto.common.block.UncheckedBlock;
import com.facebook.presto.common.function.SqlFunctionProperties;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static java.lang.Long.rotateLeft;
import static java.lang.String.format;

public final class TinyintType
        extends AbstractPrimitiveType
        implements FixedWidthType
{
    public static final TinyintType TINYINT = new TinyintType();

    private TinyintType()
    {
        super(parseTypeSignature(StandardTypes.TINYINT), long.class);
    }

    @Override
    public int getFixedSize()
    {
        return Byte.BYTES;
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
        return new ByteArrayBlockBuilder(
                blockBuilderStatus,
                Math.min(expectedEntries, maxBlockSizeInBytes / Byte.BYTES));
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, Byte.BYTES);
    }

    @Override
    public BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new ByteArrayBlockBuilder(null, positionCount);
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
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        return block.getByte(position);
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        int leftValue = leftBlock.getByte(leftPosition);
        int rightValue = rightBlock.getByte(rightPosition);
        return leftValue == rightValue;
    }

    @Override
    public long hash(Block block, int position)
    {
        return hash(block.getByte(position));
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        // WARNING: the correctness of InCodeGenerator is dependent on the implementation of this
        // function being the equivalence of internal long representation.
        byte leftValue = leftBlock.getByte(leftPosition);
        byte rightValue = rightBlock.getByte(rightPosition);
        return Byte.compare(leftValue, rightValue);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            blockBuilder.writeByte(block.getByte(position)).closeEntry();
        }
    }

    @Override
    public long getLong(Block block, int position)
    {
        return (long) block.getByte(position);
    }

    @Override
    public byte getByte(Block block, int position)
    {
        return readByte((ByteArrayBlock) block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(position));
    }

    private static byte readByte(ByteArrayBlock block, int position)
    {
        return block.getByte(position);
    }

    @Override
    public long getLongUnchecked(UncheckedBlock block, int internalPosition)
    {
        return (long) block.getByteUnchecked(internalPosition);
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value)
    {
        if (value > Byte.MAX_VALUE) {
            throw new GenericInternalException(format("Value %d exceeds MAX_BYTE", value));
        }
        else if (value < Byte.MIN_VALUE) {
            throw new GenericInternalException(format("Value %d is less than MIN_BYTE", value));
        }

        blockBuilder.writeByte((int) value).closeEntry();
    }

    @Override
    public boolean equals(Object other)
    {
        return other == TINYINT;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }

    public static long hash(byte value)
    {
        // xxhash64 mix
        return rotateLeft(value * 0xC2B2AE3D27D4EB4FL, 31) * 0x9E3779B185EBCA87L;
    }
}
