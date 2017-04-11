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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.FixedWidthBlockBuilder;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.type.Decimals.MAX_PRECISION;
import static com.facebook.presto.spi.type.Decimals.decodeUnscaledValue;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.UNSCALED_DECIMAL_128_SLICE_LENGTH;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.compare;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

final class LongDecimalType
        extends DecimalType
{
    LongDecimalType(int precision, int scale)
    {
        super(precision, scale, Slice.class);
        validatePrecisionScale(precision, scale, MAX_PRECISION);
    }

    @Override
    public int getFixedSize()
    {
        return UNSCALED_DECIMAL_128_SLICE_LENGTH;
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        return new FixedWidthBlockBuilder(
                getFixedSize(),
                blockBuilderStatus,
                Math.min(expectedEntries, blockBuilderStatus.getMaxBlockSizeInBytes() / getFixedSize()));
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, getFixedSize());
    }

    @Override
    public BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new FixedWidthBlockBuilder(getFixedSize(), positionCount);
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        Slice slice = block.getSlice(position, 0, getFixedSize());
        return new SqlDecimal(decodeUnscaledValue(slice), getPrecision(), getScale());
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        return compareTo(leftBlock, leftPosition, rightBlock, rightPosition) == 0;
    }

    @Override
    public long hash(Block block, int position)
    {
        long low = block.getLong(position, 0);
        long high = block.getLong(position, SIZE_OF_LONG);
        return UnscaledDecimal128Arithmetic.hash(low, high);
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        long leftLow = leftBlock.getLong(leftPosition, 0);
        long leftHigh = leftBlock.getLong(leftPosition, SIZE_OF_LONG);
        long rightLow = rightBlock.getLong(rightPosition, 0);
        long rightHigh = rightBlock.getLong(rightPosition, SIZE_OF_LONG);
        return compare(leftLow, leftHigh, rightLow, rightHigh);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            block.writeBytesTo(position, 0, getFixedSize(), blockBuilder);
            blockBuilder.closeEntry();
        }
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        writeSlice(blockBuilder, value, 0, value.length());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        blockBuilder.writeBytes(value, offset, length).closeEntry();
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        return block.getSlice(position, 0, getFixedSize());
    }
}
