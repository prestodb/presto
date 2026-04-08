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
import com.facebook.presto.common.block.Fixed12Block;
import com.facebook.presto.common.block.Fixed12BlockBuilder;
import com.facebook.presto.common.block.PageBuilderStatus;
import com.facebook.presto.common.function.SqlFunctionProperties;

import static com.facebook.presto.common.block.Fixed12Block.FIXED12_BYTES;

/**
 * Long timestamp type (precision 7-12). Values are stored as 12 bytes
 * using {@link Fixed12Block}: the first 8 bytes store epoch microseconds
 * and the last 4 bytes store picoseconds within the microsecond (0-999999).
 */
public final class LongTimestampType
        extends TimestampType
{
    LongTimestampType(int precision)
    {
        super(precision, LongTimestamp.class);
        if (precision <= Timestamps.MAX_SHORT_PRECISION) {
            throw new IllegalArgumentException("Long timestamp precision must be > " + Timestamps.MAX_SHORT_PRECISION + ": " + precision);
        }
    }

    @Override
    public int getFixedSize()
    {
        return FIXED12_BYTES;
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        if (block instanceof Fixed12Block) {
            Fixed12Block fixed12Block = (Fixed12Block) block;
            long epochMicros = fixed12Block.getFixed12First(position);
            int picosOfMicro = fixed12Block.getFixed12Second(position);
            return new LongTimestamp(epochMicros, picosOfMicro);
        }

        // Fallback: read as long + int from generic block
        long epochMicros = block.getLong(position, 0);
        int picosOfMicro = block.getInt(position);
        return new LongTimestamp(epochMicros, picosOfMicro);
    }

    /**
     * Gets the LongTimestamp object from a block at the given position.
     */
    public LongTimestamp getObject(Block block, int position)
    {
        if (block instanceof Fixed12Block) {
            Fixed12Block fixed12Block = (Fixed12Block) block;
            return new LongTimestamp(
                    fixed12Block.getFixed12First(position),
                    fixed12Block.getFixed12Second(position));
        }
        return new LongTimestamp(
                block.getLong(position, 0),
                block.getInt(position));
    }

    /**
     * Writes a LongTimestamp value to a block builder.
     */
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        LongTimestamp timestamp = (LongTimestamp) value;
        if (blockBuilder instanceof Fixed12BlockBuilder) {
            ((Fixed12BlockBuilder) blockBuilder).writeFixed12(timestamp.getEpochMicros(), timestamp.getPicosOfMicro());
        }
        else {
            blockBuilder.writeLong(timestamp.getEpochMicros());
            blockBuilder.writeInt(timestamp.getPicosOfMicro());
            blockBuilder.closeEntry();
        }
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            LongTimestamp value = getObject(block, position);
            writeObject(blockBuilder, value);
        }
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        LongTimestamp left = getObject(leftBlock, leftPosition);
        LongTimestamp right = getObject(rightBlock, rightPosition);
        return left.equals(right);
    }

    @Override
    public long hash(Block block, int position)
    {
        LongTimestamp value = getObject(block, position);
        return value.hashCode();
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        LongTimestamp left = getObject(leftBlock, leftPosition);
        LongTimestamp right = getObject(rightBlock, rightPosition);
        return left.compareTo(right);
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
        return new Fixed12BlockBuilder(
                blockBuilderStatus,
                Math.min(expectedEntries, maxBlockSizeInBytes / FIXED12_BYTES));
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, FIXED12_BYTES);
    }

    @Override
    public BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new Fixed12BlockBuilder(null, positionCount);
    }
}
