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
import com.facebook.presto.common.block.Fixed12ArrayBlockBuilder;
import com.facebook.presto.common.block.PageBuilderStatus;
import com.facebook.presto.common.function.SqlFunctionProperties;

import static com.facebook.presto.common.block.Fixed12ArrayBlock.FIXED12_BYTES;
import static com.facebook.presto.common.block.Fixed12ArrayBlock.SIZE_IN_BYTES_PER_POSITION;
import static java.lang.Math.min;
import static java.lang.String.format;

/**
 * TIMESTAMP(p) for p &gt; {@link TimestampType#MAX_SHORT_PRECISION}: the value does not fit in a
 * single {@code long}, so it is stored as an {@code (epochMicros, picosOfMicro)} pair in a
 * {@code Fixed12ArrayBlock} and {@link #getJavaType()} is {@link LongTimestamp}.
 */
public final class LongTimestampType
        extends TimestampType
{
    LongTimestampType(int precision)
    {
        super(precision, LongTimestamp.class);
    }

    @Override
    public int getFixedSize()
    {
        return FIXED12_BYTES;
    }

    /**
     * Not for scan/projection hot paths — allocates a {@link LongTimestamp} per call.
     */
    @Override
    public LongTimestamp getObject(Block block, int position)
    {
        return new LongTimestamp(block.getLong(position, 0), block.getInt(position));
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        LongTimestamp timestamp = (LongTimestamp) value;
        blockBuilder.writeLong(timestamp.getEpochMicros())
                .writeInt(timestamp.getPicosOfMicro())
                .closeEntry();
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            blockBuilder.writeLong(block.getLong(position, 0))
                    .writeInt(block.getInt(position))
                    .closeEntry();
        }
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        return leftBlock.getLong(leftPosition, 0) == rightBlock.getLong(rightPosition, 0)
                && leftBlock.getInt(leftPosition) == rightBlock.getInt(rightPosition);
    }

    @Override
    public long hash(Block block, int position)
    {
        // int->long widening sign-extends, but picosOfMicro is always non-negative, so this is safe.
        long epochHash = AbstractLongType.hash(block.getLong(position, 0));
        return 31 * epochHash + AbstractLongType.hash(block.getInt(position));
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        int epochCompare = Long.compare(leftBlock.getLong(leftPosition, 0), rightBlock.getLong(rightPosition, 0));
        if (epochCompare != 0) {
            return epochCompare;
        }
        return Integer.compare(leftBlock.getInt(leftPosition), rightBlock.getInt(rightPosition));
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        // expectedBytesPerEntry is ignored: a Fixed12ArrayBlock position always costs
        // SIZE_IN_BYTES_PER_POSITION, which is what the builder reports to BlockBuilderStatus.
        int maxBlockSizeInBytes = blockBuilderStatus == null
                ? PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES
                : blockBuilderStatus.getMaxPageSizeInBytes();
        return new Fixed12ArrayBlockBuilder(
                blockBuilderStatus,
                min(expectedEntries, maxBlockSizeInBytes / SIZE_IN_BYTES_PER_POSITION));
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, getFixedSize());
    }

    @Override
    public BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new Fixed12ArrayBlockBuilder(null, positionCount);
    }

    // TODO(#27934 Phase 2): Build a SqlTimestamp from the LongTimestamp representation.
    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        throw unsupported("getObjectValue");
    }

    @Override
    public long getEpochSecond(long timestamp)
    {
        throw unsupported("getEpochSecond");
    }

    @Override
    public int getNanos(long timestamp)
    {
        throw unsupported("getNanos");
    }

    @Override
    public long toEpochMillis(long timestamp)
    {
        throw unsupported("toEpochMillis");
    }

    @Override
    public long toEpochMicros(long timestamp)
    {
        throw unsupported("toEpochMicros");
    }

    @Override
    public long fromEpochComponents(long epochSecond, int nanos)
    {
        throw unsupported("fromEpochComponents");
    }

    private UnsupportedOperationException unsupported(String method)
    {
        return new UnsupportedOperationException(format(
                "%s is not supported for TIMESTAMP(%d); use getObject(block, position) to obtain the LongTimestamp representation",
                method,
                getPrecision()));
    }
}
