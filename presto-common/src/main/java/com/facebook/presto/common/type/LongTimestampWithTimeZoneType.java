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
import static com.facebook.presto.common.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.common.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.common.type.DateTimeEncoding.unpackZoneKey;
import static com.facebook.presto.common.type.TimeZoneKey.getTimeZoneKey;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * TIMESTAMP(p) WITH TIME ZONE for p &gt; {@link TimestampWithTimeZoneType#MAX_SHORT_PRECISION}: the
 * short-form packed epoch-millis and time zone key {@code long} plus {@code picosOfMilli}, stored in a
 * {@code Fixed12ArrayBlock}, so {@link #getJavaType()} is {@link LongTimestampWithTimeZone}.
 */
public final class LongTimestampWithTimeZoneType
        extends TimestampWithTimeZoneType
{
    LongTimestampWithTimeZoneType(int precision)
    {
        super(precision, LongTimestampWithTimeZone.class);
    }

    @Override
    public int getFixedSize()
    {
        return FIXED12_BYTES;
    }

    /**
     * Not for scan/projection hot paths: allocates a {@link LongTimestampWithTimeZone} per call.
     */
    @Override
    public LongTimestampWithTimeZone getObject(Block block, int position)
    {
        long packedEpochMillis = block.getLong(position, 0);
        return new LongTimestampWithTimeZone(
                unpackMillisUtc(packedEpochMillis),
                block.getInt(position),
                unpackZoneKey(packedEpochMillis).getKey());
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) requireNonNull(value, "value is null");
        blockBuilder.writeLong(packDateTimeWithZone(timestamp.getEpochMillis(), getTimeZoneKey(timestamp.getTimeZoneKey())))
                .writeInt(timestamp.getPicosOfMilli())
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
        return unpackMillisUtc(leftBlock.getLong(leftPosition, 0)) == unpackMillisUtc(rightBlock.getLong(rightPosition, 0))
                && leftBlock.getInt(leftPosition) == rightBlock.getInt(rightPosition);
    }

    @Override
    public long hash(Block block, int position)
    {
        // int->long widening sign-extends, but picosOfMilli is always non-negative, so this is safe.
        long epochHash = AbstractLongType.hash(unpackMillisUtc(block.getLong(position, 0)));
        return 31 * epochHash + AbstractLongType.hash(block.getInt(position));
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        int epochCompare = Long.compare(unpackMillisUtc(leftBlock.getLong(leftPosition, 0)), unpackMillisUtc(rightBlock.getLong(rightPosition, 0)));
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

    // TODO(#27934 Phase 2): Build a SqlTimestampWithTimeZone from the LongTimestampWithTimeZone representation.
    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        throw new UnsupportedOperationException(format(
                "getObjectValue is not supported for TIMESTAMP(%d) WITH TIME ZONE: SqlTimestampWithTimeZone does not yet carry sub-millisecond precision",
                getPrecision()));
    }
}
