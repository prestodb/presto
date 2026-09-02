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
import com.facebook.presto.common.function.SqlFunctionProperties;
import io.airlift.slice.Slice;

import java.util.concurrent.TimeUnit;

import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * TIMESTAMP(p) for p &lt;= {@link TimestampType#MAX_SHORT_PRECISION}: the value is a single
 * epoch-scaled {@code long} stored in a {@code LongArrayBlock}, so {@link #getJavaType()} is
 * {@code long.class}.
 */
public final class ShortTimestampType
        extends TimestampType
{
    // 10^p, the number of stored units per second.
    private static final long[] PRECISION_SCALE = {
            1L,                     // p=0  (seconds)
            10L,                    // p=1
            100L,                   // p=2
            1_000L,                 // p=3  (milliseconds)
            10_000L,                // p=4
            100_000L,               // p=5
            1_000_000L,             // p=6  (microseconds)
    };

    ShortTimestampType(int precision)
    {
        super(precision, long.class);
    }

    @Override
    public int getFixedSize()
    {
        return Long.BYTES;
    }

    @Override
    public long getLong(Block block, int position)
    {
        return block.getLong(position);
    }

    @Override
    public long getLongUnchecked(UncheckedBlock block, int internalPosition)
    {
        return block.getLongUnchecked(internalPosition);
    }

    @Override
    public Slice getSlice(Block block, int position)
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
        return leftBlock.getLong(leftPosition) == rightBlock.getLong(rightPosition);
    }

    @Override
    public long hash(Block block, int position)
    {
        return AbstractLongType.hash(block.getLong(position));
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        return Long.compare(leftBlock.getLong(leftPosition), rightBlock.getLong(rightPosition));
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        int maxBlockSizeInBytes = blockBuilderStatus == null
                ? PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES
                : blockBuilderStatus.getMaxPageSizeInBytes();
        return new LongArrayBlockBuilder(
                blockBuilderStatus,
                min(expectedEntries, maxBlockSizeInBytes / getFixedSize()));
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, getFixedSize());
    }

    @Override
    public BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new LongArrayBlockBuilder(null, positionCount);
    }

    // TODO(#27934 Phase 2): Implement for p=0–2 and p=4–5 once SqlTimestamp carries a precision.
    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        TimeUnit unit = toTimeUnit(getPrecision());
        if (properties.isLegacyTimestamp()) {
            return new SqlTimestamp(block.getLong(position), properties.getTimeZoneKey(), unit);
        }
        return new SqlTimestamp(block.getLong(position), unit);
    }

    // Floor division gives the correct epoch-second for negative (pre-1970) timestamps.
    @Override
    public long getEpochSecond(long timestamp)
    {
        return floorDiv(timestamp, PRECISION_SCALE[getPrecision()]);
    }

    // Floor modulo handles negative (pre-1970) timestamps correctly; Java % does not.
    @Override
    public int getNanos(long timestamp)
    {
        // scale <= 1_000_000 < 1e9 for every short precision, so the multiply below never overflows.
        long scale = PRECISION_SCALE[getPrecision()];
        return (int) (floorMod(timestamp, scale) * (1_000_000_000L / scale));
    }

    @Override
    public long toEpochMillis(long timestamp)
    {
        return getEpochSecond(timestamp) * 1_000L + getNanos(timestamp) / 1_000_000;
    }

    @Override
    public long toEpochMicros(long timestamp)
    {
        return getEpochSecond(timestamp) * 1_000_000L + getNanos(timestamp) / 1_000;
    }

    @Override
    public long fromEpochComponents(long epochSecond, int nanos)
    {
        if (nanos < 0 || nanos >= 1_000_000_000) {
            throw new IllegalArgumentException("nanos must be in range [0, 999_999_999]: " + nanos);
        }
        // scale is 10^p, so 1_000_000_000 / scale divides exactly for all short precisions p=0..6.
        long scale = PRECISION_SCALE[getPrecision()];
        return epochSecond * scale + nanos / (1_000_000_000L / scale);
    }

    private static TimeUnit toTimeUnit(int precision)
    {
        // Only p=3 (millis) and p=6 (micros) map directly to a TimeUnit.
        if (precision == DEFAULT_PRECISION) {
            return MILLISECONDS;
        }
        if (precision == MAX_SHORT_PRECISION) {
            return MICROSECONDS;
        }
        throw new UnsupportedOperationException(
                "Unsupported precision for TimeUnit conversion: TIMESTAMP(" + precision + ")");
    }
}
