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
import com.facebook.presto.common.block.Fixed12ArrayBlock;
import com.facebook.presto.common.block.Fixed12ArrayBlockBuilder;
import com.facebook.presto.common.block.PageBuilderStatus;
import com.facebook.presto.common.function.SqlFunctionProperties;

import java.util.Objects;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

// Short timestamps (p ≤ 6) are stored as a single epoch-scaled long (e.g. epoch-millis for p=3,
// epoch-micros for p=6). High-precision timestamps (p > 6) require LongTimestamp, a separate
// 12-byte fixed-width value type that pairs epochMicros with a picosOfMicro remainder.
public final class TimestampType
        extends AbstractLongType
{
    public static final int MAX_PRECISION = 12;
    public static final int MAX_SHORT_PRECISION = 6;
    public static final int DEFAULT_PRECISION = 3;

    private static final long[] PRECISION_SCALE = {
            1L,                     // p=0  (seconds)
            10L,                    // p=1
            100L,                   // p=2
            1_000L,                 // p=3  (milliseconds)
            10_000L,                // p=4
            100_000L,               // p=5
            1_000_000L,             // p=6  (microseconds)
            10_000_000L,            // p=7
            100_000_000L,           // p=8
            1_000_000_000L,         // p=9  (nanoseconds)
            10_000_000_000L,        // p=10
            100_000_000_000L,       // p=11
            1_000_000_000_000L,     // p=12 (picoseconds)
    };

    private static final TimestampType[] INSTANCES = new TimestampType[MAX_PRECISION + 1];

    static {
        for (int p = 0; p <= MAX_PRECISION; p++) {
            INSTANCES[p] = new TimestampType(p);
        }
    }

    public static final TimestampType TIMESTAMP = INSTANCES[DEFAULT_PRECISION];

    // Interned instance for microsecond precision. Type signature is now "timestamp(6)".
    public static final TimestampType TIMESTAMP_MICROSECONDS = INSTANCES[MAX_SHORT_PRECISION];

    private final int precision;

    public static TimestampType createTimestampType(int precision)
    {
        if (precision < 0 || precision > MAX_PRECISION) {
            throw new IllegalArgumentException(format(
                    "TIMESTAMP precision must be in range [0, %s]: %s", MAX_PRECISION, precision));
        }
        return INSTANCES[precision];
    }

    private TimestampType(int precision)
    {
        super(buildTypeSignature(precision));
        this.precision = precision;
    }

    private static TypeSignature buildTypeSignature(int precision)
    {
        if (precision == DEFAULT_PRECISION) {
            // Preserve "timestamp" (no parameter) so existing serialized metadata continues to parse.
            return parseTypeSignature(StandardTypes.TIMESTAMP);
        }
        // All other precisions serialize as "timestamp(p)" with a numeric parameter.
        return new TypeSignature(StandardTypes.TIMESTAMP, TypeSignatureParameter.of((long) precision));
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        if (!isShort()) {
            throw new UnsupportedOperationException(
                    "getObjectValue is not supported for TIMESTAMP(" + precision + "); read the block as LongTimestamp");
        }
        // Normalize any short precision (0–6) to epoch-millis so SqlTimestamp always receives
        // a well-defined unit regardless of which precision was stored.
        long epochMillis = toEpochMillis(block.getLong(position));
        if (properties.isLegacyTimestamp()) {
            return new SqlTimestamp(epochMillis, properties.getTimeZoneKey(), MILLISECONDS);
        }
        return new SqlTimestamp(epochMillis, MILLISECONDS);
    }

    public int getPrecision()
    {
        return precision;
    }

    // p ≤ 6 fits in a single long; p > 6 requires the LongTimestamp value type.
    public boolean isShort()
    {
        return precision <= MAX_SHORT_PRECISION;
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        if (isShort()) {
            return super.createBlockBuilder(blockBuilderStatus, expectedEntries, expectedBytesPerEntry);
        }
        int maxBlockSizeInBytes;
        if (blockBuilderStatus == null) {
            maxBlockSizeInBytes = PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
        }
        else {
            maxBlockSizeInBytes = blockBuilderStatus.getMaxPageSizeInBytes();
        }
        return new Fixed12ArrayBlockBuilder(
                blockBuilderStatus,
                Math.min(expectedEntries, maxBlockSizeInBytes / Fixed12ArrayBlock.FIXED12_BYTES));
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        if (isShort()) {
            return super.createBlockBuilder(blockBuilderStatus, expectedEntries);
        }
        return createBlockBuilder(blockBuilderStatus, expectedEntries, Fixed12ArrayBlock.FIXED12_BYTES);
    }

    @Override
    public BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        if (isShort()) {
            return super.createFixedSizeBlockBuilder(positionCount);
        }
        return new Fixed12ArrayBlockBuilder(null, positionCount);
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object other)
    {
        // One interned instance per precision level, so reference equality is sufficient.
        return this == other;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), precision);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else if (!isShort()) {
            blockBuilder.writeLong(block.getLong(position, 0))
                    .writeInt(block.getInt(position))
                    .closeEntry();
        }
        else {
            blockBuilder.writeLong(block.getLong(position)).closeEntry();
        }
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        if (!isShort()) {
            return leftBlock.getLong(leftPosition, 0) == rightBlock.getLong(rightPosition, 0)
                    && leftBlock.getInt(leftPosition) == rightBlock.getInt(rightPosition);
        }
        return super.equalTo(leftBlock, leftPosition, rightBlock, rightPosition);
    }

    @Override
    public long hash(Block block, int position)
    {
        if (!isShort()) {
            long epochMicros = block.getLong(position, 0);
            int picosOfMicro = block.getInt(position);
            return AbstractLongType.hash(epochMicros) ^ AbstractLongType.hash(picosOfMicro);
        }
        return super.hash(block, position);
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        if (!isShort()) {
            int cmp = Long.compare(leftBlock.getLong(leftPosition, 0), rightBlock.getLong(rightPosition, 0));
            if (cmp != 0) {
                return cmp;
            }
            return Integer.compare(leftBlock.getInt(leftPosition), rightBlock.getInt(rightPosition));
        }
        return super.compareTo(leftBlock, leftPosition, rightBlock, rightPosition);
    }

    // Floor division handles negative (pre-1970) timestamps correctly; Java % does not.
    public long getEpochSecond(long timestamp)
    {
        checkShort("getEpochSecond");
        return floorDiv(timestamp, PRECISION_SCALE[precision]);
    }

    // Floor modulo handles negative (pre-1970) timestamps correctly; Java % does not.
    public int getNanos(long timestamp)
    {
        checkShort("getNanos");
        long fractional = floorMod(timestamp, PRECISION_SCALE[precision]);
        return (int) (fractional * (1_000_000_000L / PRECISION_SCALE[precision]));
    }

    public long toEpochMillis(long timestamp)
    {
        checkShort("toEpochMillis");
        return getEpochSecond(timestamp) * 1_000L + getNanos(timestamp) / 1_000_000;
    }

    private void checkShort(String method)
    {
        if (!isShort()) {
            throw new UnsupportedOperationException(
                    method + " requires a short timestamp (p ≤ " + MAX_SHORT_PRECISION + "); use LongTimestamp for p > " + MAX_SHORT_PRECISION);
        }
    }

    public long fromEpochComponents(long epochSecond, int nanos)
    {
        if (!isShort()) {
            // p > 6 timestamps are stored as LongTimestamp (epochMicros + picosOfMicro),
            // not as a single scaled long; this method cannot produce a valid value for them.
            throw new UnsupportedOperationException(
                    "fromEpochComponents requires a short timestamp (p ≤ 6); use LongTimestamp for p > 6");
        }
        // For p ≤ 6, scale ≤ 1_000_000, so 1_000_000_000 / scale is always ≥ 1000 (no zero divisor).
        long scale = PRECISION_SCALE[precision];
        return epochSecond * scale + nanos / (1_000_000_000L / scale);
    }
}
