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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import io.airlift.slice.Murmur3Hash32;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.iceberg.PartitionField;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.Decimals.encodeScaledValue;
import static com.facebook.presto.common.type.Decimals.encodeShortScaledValue;
import static com.facebook.presto.common.type.Decimals.isLongDecimal;
import static com.facebook.presto.common.type.Decimals.isShortDecimal;
import static com.facebook.presto.common.type.Decimals.readBigDecimal;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static io.airlift.slice.SliceUtf8.offsetOfCodePoint;
import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;

public final class PartitionTransforms
{
    private static final Pattern BUCKET_PATTERN = Pattern.compile("bucket\\[(\\d+)]");
    private static final Pattern TRUNCATE_PATTERN = Pattern.compile("truncate\\[(\\d+)]");

    private static final DateTimeField YEAR_FIELD = ISOChronology.getInstanceUTC().year();
    private static final DateTimeField MONTH_FIELD = ISOChronology.getInstanceUTC().monthOfYear();
    private static final DateTimeField DAY_OF_YEAR_FIELD = ISOChronology.getInstanceUTC().dayOfYear();
    private static final DateTimeField DAY_OF_MONTH_FIELD = ISOChronology.getInstanceUTC().dayOfMonth();

    private PartitionTransforms() {}

    public static ColumnTransform getColumnTransform(PartitionField field, Type type)
    {
        String transform = field.transform().toString();

        if (transform.equals("identity")) {
            return new ColumnTransform(type, Function.identity());
        }

        Matcher matcher = BUCKET_PATTERN.matcher(transform);
        if (matcher.matches()) {
            int count = parseInt(matcher.group(1));
            if (type.equals(INTEGER)) {
                return new ColumnTransform(INTEGER, block -> bucketInteger(block, count));
            }
            if (type.equals(BIGINT)) {
                return new ColumnTransform(INTEGER, block -> bucketBigint(block, count));
            }
            if (isShortDecimal(type)) {
                DecimalType decimal = (DecimalType) type;
                return new ColumnTransform(INTEGER, block -> bucketShortDecimal(decimal, block, count));
            }
            if (isLongDecimal(type)) {
                DecimalType decimal = (DecimalType) type;
                return new ColumnTransform(INTEGER, block -> bucketLongDecimal(decimal, block, count));
            }
            if (type.equals(DATE)) {
                return new ColumnTransform(INTEGER, block -> bucketDate(block, count));
            }
            if (type instanceof VarcharType) {
                return new ColumnTransform(INTEGER, block -> bucketVarchar(block, count));
            }
            if (type.equals(VARBINARY)) {
                return new ColumnTransform(INTEGER, block -> bucketVarbinary(block, count));
            }
            throw new UnsupportedOperationException("Unsupported type for 'bucket': " + field);
        }

        matcher = TRUNCATE_PATTERN.matcher(transform);
        if (matcher.matches()) {
            int width = parseInt(matcher.group(1));
            if (type.equals(INTEGER)) {
                return new ColumnTransform(INTEGER, block -> truncateInteger(block, width));
            }
            if (type.equals(BIGINT)) {
                return new ColumnTransform(BIGINT, block -> truncateBigint(block, width));
            }
            if (isShortDecimal(type)) {
                DecimalType decimal = (DecimalType) type;
                return new ColumnTransform(type, block -> truncateShortDecimal(decimal, block, width));
            }
            if (isLongDecimal(type)) {
                DecimalType decimal = (DecimalType) type;
                return new ColumnTransform(type, block -> truncateLongDecimal(decimal, block, width));
            }
            if (type instanceof VarcharType) {
                return new ColumnTransform(VARCHAR, block -> truncateVarchar(block, width));
            }
            if (type.equals(VARBINARY)) {
                return new ColumnTransform(VARBINARY, block -> truncateVarbinary(block, width));
            }
            throw new UnsupportedOperationException("Unsupported type for 'truncate': " + field);
        }

        throw new UnsupportedOperationException("Unsupported partition transform: " + field);
    }

    private static Block bucketInteger(Block block, int count)
    {
        return bucketBlock(block, count, position -> bucketHash(INTEGER.getLong(block, position)));
    }

    private static Block bucketBigint(Block block, int count)
    {
        return bucketBlock(block, count, position -> bucketHash(BIGINT.getLong(block, position)));
    }

    private static Block bucketShortDecimal(DecimalType decimal, Block block, int count)
    {
        return bucketBlock(block, count, position -> {
            // TODO: write optimized implementation
            BigDecimal value = readBigDecimal(decimal, block, position);
            return bucketHash(Slices.wrappedBuffer(value.unscaledValue().toByteArray()));
        });
    }

    private static Block bucketLongDecimal(DecimalType decimal, Block block, int count)
    {
        return bucketBlock(block, count, position -> {
            // TODO: write optimized implementation
            BigDecimal value = readBigDecimal(decimal, block, position);
            return bucketHash(Slices.wrappedBuffer(value.unscaledValue().toByteArray()));
        });
    }

    private static Block bucketDate(Block block, int count)
    {
        return bucketBlock(block, count, position -> bucketHash(DATE.getLong(block, position)));
    }

    private static Block bucketVarchar(Block block, int count)
    {
        return bucketBlock(block, count, position -> bucketHash(VARCHAR.getSlice(block, position)));
    }

    private static Block bucketVarbinary(Block block, int count)
    {
        return bucketBlock(block, count, position -> bucketHash(VARCHAR.getSlice(block, position)));
    }

    private static Block bucketBlock(Block block, int count, IntUnaryOperator hasher)
    {
        BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            int hash = hasher.applyAsInt(position);
            int bucket = (hash & Integer.MAX_VALUE) % count;
            INTEGER.writeLong(builder, bucket);
        }
        return builder.build();
    }

    private static int bucketHash(long value)
    {
        return Murmur3Hash32.hash(value);
    }

    private static int bucketHash(Slice value)
    {
        return Murmur3Hash32.hash(value);
    }

    private static Block truncateInteger(Block block, int width)
    {
        BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            long value = INTEGER.getLong(block, position);
            value -= ((value % width) + width) % width;
            INTEGER.writeLong(builder, value);
        }
        return builder.build();
    }

    private static Block truncateBigint(Block block, int width)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            long value = BIGINT.getLong(block, position);
            value -= ((value % width) + width) % width;
            BIGINT.writeLong(builder, value);
        }
        return builder.build();
    }

    private static Block truncateShortDecimal(DecimalType type, Block block, int width)
    {
        BigInteger unscaledWidth = BigInteger.valueOf(width);
        BlockBuilder builder = type.createBlockBuilder(null, block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            // TODO: write optimized implementation
            BigDecimal value = readBigDecimal(type, block, position);
            value = truncateDecimal(value, unscaledWidth);
            type.writeLong(builder, encodeShortScaledValue(value, type.getScale()));
        }
        return builder.build();
    }

    private static Block truncateLongDecimal(DecimalType type, Block block, int width)
    {
        BigInteger unscaledWidth = BigInteger.valueOf(width);
        BlockBuilder builder = type.createBlockBuilder(null, block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            // TODO: write optimized implementation
            BigDecimal value = readBigDecimal(type, block, position);
            value = truncateDecimal(value, unscaledWidth);
            type.writeSlice(builder, encodeScaledValue(value, type.getScale()));
        }
        return builder.build();
    }

    private static BigDecimal truncateDecimal(BigDecimal value, BigInteger unscaledWidth)
    {
        BigDecimal remainder = new BigDecimal(
                value.unscaledValue()
                        .remainder(unscaledWidth)
                        .add(unscaledWidth)
                        .remainder(unscaledWidth),
                value.scale());
        return value.subtract(remainder);
    }

    private static Block truncateVarchar(Block block, int max)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            Slice value = VARCHAR.getSlice(block, position);
            value = truncateVarchar(value, max);
            VARCHAR.writeSlice(builder, value);
        }
        return builder.build();
    }

    private static Slice truncateVarchar(Slice value, int max)
    {
        if (value.length() <= max) {
            return value;
        }
        int end = offsetOfCodePoint(value, 0, max);
        if (end < 0) {
            return value;
        }
        return value.slice(0, end);
    }

    private static Block truncateVarbinary(Block block, int max)
    {
        BlockBuilder builder = VARBINARY.createBlockBuilder(null, block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            Slice value = VARBINARY.getSlice(block, position);
            if (value.length() > max) {
                value = value.slice(0, max);
            }
            VARBINARY.writeSlice(builder, value);
        }
        return builder.build();
    }

    public static class ColumnTransform
    {
        private final Type type;
        private final Function<Block, Block> transform;

        public ColumnTransform(Type type, Function<Block, Block> transform)
        {
            this.type = requireNonNull(type, "resultType is null");
            this.transform = requireNonNull(transform, "transform is null");
        }

        public Type getType()
        {
            return type;
        }

        public Function<Block, Block> getTransform()
        {
            return transform;
        }
    }
}
