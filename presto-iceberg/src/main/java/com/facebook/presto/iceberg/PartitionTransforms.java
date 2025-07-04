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
import jakarta.annotation.Nullable;
import org.apache.iceberg.PartitionField;
import org.joda.time.DateTimeField;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.function.LongUnaryOperator;
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
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static io.airlift.slice.SliceUtf8.offsetOfCodePoint;
import static java.lang.Integer.parseInt;
import static java.lang.Math.floorDiv;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.joda.time.chrono.ISOChronology.getInstanceUTC;

public final class PartitionTransforms
{
    private static final Pattern BUCKET_PATTERN = Pattern.compile("bucket\\[(\\d+)]");
    private static final Pattern TRUNCATE_PATTERN = Pattern.compile("truncate\\[(\\d+)]");
    private static final DateTimeField YEAR_UTC = getInstanceUTC().year();
    private static final DateTimeField MONTH_OF_YEAR_UTC = getInstanceUTC().monthOfYear();
    public static final int MILLISECONDS_PER_HOUR = 60 * 60 * 1000;
    public static final int MILLISECONDS_PER_DAY = MILLISECONDS_PER_HOUR * 24;

    private PartitionTransforms() {}

    /**
     * Returns a ColumnTransform based on an iceberg partition specification.
     * Year, Month, Day & Hour transform cases will return a transformBlock,
     * that will write a partition column value using INTEGER type.
     */
    public static ColumnTransform getColumnTransform(PartitionField field, Type type)
    {
        String transform = field.transform().toString();
        switch (transform) {
            case "identity":
                return new ColumnTransform(transform, type, Function.identity(), ValueTransform.identity(type));
            case "year":
                if (type.equals(DATE)) {
                    LongUnaryOperator transformYear = value -> epochYear(DAYS.toMillis(value));
                    return new ColumnTransform(transform, INTEGER,
                            block -> transformBlock(DATE, block, transformYear),
                            ValueTransform.from(DATE, transformYear));
                }
                if (type.equals(TIMESTAMP)) {
                    LongUnaryOperator transformYear = value -> epochYear(value);
                    return new ColumnTransform(transform, INTEGER,
                            block -> transformBlock(TIMESTAMP, block, transformYear),
                            ValueTransform.from(TIMESTAMP, transformYear));
                }
                throw new UnsupportedOperationException("Unsupported type for 'year': " + field);
            case "month":
                if (type.equals(DATE)) {
                    LongUnaryOperator transformMonth = value -> epochMonth(DAYS.toMillis(value));
                    return new ColumnTransform(transform, INTEGER,
                            block -> transformBlock(DATE, block, transformMonth),
                            ValueTransform.from(DATE, transformMonth));
                }
                if (type.equals(TIMESTAMP)) {
                    LongUnaryOperator transformMonth = value -> epochMonth(value);
                    return new ColumnTransform(transform, INTEGER,
                            block -> transformBlock(TIMESTAMP, block, transformMonth),
                            ValueTransform.from(TIMESTAMP, transformMonth));
                }
                throw new UnsupportedOperationException("Unsupported type for 'month': " + field);
            case "day":
                if (type.equals(DATE)) {
                    LongUnaryOperator transformDay = value -> epochDay(DAYS.toMillis(value));
                    return new ColumnTransform(transform, INTEGER,
                            block -> transformBlock(DATE, block, transformDay),
                            ValueTransform.from(DATE, transformDay));
                }
                if (type.equals(TIMESTAMP)) {
                    LongUnaryOperator transformDay = value -> epochDay(value);
                    return new ColumnTransform(transform, INTEGER,
                            block -> transformBlock(TIMESTAMP, block, transformDay),
                            ValueTransform.from(TIMESTAMP, transformDay));
                }
                throw new UnsupportedOperationException("Unsupported type for 'day': " + field);
            case "hour":
                if (type.equals(TIMESTAMP)) {
                    LongUnaryOperator transformHour = value -> epochHour(value);
                    return new ColumnTransform(transform, INTEGER,
                            block -> transformBlock(TIMESTAMP, block, transformHour),
                            ValueTransform.from(TIMESTAMP, transformHour));
                }
                throw new UnsupportedOperationException("Unsupported type for 'hour': " + field);
        }

        Matcher matcher = BUCKET_PATTERN.matcher(transform);
        if (matcher.matches()) {
            int count = parseInt(matcher.group(1));
            if (type.equals(INTEGER)) {
                return new ColumnTransform(transform, INTEGER,
                        block -> bucketInteger(block, count),
                        (block, position) -> bucketValueInteger(block, position, count));
            }
            if (type.equals(BIGINT)) {
                return new ColumnTransform(transform, INTEGER,
                        block -> bucketBigint(block, count),
                        (block, position) -> bucketValueBigint(block, position, count));
            }
            if (isShortDecimal(type)) {
                DecimalType decimal = (DecimalType) type;
                return new ColumnTransform(transform, INTEGER,
                        block -> bucketShortDecimal(decimal, block, count),
                        (block, position) -> bucketValueShortDecimal(decimal, block, position, count));
            }
            if (isLongDecimal(type)) {
                DecimalType decimal = (DecimalType) type;
                return new ColumnTransform(transform, INTEGER,
                        block -> bucketLongDecimal(decimal, block, count),
                        (block, position) -> bucketValueLongDecimal(decimal, block, position, count));
            }
            if (type.equals(DATE)) {
                return new ColumnTransform(transform, INTEGER,
                        block -> bucketDate(block, count),
                        (block, position) -> bucketValueDate(block, position, count));
            }
            if (type.equals(TIME)) {
                return new ColumnTransform(transform, INTEGER,
                        block -> bucketTime(block, count),
                        (block, position) -> bucketValueTime(block, position, count));
            }
            if (type instanceof VarcharType) {
                return new ColumnTransform(transform, INTEGER,
                        block -> bucketVarchar(block, count),
                        (block, position) -> bucketValueVarchar(block, position, count));
            }
            if (type.equals(VARBINARY)) {
                return new ColumnTransform(transform, INTEGER,
                        block -> bucketVarbinary(block, count),
                        (block, position) -> bucketValueVarbinary(block, position, count));
            }
            throw new UnsupportedOperationException("Unsupported type for 'bucket': " + field);
        }

        matcher = TRUNCATE_PATTERN.matcher(transform);
        if (matcher.matches()) {
            int width = parseInt(matcher.group(1));
            if (type.equals(INTEGER)) {
                return new ColumnTransform(transform, INTEGER,
                        block -> truncateInteger(block, width),
                        (block, position) -> {
                            if (block.isNull(position)) {
                                return null;
                            }
                            return truncateInteger(block, position, width);
                        });
            }
            if (type.equals(BIGINT)) {
                return new ColumnTransform(transform, BIGINT,
                        block -> truncateBigint(block, width),
                        (block, position) -> {
                            if (block.isNull(position)) {
                                return null;
                            }
                            return truncateBigint(block, position, width);
                        });
            }
            if (isShortDecimal(type)) {
                DecimalType decimal = (DecimalType) type;
                BigInteger unscaledWidth = BigInteger.valueOf(width);
                return new ColumnTransform(transform, type,
                        block -> truncateShortDecimal(decimal, block, unscaledWidth),
                        (block, position) -> {
                            if (block.isNull(position)) {
                                return null;
                            }
                            return truncateShortDecimal(decimal, block, position, unscaledWidth);
                        });
            }
            if (isLongDecimal(type)) {
                DecimalType decimal = (DecimalType) type;
                BigInteger unscaledWidth = BigInteger.valueOf(width);
                return new ColumnTransform(transform, type,
                        block -> truncateLongDecimal(decimal, block, unscaledWidth),
                        (block, position) -> {
                            if (block.isNull(position)) {
                                return null;
                            }
                            return truncateLongDecimal(decimal, block, position, unscaledWidth);
                        });
            }
            if (type instanceof VarcharType) {
                return new ColumnTransform(transform, VARCHAR,
                        block -> truncateVarchar(block, width),
                        (block, position) -> {
                            if (block.isNull(position)) {
                                return null;
                            }
                            return truncateVarchar(VARCHAR.getSlice(block, position), width);
                        });
            }
            if (type.equals(VARBINARY)) {
                return new ColumnTransform(transform, VARBINARY,
                        block -> truncateVarbinary(block, width),
                        (block, position) -> {
                            if (block.isNull(position)) {
                                return null;
                            }
                            return truncateVarbinary(VARBINARY.getSlice(block, position), width);
                        });
            }
            throw new UnsupportedOperationException("Unsupported type for 'truncate': " + field);
        }

        throw new UnsupportedOperationException("Unsupported partition transform: " + field);
    }

    private static Block bucketInteger(Block block, int count)
    {
        return bucketBlock(block, count, position -> bucketHash(INTEGER.getLong(block, position)));
    }

    private static int bucketValueInteger(Block block, int position, int count)
    {
        return bucketValue(block, position, count, pos -> bucketHash(INTEGER.getLong(block, pos)));
    }

    private static Block bucketBigint(Block block, int count)
    {
        return bucketBlock(block, count, position -> bucketHash(BIGINT.getLong(block, position)));
    }

    private static int bucketValueBigint(Block block, int position, int count)
    {
        return bucketValue(block, position, count, pos -> bucketHash(BIGINT.getLong(block, pos)));
    }

    private static Block bucketShortDecimal(DecimalType decimal, Block block, int count)
    {
        return bucketBlock(block, count, position -> {
            // TODO: write optimized implementation
            BigDecimal value = readBigDecimal(decimal, block, position);
            return bucketHash(Slices.wrappedBuffer(value.unscaledValue().toByteArray()));
        });
    }

    private static int bucketValueShortDecimal(DecimalType decimal, Block block, int position, int count)
    {
        return bucketValue(block, position, count, pos -> {
            // TODO: write optimized implementation
            BigDecimal value = readBigDecimal(decimal, block, pos);
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

    private static int bucketValueLongDecimal(DecimalType decimal, Block block, int position, int count)
    {
        return bucketValue(block, position, count, pos -> {
            // TODO: write optimized implementation
            BigDecimal value = readBigDecimal(decimal, block, pos);
            return bucketHash(Slices.wrappedBuffer(value.unscaledValue().toByteArray()));
        });
    }

    private static Block bucketDate(Block block, int count)
    {
        return bucketBlock(block, count, position -> bucketHash(DATE.getLong(block, position)));
    }

    private static int bucketValueDate(Block block, int position, int count)
    {
        return bucketValue(block, position, count, pos -> bucketHash(DATE.getLong(block, pos)));
    }

    private static Block bucketTime(Block block, int count)
    {
        return bucketBlock(block, count, position -> bucketHash(MILLISECONDS.toMicros(TIME.getLong(block, position))));
    }

    private static int bucketValueTime(Block block, int position, int count)
    {
        return bucketValue(block, position, count, pos -> bucketHash(MILLISECONDS.toMicros(TIME.getLong(block, pos))));
    }

    private static Block bucketVarchar(Block block, int count)
    {
        return bucketBlock(block, count, position -> bucketHash(VARCHAR.getSlice(block, position)));
    }

    private static int bucketValueVarchar(Block block, int position, int count)
    {
        return bucketValue(block, position, count, pos -> bucketHash(VARCHAR.getSlice(block, pos)));
    }

    private static Block bucketVarbinary(Block block, int count)
    {
        return bucketBlock(block, count, position -> bucketHash(VARCHAR.getSlice(block, position)));
    }

    private static int bucketValueVarbinary(Block block, int position, int count)
    {
        return bucketValue(block, position, count, pos -> bucketHash(VARCHAR.getSlice(block, pos)));
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

    private static Integer bucketValue(Block block, int position, int count, IntUnaryOperator hasher)
    {
        if (block.isNull(position)) {
            return null;
        }
        int hash = hasher.applyAsInt(position);
        int bucket = (hash & Integer.MAX_VALUE) % count;
        return bucket;
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
            INTEGER.writeLong(builder, truncateInteger(block, position, width));
        }
        return builder.build();
    }

    private static long truncateInteger(Block block, int position, int width)
    {
        long value = INTEGER.getLong(block, position);
        return value - ((value % width) + width) % width;
    }

    private static Block truncateBigint(Block block, int width)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            BIGINT.writeLong(builder, truncateBigint(block, position, width));
        }
        return builder.build();
    }

    private static long truncateBigint(Block block, int position, int width)
    {
        long value = BIGINT.getLong(block, position);
        return value - ((value % width) + width) % width;
    }

    private static Block truncateShortDecimal(DecimalType type, Block block, BigInteger unscaledWidth)
    {
        BlockBuilder builder = type.createBlockBuilder(null, block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            type.writeLong(builder, truncateShortDecimal(type, block, position, unscaledWidth));
        }
        return builder.build();
    }

    private static long truncateShortDecimal(DecimalType type, Block block, int position, BigInteger unscaledWidth)
    {
        // TODO: write optimized implementation
        BigDecimal value = readBigDecimal(type, block, position);
        value = truncateDecimal(value, unscaledWidth);
        return encodeShortScaledValue(value, type.getScale());
    }

    private static Block truncateLongDecimal(DecimalType type, Block block, BigInteger unscaledWidth)
    {
        BlockBuilder builder = type.createBlockBuilder(null, block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            type.writeSlice(builder, truncateLongDecimal(type, block, position, unscaledWidth));
        }
        return builder.build();
    }

    private static Slice truncateLongDecimal(DecimalType type, Block block, int position, BigInteger unscaledWidth)
    {
        // TODO: write optimized implementation
        BigDecimal value = readBigDecimal(type, block, position);
        value = truncateDecimal(value, unscaledWidth);
        return encodeScaledValue(value, type.getScale());
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
            VARBINARY.writeSlice(builder, truncateVarbinary(value, max));
        }
        return builder.build();
    }

    private static Slice truncateVarbinary(Slice value, int max)
    {
        if (value.length() > max) {
            value = value.slice(0, max);
        }
        return value;
    }

    static long epochYear(long epochMilli)
    {
        return YEAR_UTC.get(epochMilli) - 1970L;
    }

    static long epochMonth(long epochMilli)
    {
        long year = epochYear(epochMilli);
        return (year * 12) + MONTH_OF_YEAR_UTC.get(epochMilli) - 1L;
    }

    static long epochDay(long epochMilli)
    {
        return floorDiv(epochMilli, MILLISECONDS_PER_DAY);
    }

    static long epochHour(long epochMilli)
    {
        return floorDiv(epochMilli, MILLISECONDS_PER_HOUR);
    }

    /**
     * Returns a block that writes a partition column value using INTEGER type.
     * INTEGER type is used in DATE/TIMESTAMP cases based on an iceberg partition specification.
     */
    private static Block transformBlock(Type sourceType, Block block, LongUnaryOperator function)
    {
        BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            long value = sourceType.getLong(block, position);
            INTEGER.writeLong(builder, function.applyAsLong(value));
        }
        return builder.build();
    }

    public static class ColumnTransform
    {
        private final String transformName;
        private final Type type;
        private final Function<Block, Block> transform;
        private final ValueTransform valueTransform;

        public ColumnTransform(String transformName,
                Type type,
                Function<Block, Block> transform,
                ValueTransform valueTransform)
        {
            this.transformName = requireNonNull(transformName, "transformName is null");
            this.type = requireNonNull(type, "resultType is null");
            this.transform = requireNonNull(transform, "transform is null");
            this.valueTransform = requireNonNull(valueTransform, "valueTransform is null");
        }

        public String getTransformName()
        {
            return transformName;
        }

        public Type getType()
        {
            return type;
        }

        public Function<Block, Block> getTransform()
        {
            return transform;
        }

        public ValueTransform getValueTransform()
        {
            return valueTransform;
        }
    }

    public interface ValueTransform
    {
        static ValueTransform identity(Type type)
        {
            return (block, position) -> readNativeValue(type, block, position);
        }

        static ValueTransform from(Type sourceType, LongUnaryOperator transform)
        {
            return (block, position) -> {
                if (block.isNull(position)) {
                    return null;
                }
                return transform.applyAsLong(sourceType.getLong(block, position));
            };
        }

        @Nullable
        Object apply(Block block, int position);
    }
}
