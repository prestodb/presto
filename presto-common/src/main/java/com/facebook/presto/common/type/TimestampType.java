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
import com.facebook.presto.common.function.SqlFunctionProperties;

import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.Timestamps.MAX_PRECISION;
import static com.facebook.presto.common.type.Timestamps.MAX_SHORT_PRECISION;
import static java.lang.String.format;

/**
 * TIMESTAMP type with precision 0..12.
 * <p>
 * Precision 0-6 (short timestamps) are stored as a single long value:
 * - Precision 0-3: epoch milliseconds
 * - Precision 4-6: epoch microseconds
 * <p>
 * Precision 7-12 (long timestamps) are stored as 12 bytes (epoch microseconds + picoseconds of microsecond)
 * using {@link com.facebook.presto.common.block.Fixed12Block}.
 * <p>
 * Backward compatibility:
 * - {@link #TIMESTAMP} is an alias for TIMESTAMP(3) (millisecond precision)
 * - {@link #TIMESTAMP_MICROSECONDS} is an alias for TIMESTAMP(6) (microsecond precision)
 */
public abstract class TimestampType
        extends AbstractType
        implements FixedWidthType
{
    // Pre-defined instances for common precisions
    public static final TimestampType TIMESTAMP_SECONDS = createTimestampType(0);
    public static final TimestampType TIMESTAMP_MILLIS = createTimestampType(3);
    public static final TimestampType TIMESTAMP_MICROS = createTimestampType(6);
    public static final TimestampType TIMESTAMP_NANOS = createTimestampType(9);
    public static final TimestampType TIMESTAMP_PICOS = createTimestampType(12);

    // Backward-compatible aliases
    public static final TimestampType TIMESTAMP = TIMESTAMP_MILLIS;
    public static final TimestampType TIMESTAMP_MICROSECONDS = TIMESTAMP_MICROS;

    private final int precision;

    TimestampType(int precision, Class<?> javaType)
    {
        super(javaType);
        if (precision < 0 || precision > MAX_PRECISION) {
            throw new IllegalArgumentException(format("TIMESTAMP precision must be in range [0, %d]: %s", MAX_PRECISION, precision));
        }
        this.precision = precision;
    }

    /**
     * Creates a TimestampType with the given precision (0..12).
     */
    public static TimestampType createTimestampType(int precision)
    {
        if (precision < 0 || precision > MAX_PRECISION) {
            throw new IllegalArgumentException(format("TIMESTAMP precision must be in range [0, %d]: %s", MAX_PRECISION, precision));
        }
        if (precision <= MAX_SHORT_PRECISION) {
            return new ShortTimestampType(precision);
        }
        return new LongTimestampType(precision);
    }

    /**
     * Returns the precision (0..12) of this timestamp type.
     */
    public int getPrecision()
    {
        return precision;
    }

    /**
     * Returns true if this is a short timestamp (precision 0..6), which fits in a single long.
     */
    public boolean isShort()
    {
        return precision <= MAX_SHORT_PRECISION;
    }

    /**
     * Returns true if this is a long timestamp (precision 7..12).
     */
    public boolean isLong()
    {
        return precision > MAX_SHORT_PRECISION;
    }

    /**
     * Returns the storage unit for short timestamps.
     * For precision 0-3, returns MILLISECONDS.
     * For precision 4-6, returns MICROSECONDS.
     */
    public TimeUnit getStorageUnit()
    {
        if (precision <= 3) {
            return TimeUnit.MILLISECONDS;
        }
        if (precision <= MAX_SHORT_PRECISION) {
            return TimeUnit.MICROSECONDS;
        }
        throw new UnsupportedOperationException("Long timestamps do not have a simple TimeUnit storage");
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
    public abstract Object getObjectValue(SqlFunctionProperties properties, Block block, int position);

    @Override
    public TypeSignature getTypeSignature()
    {
        if (precision == 3) {
            return TypeSignature.parseTypeSignature(StandardTypes.TIMESTAMP);
        }
        if (precision == 6) {
            return TypeSignature.parseTypeSignature(StandardTypes.TIMESTAMP_MICROSECONDS);
        }
        return new TypeSignature(StandardTypes.TIMESTAMP, TypeSignatureParameter.of(precision));
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (!(other instanceof TimestampType)) {
            return false;
        }
        return this.precision == ((TimestampType) other).precision;
    }

    @Override
    public int hashCode()
    {
        return precision;
    }

    // Legacy compatibility methods

    /**
     * Gets the timestamp's total seconds from epoch.
     * Only valid for short timestamps (precision 0-6).
     */
    public long getEpochSecond(long timestamp)
    {
        return getStorageUnit().toSeconds(timestamp);
    }

    /**
     * Gets the timestamp's nanosecond portion within the current second.
     * Only valid for short timestamps (precision 0-6).
     */
    public int getNanos(long timestamp)
    {
        TimeUnit unit = getStorageUnit();
        long unitsPerSecond = unit.convert(1, TimeUnit.SECONDS);
        return (int) unit.toNanos(timestamp % unitsPerSecond);
    }
}
