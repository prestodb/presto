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

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static java.lang.String.format;

/**
 * TIMESTAMP(p), the public factory and common base for both storage representations. Like
 * {@link DecimalType}, the representation is chosen by the subclass rather than by branching
 * inside a single class:
 * <ul>
 *   <li>{@link ShortTimestampType} for p &lt;= {@link #MAX_SHORT_PRECISION}: a single epoch-scaled
 *       {@code long} (e.g. p=3 is epoch-millis, p=6 is epoch-micros), Java type {@code long.class}.</li>
 *   <li>{@link LongTimestampType} for p &gt; {@link #MAX_SHORT_PRECISION}: an
 *       {@code (epochMicros, picosOfMicro)} pair in a {@code Fixed12ArrayBlock},
 *       Java type {@link LongTimestamp}.</li>
 * </ul>
 * Generic code therefore needs no timestamp-specific special case: dispatching on
 * {@link #getJavaType()} lands on {@code getLong}/{@code writeLong} for short precisions and on
 * {@code getObject}/{@code writeObject} for long precisions.
 *
 * <p>SQL grammar, operator registration, and connector I/O for p=7–12 are tracked in
 * <a href="https://github.com/prestodb/presto/issues/27934">#27934</a>.
 */
public abstract class TimestampType
        extends AbstractPrimitiveType
        implements FixedWidthType
{
    public static final int MAX_PRECISION = 12;
    public static final int MAX_SHORT_PRECISION = 6;
    public static final int DEFAULT_PRECISION = 3;

    private static final TimestampType[] INSTANCES = new TimestampType[MAX_PRECISION + 1];

    static {
        for (int precision = 0; precision <= MAX_SHORT_PRECISION; precision++) {
            INSTANCES[precision] = new ShortTimestampType(precision);
        }
        for (int precision = MAX_SHORT_PRECISION + 1; precision <= MAX_PRECISION; precision++) {
            INSTANCES[precision] = new LongTimestampType(precision);
        }
    }

    public static final TimestampType TIMESTAMP = INSTANCES[DEFAULT_PRECISION];

    // Keeps the legacy "timestamp microseconds" type signature so existing code that matches
    // on type-signature base strings continues to work without changes.
    public static final TimestampType TIMESTAMP_MICROSECONDS = INSTANCES[MAX_SHORT_PRECISION];

    private final int precision;

    TimestampType(int precision, Class<?> javaType)
    {
        super(buildTypeSignature(precision), javaType);
        this.precision = precision;
    }

    // Only p=3 and p=6 are registered in the type manager; other precisions tracked in #27934.
    public static TimestampType createTimestampType(int precision)
    {
        if (precision < 0 || precision > MAX_PRECISION) {
            throw new IllegalArgumentException(format(
                    "TIMESTAMP precision must be in range [0, %d]: %d", MAX_PRECISION, precision));
        }
        return INSTANCES[precision];
    }

    private static TypeSignature buildTypeSignature(int precision)
    {
        if (precision == DEFAULT_PRECISION) {
            // Preserve "timestamp" (no parameter) so existing serialized metadata continues to parse.
            return parseTypeSignature(StandardTypes.TIMESTAMP);
        }
        if (precision == MAX_SHORT_PRECISION) {
            // Preserve "timestamp microseconds" for the same reason.
            return parseTypeSignature(StandardTypes.TIMESTAMP_MICROSECONDS);
        }
        // TODO(#27934 Phase 2): Register TimestampParametricType for type-registry round-trip.
        return new TypeSignature(StandardTypes.TIMESTAMP, TypeSignatureParameter.of((long) precision));
    }

    public final int getPrecision()
    {
        return precision;
    }

    public final boolean isShort()
    {
        return precision <= MAX_SHORT_PRECISION;
    }

    // Used by Iceberg partition value conversion.
    public final boolean isMillisPrecision()
    {
        return precision == DEFAULT_PRECISION;
    }

    @Override
    public final boolean isComparable()
    {
        return true;
    }

    @Override
    public final boolean isOrderable()
    {
        return true;
    }

    // Instances are interned, so reference equality is correct; overridden only for checkstyle's EqualsHashCode rule.
    @Override
    public final boolean equals(Object other)
    {
        return this == other;
    }

    @Override
    public final int hashCode()
    {
        return System.identityHashCode(this);
    }

    /**
     * Number of whole seconds since 1970-01-01T00:00:00 UTC in a short-precision value.
     * Long precisions carry their fractional part outside the {@code long} and throw.
     */
    // TODO(#27934 Phase 3): Add getEpochSecond(LongTimestamp) for date_trunc/date_add/AT TIME ZONE.
    public abstract long getEpochSecond(long timestamp);

    /**
     * Nanosecond-of-second of a short-precision value. Long precisions throw.
     */
    // TODO(#27934 Phase 3): Add getNanos(LongTimestamp) for date_format/date_trunc.
    public abstract int getNanos(long timestamp);

    // TODO(#27934 Phase 4): Add toEpochMillis(LongTimestamp) for Iceberg/ORC/Parquet/JDBC.
    public abstract long toEpochMillis(long timestamp);

    // TODO(#27934 Phase 4): Add toEpochMicros(LongTimestamp) for ORC/Parquet microsecond writes.
    public abstract long toEpochMicros(long timestamp);

    // TODO(#27934 Phase 4): Add fromEpochComponents(epochSecond, nanos) -> LongTimestamp for Parquet/ORC nanosecond reads.
    public abstract long fromEpochComponents(long epochSecond, int nanos);
}
