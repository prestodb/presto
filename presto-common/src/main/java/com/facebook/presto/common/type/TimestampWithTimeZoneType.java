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
 * TIMESTAMP(p) WITH TIME ZONE, the public factory and common base for both storage representations,
 * split the same way as {@link TimestampType}:
 * <ul>
 *   <li>{@link ShortTimestampWithTimeZoneType} for p &lt;= {@link #MAX_SHORT_PRECISION}: epoch-millis and
 *       the time zone key packed into a single {@code long} by {@link DateTimeEncoding},
 *       Java type {@code long.class}.</li>
 *   <li>{@link LongTimestampWithTimeZoneType} for p &gt; {@link #MAX_SHORT_PRECISION}: that packed
 *       {@code long} plus {@code picosOfMilli} in a {@code Fixed12ArrayBlock},
 *       Java type {@link LongTimestampWithTimeZone}.</li>
 * </ul>
 *
 * <p>SQL grammar, operator registration, and connector I/O for p != 3 are tracked in
 * <a href="https://github.com/prestodb/presto/issues/27934">#27934</a>.
 */
public abstract class TimestampWithTimeZoneType
        extends AbstractPrimitiveType
        implements FixedWidthType
{
    public static final int MAX_PRECISION = 12;
    // The packed long carries epoch-millis, so no finer precision fits in it.
    public static final int MAX_SHORT_PRECISION = 3;
    public static final int DEFAULT_PRECISION = 3;

    private static final TimestampWithTimeZoneType[] INSTANCES = new TimestampWithTimeZoneType[MAX_PRECISION + 1];

    static {
        for (int precision = 0; precision <= MAX_SHORT_PRECISION; precision++) {
            INSTANCES[precision] = new ShortTimestampWithTimeZoneType(precision);
        }
        for (int precision = MAX_SHORT_PRECISION + 1; precision <= MAX_PRECISION; precision++) {
            INSTANCES[precision] = new LongTimestampWithTimeZoneType(precision);
        }
    }

    public static final TimestampWithTimeZoneType TIMESTAMP_WITH_TIME_ZONE = INSTANCES[DEFAULT_PRECISION];

    private final int precision;

    TimestampWithTimeZoneType(int precision, Class<?> javaType)
    {
        super(buildTypeSignature(precision), javaType);
        this.precision = precision;
    }

    // Only p=3 is registered in the type manager; other precisions tracked in #27934.
    public static TimestampWithTimeZoneType createTimestampWithTimeZoneType(int precision)
    {
        if (precision < 0 || precision > MAX_PRECISION) {
            throw new IllegalArgumentException(format(
                    "TIMESTAMP WITH TIME ZONE precision must be in range [0, %d]: %d", MAX_PRECISION, precision));
        }
        return INSTANCES[precision];
    }

    private static TypeSignature buildTypeSignature(int precision)
    {
        if (precision == DEFAULT_PRECISION) {
            // Preserve "timestamp with time zone" (no parameter) so existing serialized metadata continues to parse.
            return parseTypeSignature(StandardTypes.TIMESTAMP_WITH_TIME_ZONE);
        }
        // TODO(#27934 Phase 2): Register TimestampWithTimeZoneParametricType for type-registry round-trip.
        return new TypeSignature(StandardTypes.TIMESTAMP_WITH_TIME_ZONE, TypeSignatureParameter.of((long) precision));
    }

    public final int getPrecision()
    {
        return precision;
    }

    public final boolean isShort()
    {
        return precision <= MAX_SHORT_PRECISION;
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

    /**
     * Timestamp with time zone represents a single point in time.  Multiple timestamps with timezones may
     * each refer to the same point in time.  For example, 9:00am in New York is the same point in time as
     * 2:00pm in London.  While those two timestamps may be encoded differently, they each refer to the same
     * point in time.  Therefore, it's possible encode multiple timestamps which each represent the same
     * point in time, and hence it's not safe to use equality as a proxy for identity.
     */
    @Override
    public final boolean equalValuesAreIdentical()
    {
        return false;
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
}
