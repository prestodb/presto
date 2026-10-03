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
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.common.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.DEFAULT_PRECISION;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.MAX_PRECISION;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.MAX_SHORT_PRECISION;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestTimestampWithTimeZoneType
{
    private static final short LOS_ANGELES = getTimeZoneKey("America/Los_Angeles").getKey();

    @Test
    public void testFactoryInternsEachPrecision()
    {
        for (int precision = 0; precision <= MAX_PRECISION; precision++) {
            TimestampWithTimeZoneType type = createTimestampWithTimeZoneType(precision);
            assertSame(createTimestampWithTimeZoneType(precision), type);
            assertEquals(type.getPrecision(), precision);
            for (int otherPrecision = 0; otherPrecision < precision; otherPrecision++) {
                assertNotSame(createTimestampWithTimeZoneType(otherPrecision), type);
            }
        }
    }

    @Test
    public void testInvalidPrecision()
    {
        expectThrows(IllegalArgumentException.class, () -> createTimestampWithTimeZoneType(-1));
        expectThrows(IllegalArgumentException.class, () -> createTimestampWithTimeZoneType(MAX_PRECISION + 1));
    }

    @Test
    public void testDefaultPrecisionConstant()
    {
        assertSame(TIMESTAMP_WITH_TIME_ZONE, createTimestampWithTimeZoneType(DEFAULT_PRECISION));
        assertEquals(TIMESTAMP_WITH_TIME_ZONE.getTypeSignature().toString(), "timestamp with time zone");
        assertTrue(TIMESTAMP_WITH_TIME_ZONE.getTypeSignature().getParameters().isEmpty());
    }

    @Test
    public void testNonDefaultTypeSignature()
    {
        TypeSignature signature = createTimestampWithTimeZoneType(6).getTypeSignature();
        assertEquals(signature.toString(), "timestamp(6) with time zone");
        assertEquals(parseTypeSignature(signature.toString()), signature);
    }

    @Test
    public void testEqualsAndHashCode()
    {
        assertEquals(createTimestampWithTimeZoneType(DEFAULT_PRECISION), TIMESTAMP_WITH_TIME_ZONE);
        assertEquals(createTimestampWithTimeZoneType(DEFAULT_PRECISION).hashCode(), TIMESTAMP_WITH_TIME_ZONE.hashCode());
        assertNotEquals(createTimestampWithTimeZoneType(6), TIMESTAMP_WITH_TIME_ZONE);
        assertNotEquals(createTimestampWithTimeZoneType(0), TIMESTAMP_WITH_TIME_ZONE);
    }

    @Test
    public void testRepresentationPerPrecision()
    {
        for (int precision = 0; precision <= MAX_PRECISION; precision++) {
            TimestampWithTimeZoneType type = createTimestampWithTimeZoneType(precision);
            if (precision <= MAX_SHORT_PRECISION) {
                assertTrue(type.isShort(), "precision " + precision);
                assertTrue(type instanceof ShortTimestampWithTimeZoneType, "precision " + precision);
                assertEquals(type.getJavaType(), long.class);
                assertEquals(type.getFixedSize(), Long.BYTES);
            }
            else {
                assertFalse(type.isShort(), "precision " + precision);
                assertTrue(type instanceof LongTimestampWithTimeZoneType, "precision " + precision);
                assertEquals(type.getJavaType(), LongTimestampWithTimeZone.class);
                assertEquals(type.getFixedSize(), Long.BYTES + Integer.BYTES);
            }
        }
    }

    @Test
    public void testShortGetObjectValue()
    {
        long packed = packDateTimeWithZone(-1L, getTimeZoneKey(LOS_ANGELES));
        for (int precision = 0; precision <= MAX_SHORT_PRECISION; precision++) {
            TimestampWithTimeZoneType type = createTimestampWithTimeZoneType(precision);
            BlockBuilder builder = type.createBlockBuilder(null, 2);
            type.writeLong(builder, packed);
            builder.appendNull();
            Block block = builder.build();

            assertEquals(type.getObjectValue(null, block, 0), new SqlTimestampWithTimeZone(packed));
            assertNull(type.getObjectValue(null, block, 1));
        }
    }

    @Test
    public void testShortComparesInstantsAcrossZones()
    {
        TimestampWithTimeZoneType type = createTimestampWithTimeZoneType(0);
        BlockBuilder builder = type.createBlockBuilder(null, 3);
        type.writeLong(builder, packDateTimeWithZone(1_000L, UTC_KEY));
        type.writeLong(builder, packDateTimeWithZone(1_000L, getTimeZoneKey(LOS_ANGELES)));
        type.writeLong(builder, packDateTimeWithZone(-1_000L, UTC_KEY));
        Block block = builder.build();

        assertTrue(type.equalTo(block, 0, block, 1));
        assertEquals(type.hash(block, 0), type.hash(block, 1));
        assertEquals(type.compareTo(block, 0, block, 1), 0);
        assertTrue(type.compareTo(block, 2, block, 0) < 0);
    }

    @Test
    public void testLongRoundTrip()
    {
        LongTimestampWithTimeZone value = new LongTimestampWithTimeZone(-1L, 999_999_999, LOS_ANGELES);
        for (int precision = MAX_SHORT_PRECISION + 1; precision <= MAX_PRECISION; precision++) {
            TimestampWithTimeZoneType type = createTimestampWithTimeZoneType(precision);
            BlockBuilder builder = type.createBlockBuilder(null, 2);
            type.writeObject(builder, value);
            builder.appendNull();
            Block block = builder.build();
            assertEquals(type.getObject(block, 0), value);
            assertTrue(block.isNull(1));

            BlockBuilder copyBuilder = type.createBlockBuilder(null, 2);
            type.appendTo(block, 0, copyBuilder);
            type.appendTo(block, 1, copyBuilder);
            Block copy = copyBuilder.build();
            assertEquals(type.getObject(copy, 0), value);
            assertTrue(copy.isNull(1));
        }
    }

    @Test
    public void testLongComparison()
    {
        TimestampWithTimeZoneType type = createTimestampWithTimeZoneType(9);
        BlockBuilder builder = type.createBlockBuilder(null, 4);
        type.writeObject(builder, new LongTimestampWithTimeZone(1_000L, 5_000, UTC_KEY.getKey()));
        type.writeObject(builder, new LongTimestampWithTimeZone(1_000L, 5_000, LOS_ANGELES));
        type.writeObject(builder, new LongTimestampWithTimeZone(1_000L, 6_000, UTC_KEY.getKey()));
        type.writeObject(builder, new LongTimestampWithTimeZone(-1L, 999_999_000, UTC_KEY.getKey()));
        Block block = builder.build();

        // Same instant in different zones.
        assertTrue(type.equalTo(block, 0, block, 1));
        assertEquals(type.hash(block, 0), type.hash(block, 1));
        assertEquals(type.compareTo(block, 0, block, 1), 0);

        // Differs only below the millisecond.
        assertFalse(type.equalTo(block, 0, block, 2));
        assertNotEquals(type.hash(block, 0), type.hash(block, 2));
        assertTrue(type.compareTo(block, 0, block, 2) < 0);

        assertTrue(type.compareTo(block, 3, block, 0) < 0);
    }

    @Test
    public void testLongRejectsShortAccessors()
    {
        TimestampWithTimeZoneType type = createTimestampWithTimeZoneType(9);
        BlockBuilder builder = type.createBlockBuilder(null, 2);
        expectThrows(UnsupportedOperationException.class, () -> type.writeLong(builder, 0L));
        type.writeObject(builder, new LongTimestampWithTimeZone(0L, 0, UTC_KEY.getKey()));
        builder.appendNull();
        Block block = builder.build();

        expectThrows(UnsupportedOperationException.class, () -> type.getLong(block, 0));
        expectThrows(UnsupportedOperationException.class, () -> type.getObjectValue(null, block, 0));
        assertNull(type.getObjectValue(null, block, 1));
    }
}
