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
package com.facebook.presto.type;

import com.facebook.presto.common.type.LongTimestampType;
import com.facebook.presto.common.type.ShortTimestampType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeParameter;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROS;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MILLIS;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_NANOS;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_PICOS;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestTimestampParametricType
{
    private final TimestampParametricType parametricType = new TimestampParametricType();

    @Test
    public void testGetName()
    {
        assertEquals(parametricType.getName(), "timestamp");
    }

    @Test
    public void testDefaultPrecision()
    {
        Type type = parametricType.createType(Collections.emptyList());
        assertTrue(type instanceof TimestampType);
        assertEquals(((TimestampType) type).getPrecision(), 3);
    }

    @Test
    public void testPrecision0()
    {
        Type type = parametricType.createType(List.of(TypeParameter.of(0L)));
        assertTrue(type instanceof ShortTimestampType);
        assertEquals(((TimestampType) type).getPrecision(), 0);
    }

    @Test
    public void testPrecision3()
    {
        Type type = parametricType.createType(List.of(TypeParameter.of(3L)));
        assertTrue(type instanceof ShortTimestampType);
        assertEquals(((TimestampType) type).getPrecision(), 3);
    }

    @Test
    public void testPrecision6()
    {
        Type type = parametricType.createType(List.of(TypeParameter.of(6L)));
        assertTrue(type instanceof ShortTimestampType);
        assertEquals(((TimestampType) type).getPrecision(), 6);
    }

    @Test
    public void testPrecision7()
    {
        Type type = parametricType.createType(List.of(TypeParameter.of(7L)));
        assertTrue(type instanceof LongTimestampType);
        assertEquals(((TimestampType) type).getPrecision(), 7);
    }

    @Test
    public void testPrecision9()
    {
        Type type = parametricType.createType(List.of(TypeParameter.of(9L)));
        assertTrue(type instanceof LongTimestampType);
        assertEquals(((TimestampType) type).getPrecision(), 9);
    }

    @Test
    public void testPrecision12()
    {
        Type type = parametricType.createType(List.of(TypeParameter.of(12L)));
        assertTrue(type instanceof LongTimestampType);
        assertEquals(((TimestampType) type).getPrecision(), 12);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testPrecisionNegative()
    {
        parametricType.createType(List.of(TypeParameter.of(-1L)));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testPrecisionTooHigh()
    {
        parametricType.createType(List.of(TypeParameter.of(13L)));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTooManyParameters()
    {
        parametricType.createType(List.of(TypeParameter.of(3L), TypeParameter.of(6L)));
    }

    @Test
    public void testPreDefinedConstants()
    {
        assertEquals(TIMESTAMP_SECONDS.getPrecision(), 0);
        assertEquals(TIMESTAMP_MILLIS.getPrecision(), 3);
        assertEquals(TIMESTAMP_MICROS.getPrecision(), 6);
        assertEquals(TIMESTAMP_NANOS.getPrecision(), 9);
        assertEquals(TIMESTAMP_PICOS.getPrecision(), 12);

        assertTrue(TIMESTAMP_SECONDS instanceof ShortTimestampType);
        assertTrue(TIMESTAMP_MILLIS instanceof ShortTimestampType);
        assertTrue(TIMESTAMP_MICROS instanceof ShortTimestampType);
        assertTrue(TIMESTAMP_NANOS instanceof LongTimestampType);
        assertTrue(TIMESTAMP_PICOS instanceof LongTimestampType);
    }

    @Test
    public void testBackwardCompatibleAliases()
    {
        assertSame(TIMESTAMP, TIMESTAMP_MILLIS);
        assertSame(TIMESTAMP_MICROSECONDS, TIMESTAMP_MICROS);
    }

    @Test
    public void testTimestampTypeEquality()
    {
        TimestampType ts3a = TimestampType.createTimestampType(3);
        TimestampType ts3b = TimestampType.createTimestampType(3);
        assertEquals(ts3a, ts3b);
        assertEquals(ts3a.hashCode(), ts3b.hashCode());

        TimestampType ts6 = TimestampType.createTimestampType(6);
        assertFalse(ts3a.equals(ts6));
    }

    @Test
    public void testIsShortIsLong()
    {
        for (int p = 0; p <= 6; p++) {
            TimestampType type = TimestampType.createTimestampType(p);
            assertTrue(type.isShort(), "precision " + p + " should be short");
            assertFalse(type.isLong(), "precision " + p + " should not be long");
        }
        for (int p = 7; p <= 12; p++) {
            TimestampType type = TimestampType.createTimestampType(p);
            assertFalse(type.isShort(), "precision " + p + " should not be short");
            assertTrue(type.isLong(), "precision " + p + " should be long");
        }
    }

    @Test
    public void testIsComparableAndOrderable()
    {
        for (int p = 0; p <= 12; p++) {
            TimestampType type = TimestampType.createTimestampType(p);
            assertTrue(type.isComparable());
            assertTrue(type.isOrderable());
        }
    }

    @Test
    public void testAllPrecisionsCreate()
    {
        for (int p = 0; p <= 12; p++) {
            TimestampType type = TimestampType.createTimestampType(p);
            assertEquals(type.getPrecision(), p);
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testCreateTimestampTypeNegative()
    {
        TimestampType.createTimestampType(-1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testCreateTimestampTypeTooHigh()
    {
        TimestampType.createTimestampType(13);
    }
}
