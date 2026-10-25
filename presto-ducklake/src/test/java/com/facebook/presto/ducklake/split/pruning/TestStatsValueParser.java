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
package com.facebook.presto.ducklake.split.pruning;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateTimeEncoding;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.UuidType;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestStatsValueParser
{
    @Test
    public void testIntegers()
    {
        assertEquals(StatsValueParser.parseMin("int8", TinyintType.TINYINT, "1"), Optional.of(1L));
        assertEquals(StatsValueParser.parseMax("int16", SmallintType.SMALLINT, "60000"), Optional.of(60000L));
        assertEquals(StatsValueParser.parseMin("int32", IntegerType.INTEGER, "  1  "), Optional.of(1L));
        assertEquals(StatsValueParser.parseMax("int64", BigintType.BIGINT, "60000"), Optional.of(60000L));
    }

    @Test
    public void testIntegerOverflowIsEmpty()
    {
        assertFalse(StatsValueParser.parseMin("int8", TinyintType.TINYINT, "999999999999999999999").isPresent());
    }

    @Test
    public void testBoolean()
    {
        assertEquals(StatsValueParser.parseMin("boolean", BooleanType.BOOLEAN, "1"), Optional.of(Boolean.TRUE));
        assertEquals(StatsValueParser.parseMin("boolean", BooleanType.BOOLEAN, "0"), Optional.of(Boolean.FALSE));
        assertEquals(StatsValueParser.parseMin("boolean", BooleanType.BOOLEAN, "true"), Optional.of(Boolean.TRUE));
        assertEquals(StatsValueParser.parseMin("boolean", BooleanType.BOOLEAN, "FALSE"), Optional.of(Boolean.FALSE));
        assertFalse(StatsValueParser.parseMin("boolean", BooleanType.BOOLEAN, "not-a-bool").isPresent());
    }

    @Test
    public void testReal()
    {
        Optional<Object> parsed = StatsValueParser.parseMin("float32", RealType.REAL, "3.14");
        assertEquals(parsed, Optional.of((long) Float.floatToIntBits(3.14f)));
    }

    @Test
    public void testDouble()
    {
        assertEquals(StatsValueParser.parseMin("float64", DoubleType.DOUBLE, "-994.79"), Optional.of(-994.79d));
        assertEquals(StatsValueParser.parseMax("float64", DoubleType.DOUBLE, "2.718281828"), Optional.of(2.718281828d));
    }

    @Test
    public void testRealAndDoubleInfinityAndNanAreEmpty()
    {
        assertFalse(StatsValueParser.parseMin("float32", RealType.REAL, "inf").isPresent());
        assertFalse(StatsValueParser.parseMax("float32", RealType.REAL, "-inf").isPresent());
        assertFalse(StatsValueParser.parseMin("float32", RealType.REAL, "nan").isPresent());
        assertFalse(StatsValueParser.parseMin("float64", DoubleType.DOUBLE, "Inf").isPresent());
        assertFalse(StatsValueParser.parseMax("float64", DoubleType.DOUBLE, "-INF").isPresent());
        assertFalse(StatsValueParser.parseMin("float64", DoubleType.DOUBLE, "NaN").isPresent());
    }

    @Test
    public void testShortDecimal()
    {
        DecimalType type = DecimalType.createDecimalType(18, 3);
        assertEquals(StatsValueParser.parseMin("decimal(18,3)", type, "12345.678"), Optional.of(12345678L));
        assertEquals(StatsValueParser.parseMax("decimal(18,3)", type, "12345.678"), Optional.of(12345678L));
    }

    @Test
    public void testLongDecimal()
    {
        DecimalType type = DecimalType.createDecimalType(30, 3);
        Optional<Object> parsed = StatsValueParser.parseMin("decimal(30,3)", type, "12345.678");
        assertEquals(parsed, Optional.of(Decimals.encodeScaledValue(new BigDecimal("12345.678"), 3)));
    }

    @Test
    public void testVarchar()
    {
        VarcharType type = VarcharType.createUnboundedVarcharType();
        assertEquals(StatsValueParser.parseMin("varchar", type, "hello"), Optional.of(Slices.utf8Slice("hello")));
        assertEquals(StatsValueParser.parseMax("varchar", type, "hello"), Optional.of(Slices.utf8Slice("hellp")));
    }

    @Test
    public void testVarcharMaxSkipsSurrogateRange()
    {
        // U+D7FF is the last code point below the UTF-16 surrogate range; incrementing it must
        // jump over the surrogates to U+E000 rather than produce a lone, unencodable surrogate.
        VarcharType type = VarcharType.createUnboundedVarcharType();
        String value = "a퟿";
        String expected = "a" + new String(Character.toChars(0xE000));

        assertEquals(StatsValueParser.parseMax("varchar", type, value), Optional.of(Slices.utf8Slice(expected)));
    }

    @Test
    public void testDate()
    {
        DateType type = DateType.DATE;
        assertEquals(StatsValueParser.parseMin("date", type, "2024-06-15"), Optional.of(LocalDate.of(2024, 6, 15).toEpochDay()));
    }

    @Test
    public void testDateInfinityIsEmpty()
    {
        assertFalse(StatsValueParser.parseMin("date", DateType.DATE, "-infinity").isPresent());
        assertFalse(StatsValueParser.parseMax("date", DateType.DATE, "infinity").isPresent());
    }

    @Test
    public void testTimestampWithNoFraction()
    {
        long expected = toEpochMillis(LocalDateTime.of(2024, 6, 15, 13, 45, 30));
        assertEquals(StatsValueParser.parseMin("timestamp", TimestampType.TIMESTAMP, "2024-06-15 13:45:30"), Optional.of(expected));
        assertEquals(StatsValueParser.parseMax("timestamp", TimestampType.TIMESTAMP, "2024-06-15 13:45:30"), Optional.of(expected));
    }

    @Test
    public void testTimestampWithMillisecondFraction()
    {
        long expected = toEpochMillis(LocalDateTime.of(2024, 6, 15, 13, 45, 30)) + 123;
        assertEquals(StatsValueParser.parseMin("timestamp_ms", TimestampType.TIMESTAMP, "2024-06-15 13:45:30.123"), Optional.of(expected));
        assertEquals(StatsValueParser.parseMax("timestamp_ms", TimestampType.TIMESTAMP, "2024-06-15 13:45:30.123"), Optional.of(expected));
    }

    @Test
    public void testTimestampWithMicrosecondFractionFloorsMinAndCeilsMax()
    {
        long base = toEpochMillis(LocalDateTime.of(2024, 6, 15, 13, 45, 30));
        assertEquals(StatsValueParser.parseMin("timestamp", TimestampType.TIMESTAMP, "2024-06-15 13:45:30.123456"), Optional.of(base + 123));
        assertEquals(StatsValueParser.parseMax("timestamp", TimestampType.TIMESTAMP, "2024-06-15 13:45:30.123456"), Optional.of(base + 124));
    }

    @Test
    public void testTimestampWithNanosecondFractionFloorsMinAndCeilsMax()
    {
        long base = toEpochMillis(LocalDateTime.of(2024, 6, 15, 13, 45, 30));
        assertEquals(StatsValueParser.parseMin("timestamp_ns", TimestampType.TIMESTAMP, "2024-06-15 13:45:30.123456789"), Optional.of(base + 123));
        assertEquals(StatsValueParser.parseMax("timestamp_ns", TimestampType.TIMESTAMP, "2024-06-15 13:45:30.123456789"), Optional.of(base + 124));
    }

    @Test
    public void testTimestampInfinityIsEmpty()
    {
        assertFalse(StatsValueParser.parseMin("timestamp", TimestampType.TIMESTAMP, "-infinity").isPresent());
        assertFalse(StatsValueParser.parseMax("timestamp", TimestampType.TIMESTAMP, "infinity").isPresent());
    }

    @Test
    public void testTimestampWithTimeZone()
    {
        long millisUtc = toEpochMillis(LocalDateTime.of(2024, 6, 15, 13, 45, 30));
        long expectedMin = DateTimeEncoding.packDateTimeWithZone(millisUtc + 123, TimeZoneKey.UTC_KEY);
        long expectedMax = DateTimeEncoding.packDateTimeWithZone(millisUtc + 124, TimeZoneKey.UTC_KEY);

        assertEquals(
                StatsValueParser.parseMin("timestamptz", TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE, "2024-06-15 13:45:30.123456+00"),
                Optional.of(expectedMin));
        assertEquals(
                StatsValueParser.parseMax("timestamptz", TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE, "2024-06-15 13:45:30.123456+00"),
                Optional.of(expectedMax));
    }

    @Test
    public void testGarbageStringsAreEmptyForEveryType()
    {
        String garbage = "not-a-valid-value";
        assertFalse(StatsValueParser.parseMin("int32", IntegerType.INTEGER, garbage).isPresent());
        assertFalse(StatsValueParser.parseMin("boolean", BooleanType.BOOLEAN, garbage).isPresent());
        assertFalse(StatsValueParser.parseMin("float32", RealType.REAL, garbage).isPresent());
        assertFalse(StatsValueParser.parseMin("float64", DoubleType.DOUBLE, garbage).isPresent());
        assertFalse(StatsValueParser.parseMin("decimal(18,3)", DecimalType.createDecimalType(18, 3), garbage).isPresent());
        assertFalse(StatsValueParser.parseMin("date", DateType.DATE, garbage).isPresent());
        assertFalse(StatsValueParser.parseMin("timestamp", TimestampType.TIMESTAMP, garbage).isPresent());
        assertFalse(StatsValueParser.parseMin("timestamptz", TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE, garbage).isPresent());
    }

    @Test
    public void testUnsupportedTypesAreAlwaysEmpty()
    {
        assertFalse(StatsValueParser.parseMin("time", TimeType.TIME, "13:45:30").isPresent());
        assertFalse(StatsValueParser.parseMin("uuid", UuidType.UUID, "6c40c04c-1234-4f5a-8b0e-000000000000").isPresent());
        assertFalse(StatsValueParser.parseMin("blob", VarbinaryType.VARBINARY, "48656C6C6F").isPresent());
        assertFalse(StatsValueParser.parseMin("array", new ArrayType(IntegerType.INTEGER), "[1,2,3]").isPresent());
    }

    private static long toEpochMillis(LocalDateTime value)
    {
        return value.toInstant(ZoneOffset.UTC).toEpochMilli();
    }
}
