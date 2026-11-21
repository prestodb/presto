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
package com.facebook.presto.ducklake.reader;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.DateTimeEncoding;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.UuidType;
import com.facebook.presto.ducklake.DuckLakeColumnHandle;
import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.JsonType.JSON;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.ducklake.DuckLakeColumnHandle.primitiveColumnHandle;
import static com.facebook.presto.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_FEATURE;
import static org.testng.Assert.assertEquals;

public class TestDefaultValueParser
{
    @Test
    public void testBoolean()
    {
        assertEquals(DefaultValueParser.parse(column("boolean", BOOLEAN), "true"), Boolean.TRUE);
    }

    @Test
    public void testInteger()
    {
        assertEquals(DefaultValueParser.parse(column("int32", INTEGER), "42"), 42L);
    }

    @Test
    public void testReal()
    {
        assertEquals(DefaultValueParser.parse(column("float32", REAL), "1.5"), (long) Float.floatToIntBits(1.5f));
    }

    @Test
    public void testDouble()
    {
        assertEquals(DefaultValueParser.parse(column("float64", DOUBLE), "1.5"), 1.5);
    }

    @Test
    public void testDecimal()
    {
        Object value = DefaultValueParser.parse(column("decimal", createDecimalType(10, 2)), "12.34");
        assertEquals(value, new BigDecimal("12.34").unscaledValue().longValueExact());
    }

    @Test
    public void testDate()
    {
        assertEquals(DefaultValueParser.parse(column("date", DATE), "2024-06-15"), LocalDate.parse("2024-06-15").toEpochDay());
    }

    @Test
    public void testTimestamp()
    {
        Object value = DefaultValueParser.parse(column("timestamp", TIMESTAMP), "2024-06-15 13:45:30.123456");
        assertEquals(value, LocalDateTime.parse("2024-06-15T13:45:30.123456").toInstant(ZoneOffset.UTC).toEpochMilli());
    }

    @Test
    public void testTimestampWithTimeZone()
    {
        Object value = DefaultValueParser.parse(column("timestamptz", TIMESTAMP_WITH_TIME_ZONE), "2024-06-15 13:45:30+00");
        long expectedMillis = LocalDateTime.parse("2024-06-15T13:45:30").toInstant(ZoneOffset.UTC).toEpochMilli();
        assertEquals(value, DateTimeEncoding.packDateTimeWithZone(expectedMillis, TimeZoneKey.UTC_KEY));
    }

    @Test
    public void testVarchar()
    {
        assertEquals(DefaultValueParser.parse(column("varchar", VARCHAR), "hello"), Slices.utf8Slice("hello"));
    }

    @Test
    public void testTimeWithSixDigitFraction()
    {
        // 13:45:30 is 49530 seconds; the ".123456" fraction contributes 123456000 nanoseconds,
        // which truncates to 123 milliseconds.
        long expectedMillisOfDay = 49530L * 1000 + 123;
        assertEquals(DefaultValueParser.parse(column("time", TIME), "13:45:30.123456"), expectedMillisOfDay);
    }

    @Test
    public void testUuid()
    {
        String text = "12345678-1234-1234-1234-123456789abc";
        Slice expected = UuidType.javaUuidToPrestoUuid(UUID.fromString(text));
        assertEquals(DefaultValueParser.parse(column("uuid", UuidType.UUID), text), expected);
    }

    @Test
    public void testJson()
    {
        assertEquals(DefaultValueParser.parse(column("json", JSON), "{\"a\":1}"), Slices.utf8Slice("{\"a\":1}"));
    }

    @Test
    public void testVarbinaryWithEscapeAndPrintableByte()
    {
        // "A" (printable) followed by "\x00" (escape) followed by "B" (printable).
        Object value = DefaultValueParser.parse(column("blob", VARBINARY), "A\\x00B");
        assertEquals(value, Slices.wrappedBuffer(new byte[] {'A', 0x00, 'B'}));
    }

    @Test
    public void testNestedTypeThrows()
    {
        assertThrowsUnsupported(() -> DefaultValueParser.parse(column("list", new ArrayType(INTEGER)), "[1, 2, 3]"));
    }

    @Test
    public void testMalformedIntegerThrows()
    {
        assertThrowsUnsupported(() -> DefaultValueParser.parse(column("int32", INTEGER), "not-a-number"));
    }

    @Test
    public void testMalformedBlobEscapeThrows()
    {
        assertThrowsUnsupported(() -> DefaultValueParser.parse(column("blob", VARBINARY), "A\\xZZ"));
    }

    private static DuckLakeColumnHandle column(String duckLakeType, Type type)
    {
        return primitiveColumnHandle(1, "c", duckLakeType, type);
    }

    private static void assertThrowsUnsupported(Runnable runnable)
    {
        try {
            runnable.run();
            throw new AssertionError("expected PrestoException");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), DUCKLAKE_UNSUPPORTED_FEATURE.toErrorCode());
        }
    }
}
