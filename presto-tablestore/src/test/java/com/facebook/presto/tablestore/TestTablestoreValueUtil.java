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
package com.facebook.presto.tablestore;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.facebook.presto.common.predicate.Marker;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static com.alicloud.openservices.tablestore.model.PrimaryKeyValue.INF_MAX;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.tablestore.TablestoreValueUtil.addOneForPrimaryKeyValue;
import static com.facebook.presto.tablestore.TablestoreValueUtil.convert;
import static com.facebook.presto.tablestore.TablestoreValueUtil.toColumnValue;
import static com.facebook.presto.tablestore.TablestoreValueUtil.toPrimaryKeyValue;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

public class TestTablestoreValueUtil
{
    @Test
    public void testAddOne()
    {
        PrimaryKeyValue pk = INF_MAX;
        PrimaryKeyValue x = addOneForPrimaryKeyValue(pk);
        assertEquals(INF_MAX, x);

        pk = PrimaryKeyValue.fromLong(123);
        x = addOneForPrimaryKeyValue(pk);
        assertEquals(124L, x.asLong());

        pk = PrimaryKeyValue.fromLong(Long.MAX_VALUE);
        x = addOneForPrimaryKeyValue(pk);
        assertEquals(INF_MAX, x);

        pk = PrimaryKeyValue.fromString("asdf");
        x = addOneForPrimaryKeyValue(pk);
        assertEquals("asdf\0", x.asString());

        pk = PrimaryKeyValue.fromBinary("xxxx".getBytes());
        x = addOneForPrimaryKeyValue(pk);
        assertArrayEquals("xxxx\0".getBytes(), x.asBinary());
    }

    @Test
    public void testToColumnValue()
    {
        Marker marker = Marker.exactly(INTEGER, 123L);
        ColumnValue x = toColumnValue(marker);
        assertEquals(123L, x.asLong());

        marker = Marker.exactly(BIGINT, 123L);
        x = toColumnValue(marker);
        assertEquals(123L, x.asLong());

        marker = Marker.exactly(VARCHAR, Slices.utf8Slice("123"));
        x = toColumnValue(marker);
        assertEquals("123", x.asString());

        marker = Marker.exactly(CharType.createCharType(3), Slices.utf8Slice("123"));
        x = toColumnValue(marker);
        assertEquals("123", x.asString());

        marker = Marker.exactly(DOUBLE, 11.11);
        x = toColumnValue(marker);
        assertEquals(11.11, x.asDouble(), 0.0000005);

        marker = Marker.exactly(BOOLEAN, true);
        x = toColumnValue(marker);
        assertTrue(x.asBoolean());

        marker = Marker.exactly(VARBINARY, Slices.wrappedBuffer("133".getBytes()));
        x = toColumnValue(marker);
        assertArrayEquals("133".getBytes(), x.asBinary());
    }

    @Test
    public void testToPrimaryKeyValue()
    {
        Marker marker = Marker.exactly(INTEGER, 123L);
        PrimaryKeyValue x = toPrimaryKeyValue(marker);
        assertEquals(123L, x.asLong());

        marker = Marker.exactly(BIGINT, 123L);
        x = toPrimaryKeyValue(marker);
        assertEquals(123L, x.asLong());

        marker = Marker.exactly(VARCHAR, Slices.utf8Slice("123"));
        x = toPrimaryKeyValue(marker);
        assertEquals("123", x.asString());

        marker = Marker.exactly(CharType.createCharType(3), Slices.utf8Slice("123"));
        x = toPrimaryKeyValue(marker);
        assertEquals("123", x.asString());
    }

    @Test
    public void testConvertUnsupported()
    {
        try {
            convert(VARCHAR, true, null);
            fail();
        }
        catch (Exception e) {
            assertEquals("Unsupported conversion for PrimaryKeyValue, the required tablestore type is varchar, but the value is NULL and accepted java types is [class java.lang.String, class io.airlift.slice.Slice, class [B, class java.lang.Number, class java.lang.Boolean, class java.lang.Character]", e.getMessage());
        }

        try {
            convert(VARCHAR, false, new Object());
            fail();
        }
        catch (Exception e) {
            assertEquals("Unsupported conversion for ColumnValue, the required tablestore type is varchar, but the type of value is class java.lang.Object and accepted java types is [class java.lang.String, class io.airlift.slice.Slice, class [B, class java.lang.Number, class java.lang.Boolean, class java.lang.Character]", e.getMessage());
        }

        try {
            convert(HYPER_LOG_LOG, false, "");
            fail();
        }
        catch (Exception e) {
            assertEquals("Unsupported conversion for ColumnValue, the required tablestore type isn't supported not:HyperLogLog", e.getMessage());
        }

        try {
            convert(INTEGER, false, "asdasdf");
            fail();
        }
        catch (Exception e) {
            assertEquals("convert() failed for ColumnValue, required type is integer, and type of value is class java.lang.String, and value=asdasdf", e.getMessage());
        }

        try {
            convert(INTEGER, false, "01234567890123456789#################");
            fail();
        }
        catch (Exception e) {
            assertEquals("convert() failed for ColumnValue, required type is integer, and type of value is class java.lang.String, and value=01234567890123456789...", e.getMessage());
        }
    }

    @Test
    public void testConvertString()
    {
        PrimaryKeyValue y = convert(VARCHAR, true, "123");
        assertEquals("123", y.asString());
        y = convert(CharType.createCharType(12), true, 123);
        assertEquals("123", y.asString());
        y = convert(CharType.createCharType(12), true, 123.1);
        assertEquals("123.1", y.asString());
        y = convert(CharType.createCharType(12), true, "123".getBytes());
        assertEquals("123", y.asString());
        y = convert(VARCHAR, true, Slices.utf8Slice("123"));
        assertEquals("123", y.asString());
        y = convert(VARCHAR, true, '1');
        assertEquals("1", y.asString());
        y = convert(VARCHAR, true, true);
        assertEquals("true", y.asString());

        ColumnValue z = convert(VARCHAR, false, "123");
        assertEquals("123", z.asString());
        z = convert(CharType.createCharType(12), false, 123);
        assertEquals("123", z.asString());
        z = convert(CharType.createCharType(12), false, 123.1);
        assertEquals("123.1", z.asString());
        z = convert(CharType.createCharType(12), false, "123".getBytes());
        assertEquals("123", z.asString());
        z = convert(VARCHAR, false, Slices.utf8Slice("123"));
        assertEquals("123", z.asString());
        z = convert(VARCHAR, false, '1');
        assertEquals("1", z.asString());
        z = convert(VARCHAR, false, true);
        assertEquals("true", z.asString());
    }

    @Test
    public void testConvertInteger()
    {
        PrimaryKeyValue y = convert(INTEGER, true, "123");
        assertEquals(123L, y.asLong());
        y = convert(BIGINT, true, 123L);
        assertEquals(123L, y.asLong());
        y = convert(SMALLINT, true, (short) 32767);
        assertEquals(32767L, y.asLong());
        y = convert(TINYINT, true, (byte) 123);
        assertEquals(123L, y.asLong());
        y = convert(TINYINT, true, 123);
        assertEquals(123L, y.asLong());
        y = convert(INTEGER, true, 'c');
        assertEquals((long) 'c', y.asLong());

        ColumnValue z = convert(INTEGER, false, "123");
        assertEquals(123L, z.asLong());
        z = convert(BIGINT, false, 123L);
        assertEquals(123L, z.asLong());
        z = convert(SMALLINT, false, (short) 32767);
        assertEquals(32767L, z.asLong());
        z = convert(TINYINT, false, (byte) 123);
        assertEquals(123L, z.asLong());
        z = convert(TINYINT, false, 123);
        assertEquals(123L, z.asLong());
        z = convert(INTEGER, false, 'c');
        assertEquals((long) 'c', z.asLong());
    }

    @Test
    public void testConvertBoolean()
    {
        ColumnValue z = convert(BOOLEAN, false, "TRue");
        assertTrue(z.asBoolean());
        z = convert(BOOLEAN, false, 123);
        assertTrue(z.asBoolean());
        z = convert(BOOLEAN, false, false);
        assertFalse(z.asBoolean());
        z = convert(BOOLEAN, false, Boolean.FALSE);
        assertFalse(z.asBoolean());

        try {
            convert(BOOLEAN, true, "TRue");
            fail();
        }
        catch (Exception e) {
            assertEquals("Unsupported conversion for PrimaryKeyValue, the required tablestore type is boolean, but the type of value is class java.lang.String and accepted java types is [class java.lang.String, class java.lang.Number, class java.lang.Boolean]", e.getMessage());
        }
    }

    @Test
    public void testConvertDouble()
    {
        ColumnValue z = convert(DOUBLE, false, "123");
        assertEquals(123.0, z.asDouble(), 0.00000001);
        z = convert(DOUBLE, false, 123);
        assertEquals(123.0, z.asDouble(), 0.00000001);
        z = convert(REAL, false, 123.1);
        assertEquals(123.1, z.asDouble(), 0.00000001);

        try {
            convert(DOUBLE, true, "123");
            fail();
        }
        catch (Exception e) {
            assertEquals("Unsupported conversion for PrimaryKeyValue, the required tablestore type is double, but the type of value is class java.lang.String and accepted java types is [class java.lang.String, class java.lang.Number, class java.lang.Double]", e.getMessage());
        }
    }

    @Test
    public void testConvertBinary()
    {
        byte[] bs = "123".getBytes();
        PrimaryKeyValue y = convert(VARBINARY, true, "123");
        assertArrayEquals(bs, y.asBinary());
        y = convert(VARBINARY, true, bs);
        assertArrayEquals(bs, y.asBinary());
        y = convert(VARBINARY, true, Slices.utf8Slice("123"));
        assertArrayEquals(bs, y.asBinary());

        ColumnValue z = convert(VARBINARY, false, "123");
        assertArrayEquals(bs, z.asBinary());
        z = convert(VARBINARY, false, bs);
        assertArrayEquals(bs, z.asBinary());
        z = convert(VARBINARY, false, Slices.utf8Slice("123"));
        assertArrayEquals(bs, z.asBinary());
    }

    @Test
    public void testToPrimaryKeyValueFailed()
    {
        Marker marker = Marker.exactly(VARBINARY, Slices.utf8Slice("123"));
        PrimaryKeyValue pk = toPrimaryKeyValue(marker);
        assertArrayEquals("123".getBytes(), pk.asBinary());

        marker = Marker.exactly(DateType.DATE, 123L);
        try {
            toPrimaryKeyValue(marker);
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("Unsupported"));
        }

        marker = Marker.exactly(TimestampType.TIMESTAMP, 123L);
        try {
            toPrimaryKeyValue(marker);
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("Unsupported"));
        }

        marker = Marker.exactly(TimeType.TIME, 123L);
        try {
            toPrimaryKeyValue(marker);
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("Unsupported"));
        }
    }
}
