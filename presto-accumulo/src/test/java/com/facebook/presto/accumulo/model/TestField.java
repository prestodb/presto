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
package com.facebook.presto.accumulo.model;

import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.GregorianCalendar;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class TestField
{
    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "type is null")
    public void testTypeIsNull()
            throws Exception
    {
        new Field(null, null);
    }

    @Test
    public void testArray()
            throws Exception
    {
        Type type = new ArrayType(VARCHAR);
        Block expected = AccumuloRowSerializer.getBlockFromArray(VARCHAR, ImmutableList.of("a", "b", "c"));
        Field f1 = new Field(expected, type);
        assertEquals(f1.getArray(), expected);
        assertEquals(f1.getObject(), expected);
        assertEquals(f1.getType(), type);
        assertEquals(f1.toString(), "ARRAY ['a','b','c']");

        Field f2 = new Field(f1);
        assertEquals(f2, f1);
    }

    @Test
    public void testBoolean()
            throws Exception
    {
        Type type = BOOLEAN;
        Field f1 = new Field(true, type);
        assertEquals(f1.getBoolean().booleanValue(), true);
        assertEquals(f1.getObject(), true);
        assertEquals(f1.getType(), type);
        assertEquals(f1.toString(), "true");

        f1 = new Field(false, type);
        assertEquals(f1.getBoolean().booleanValue(), false);
        assertEquals(f1.getObject(), false);
        assertEquals(f1.getType(), type);
        assertEquals(f1.toString(), "false");

        Field f2 = new Field(f1);
        assertEquals(f2, f1);
    }

    @Test
    public void testDate()
            throws Exception
    {
        Type type = DATE;
        Date expected = new Date(new GregorianCalendar(1999, 0, 1).getTime().getTime());
        Field f1 = new Field(expected, type);
        assertEquals(f1.getDate(), expected);
        assertEquals(f1.getObject(), expected);
        assertEquals(f1.getType(), type);
        assertEquals(f1.toString(), "DATE '1999-01-01'");

        Field f2 = new Field(f1);
        assertEquals(f2, f1);
    }

    @Test
    public void testDouble()
            throws Exception
    {
        Type type = DOUBLE;
        Double expected = 123.45678;
        Field f1 = new Field(expected, type);
        assertEquals(f1.getDouble(), expected);
        assertEquals(f1.getObject(), expected);
        assertEquals(f1.getType(), type);
        assertEquals(f1.toString(), "123.45678");

        Field f2 = new Field(f1);
        assertEquals(f2, f1);
    }

    @Test
    public void testFloat()
            throws Exception
    {
        Type type = REAL;
        Float expected = 123.45678f;
        Field f1 = new Field(expected, type);
        assertEquals(f1.getFloat(), expected);
        assertEquals(f1.getObject(), expected);
        assertEquals(f1.getType(), type);
        assertEquals(f1.toString(), "123.45678");

        Field f2 = new Field(f1);
        assertEquals(f2, f1);
    }

    @Test
    public void testInt()
            throws Exception
    {
        Type type = INTEGER;
        Integer expected = 12345678;
        Field f1 = new Field(expected, type);
        assertEquals(f1.getInt(), expected);
        assertEquals(f1.getObject(), expected);
        assertEquals(f1.getType(), type);
        assertEquals(f1.toString(), "12345678");

        Field f2 = new Field(f1);
        assertEquals(f2, f1);
    }

    @Test
    public void testLong()
            throws Exception
    {
        Type type = BIGINT;
        Long expected = 12345678L;
        Field f1 = new Field(expected, type);
        assertEquals(f1.getLong(), expected);
        assertEquals(f1.getObject(), expected);
        assertEquals(f1.getType(), type);
        assertEquals(f1.toString(), "12345678");

        Field f2 = new Field(f1);
        assertEquals(f2, f1);
    }

    @Test
    public void testMap()
            throws Exception
    {
        Type type = new MapType(VARCHAR, BIGINT);
        Block expected = AccumuloRowSerializer.getBlockFromMap(type, ImmutableMap.of("a", 1L, "b", 2L, "c", 3L));
        Field f1 = new Field(expected, type);
        assertEquals(f1.getMap(), expected);
        assertEquals(f1.getObject(), expected);
        assertEquals(f1.getType(), type);
        assertEquals(f1.toString(), "MAP(ARRAY ['a','b','c'], ARRAY [1,2,3])");
    }

    @Test
    public void testSmallInt()
            throws Exception
    {
        Type type = SMALLINT;
        Short expected = 12345;
        Field f1 = new Field(expected, type);
        assertEquals(f1.getShort(), expected);
        assertEquals(f1.getObject(), expected);
        assertEquals(f1.getType(), type);
        assertEquals(f1.toString(), "12345");

        Field f2 = new Field(f1);
        assertEquals(f2, f1);
    }

    @Test
    public void testTime()
            throws Exception
    {
        Type type = TIME;
        Time expected = new Time(new GregorianCalendar(1999, 0, 1, 12, 30, 0).getTime().getTime());
        Field f1 = new Field(expected, type);
        assertEquals(f1.getTime(), expected);
        assertEquals(f1.getObject(), expected);
        assertEquals(f1.getType(), type);
        assertEquals(f1.toString(), "TIME '12:30:00'");

        Field f2 = new Field(f1);
        assertEquals(f2, f1);
    }

    @Test
    public void testTimestamp()
            throws Exception
    {
        Type type = TIMESTAMP;
        Timestamp expected = new Timestamp(new GregorianCalendar(1999, 0, 1, 12, 30, 0).getTime().getTime());
        Field f1 = new Field(expected, type);
        assertEquals(f1.getTimestamp(), expected);
        assertEquals(f1.getObject(), expected);
        assertEquals(f1.getType(), type);
        assertEquals(f1.toString(), "TIMESTAMP '1999-01-01 12:30:00.0'");

        Field f2 = new Field(f1);
        assertEquals(f2, f1);
    }

    @Test
    public void testTinyInt()
            throws Exception
    {
        Type type = TINYINT;
        Byte expected = 123;
        Field f1 = new Field(expected, type);
        assertEquals(f1.getByte(), expected);
        assertEquals(f1.getObject(), expected);
        assertEquals(f1.getType(), type);
        assertEquals(f1.toString(), "123");

        Field f2 = new Field(f1);
        assertEquals(f2, f1);
    }

    @Test
    public void testVarbinary()
            throws Exception
    {
        Type type = VARBINARY;
        byte[] expected = "O'Leary".getBytes(UTF_8);
        Field f1 = new Field(expected, type);
        assertEquals(f1.getVarbinary(), expected);
        assertEquals(f1.getObject(), expected);
        assertEquals(f1.getType(), type);
        assertEquals(f1.toString(), "CAST('O''Leary' AS VARBINARY)");

        Field f2 = new Field(f1);
        assertEquals(f2, f1);
    }

    @Test
    public void testVarchar()
            throws Exception
    {
        Type type = VARCHAR;
        String expected = "O'Leary";
        Field f1 = new Field(expected, type);
        assertEquals(f1.getVarchar(), expected);
        assertEquals(f1.getObject(), expected);
        assertEquals(f1.getType(), type);
        assertEquals(f1.toString(), "'O''Leary'");

        Field f2 = new Field(f1);
        assertEquals(f2, f1);
    }
}
