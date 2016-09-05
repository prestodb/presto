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
package com.facebook.presto.accumulo.serializers;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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

public abstract class AbstractTestAccumuloRowSerializer
{
    private final Class<? extends AccumuloRowSerializer> serializerClass;
    private static final String COLUMN_NAME = "foo";

    protected AbstractTestAccumuloRowSerializer(Class<? extends AccumuloRowSerializer> serializerClass)
    {
        this.serializerClass = serializerClass;
    }

    @Test
    public void testArray()
            throws Exception
    {
        AccumuloRowSerializer serializer = serializerClass.getConstructor().newInstance();
        Type type = new ArrayType(VARCHAR);
        List<Object> expected = ImmutableList.of("a", "b", "c");
        byte[] data = serializer.encode(type, AccumuloRowSerializer.getBlockFromArray(VARCHAR, expected));
        List<Object> actual = serializer.decode(type, data);
        assertEquals(actual, expected);

        deserializeData(serializer, data);
        actual = AccumuloRowSerializer.getArrayFromBlock(VARCHAR, serializer.getArray(COLUMN_NAME, type));
        assertEquals(actual, expected);
    }

    @Test
    public void testBoolean()
            throws Exception
    {
        AccumuloRowSerializer serializer = serializerClass.getConstructor().newInstance();
        Type type = BOOLEAN;
        byte[] data = serializer.encode(type, true);
        boolean actual = serializer.decode(type, data);
        assertEquals(actual, true);

        deserializeData(serializer, data);
        actual = serializer.getBoolean(COLUMN_NAME);
        assertEquals(actual, true);

        data = serializer.encode(type, false);
        actual = serializer.decode(type, data);
        assertEquals(actual, false);

        deserializeData(serializer, data);
        actual = serializer.getBoolean(COLUMN_NAME);
        assertEquals(actual, false);
    }

    @Test
    public void testDate()
            throws Exception
    {
        AccumuloRowSerializer serializer = serializerClass.getConstructor().newInstance();
        Type type = DATE;
        Date expected = new Date(new java.util.Date().getTime());
        byte[] data = serializer.encode(type, expected);
        Date actual = new Date(serializer.decode(type, data));
        assertEquals(actual, expected);

        deserializeData(serializer, data);
        actual = serializer.getDate(COLUMN_NAME);
        assertEquals(actual, expected);
    }

    @Test
    public void testDouble()
            throws Exception
    {
        AccumuloRowSerializer serializer = serializerClass.getConstructor().newInstance();
        Type type = DOUBLE;
        Double expected = 123.45678;
        byte[] data = serializer.encode(type, expected);
        Double actual = serializer.decode(type, data);
        assertEquals(actual, expected);

        deserializeData(serializer, data);
        actual = serializer.getDouble(COLUMN_NAME);
        assertEquals(actual, expected);
    }

    @Test
    public void testFloat()
            throws Exception
    {
        AccumuloRowSerializer serializer = serializerClass.getConstructor().newInstance();
        Type type = REAL;
        Float expected = 123.45678f;
        byte[] data = serializer.encode(type, expected);
        Float actual = ((Double) serializer.decode(type, data)).floatValue();
        assertEquals(actual, expected);

        deserializeData(serializer, data);
        actual = serializer.getFloat(COLUMN_NAME);
        assertEquals(actual, expected);
    }

    @Test
    public void testInt()
            throws Exception
    {
        AccumuloRowSerializer serializer = serializerClass.getConstructor().newInstance();
        Type type = INTEGER;
        Integer expected = 123456;
        byte[] data = serializer.encode(type, expected);
        @SuppressWarnings("unchecked")
        Integer actual = ((Long) serializer.decode(type, data)).intValue();
        assertEquals(actual, expected);

        deserializeData(serializer, data);
        actual = serializer.getInt(COLUMN_NAME);
        assertEquals(actual, expected);
    }

    @Test
    public void testLong()
            throws Exception
    {
        AccumuloRowSerializer serializer = serializerClass.getConstructor().newInstance();
        Type type = BIGINT;
        Long expected = 123456L;
        byte[] data = serializer.encode(type, expected);
        Long actual = serializer.decode(type, data);
        assertEquals(actual, expected);

        deserializeData(serializer, data);
        actual = serializer.getLong(COLUMN_NAME);
        assertEquals(actual, expected);
    }

    @Test
    public void testMap()
            throws Exception
    {
        AccumuloRowSerializer serializer = serializerClass.getConstructor().newInstance();
        Type type = new MapType(VARCHAR, BIGINT);
        Map<Object, Object> expected = ImmutableMap.of("a", 1L, "b", 2L, "3", 3L);
        byte[] data = serializer.encode(type, AccumuloRowSerializer.getBlockFromMap(type, expected));
        Map<Object, Object> actual = serializer.decode(type, data);
        assertEquals(actual, expected);

        deserializeData(serializer, data);
        actual = AccumuloRowSerializer.getMapFromBlock(type, serializer.getMap(COLUMN_NAME, type));
        assertEquals(actual, expected);
    }

    @Test
    public void testSmallInt()
            throws Exception
    {
        AccumuloRowSerializer serializer = serializerClass.getConstructor().newInstance();
        Type type = SMALLINT;
        Short expected = 12345;
        byte[] data = serializer.encode(type, expected);
        Short actual = ((Long) serializer.decode(type, data)).shortValue();
        assertEquals(actual, expected);

        deserializeData(serializer, data);
        actual = serializer.getShort(COLUMN_NAME);
        assertEquals(actual, expected);
    }

    @Test
    public void testTime()
            throws Exception
    {
        AccumuloRowSerializer serializer = serializerClass.getConstructor().newInstance();
        Type type = TIME;
        Time expected = new Time(new java.util.Date().getTime());
        byte[] data = serializer.encode(type, expected);
        Time actual = new Time(serializer.decode(type, data));
        assertEquals(actual, expected);

        deserializeData(serializer, data);
        actual = serializer.getTime(COLUMN_NAME);
        assertEquals(actual, expected);
    }

    @Test
    public void testTimestamp()
            throws Exception
    {
        AccumuloRowSerializer serializer = serializerClass.getConstructor().newInstance();
        Type type = TIMESTAMP;
        Timestamp expected = new Timestamp(new java.util.Date().getTime());
        byte[] data = serializer.encode(type, expected);
        Timestamp actual = new Timestamp(serializer.decode(type, data));
        assertEquals(actual, expected);

        deserializeData(serializer, data);
        actual = serializer.getTimestamp(COLUMN_NAME);
        assertEquals(actual, expected);
    }

    @Test
    public void testTinyInt()
            throws Exception
    {
        AccumuloRowSerializer serializer = serializerClass.getConstructor().newInstance();
        Type type = TINYINT;
        Byte expected = 123;
        byte[] data = serializer.encode(type, expected);
        Byte actual = ((Long) serializer.decode(type, data)).byteValue();
        assertEquals(actual, expected);

        deserializeData(serializer, data);
        actual = serializer.getByte(COLUMN_NAME);
        assertEquals(actual, expected);
    }

    @Test
    public void testVarbinary()
            throws Exception
    {
        AccumuloRowSerializer serializer = serializerClass.getConstructor().newInstance();
        Type type = VARBINARY;
        byte[] expected = b(UUID.randomUUID().toString());
        byte[] data = serializer.encode(type, expected);
        byte[] actual = serializer.decode(type, data);
        assertEquals(actual, expected);

        deserializeData(serializer, data);
        actual = serializer.getVarbinary(COLUMN_NAME);
        assertEquals(actual, expected);
    }

    @Test
    public void testVarchar()
            throws Exception
    {
        AccumuloRowSerializer serializer = serializerClass.getConstructor().newInstance();
        Type type = VARCHAR;
        String expected = UUID.randomUUID().toString();
        byte[] data = serializer.encode(type, expected);
        String actual = serializer.decode(type, data);
        assertEquals(actual, expected);

        deserializeData(serializer, data);
        actual = serializer.getVarchar(COLUMN_NAME);
        assertEquals(actual, expected);
    }

    protected void deserializeData(AccumuloRowSerializer serializer, byte[] data)
            throws Exception
    {
        Mutation m = new Mutation("row");
        m.put(b("a"), b("a"), data);
        Key key = new Key(b("row"), b("a"), b("b"), b(), 0, false);
        Value value = new Value(data);
        serializer.setMapping(COLUMN_NAME, "a", "b");
        serializer.deserialize(new SimpleImmutableEntry<>(key, value));
    }

    protected static byte[] b(String str)
    {
        return str.getBytes(UTF_8);
    }

    protected static byte[] b()
    {
        return new byte[0];
    }
}
