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
package com.facebook.presto.hive.util;

import com.facebook.presto.hadoop.shaded.com.google.common.collect.ImmutableList;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.io.BytesWritable;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static com.facebook.presto.hive.HiveTestUtils.arraySliceOf;
import static com.facebook.presto.hive.HiveTestUtils.mapSliceOf;
import static com.facebook.presto.hive.HiveTestUtils.rowSliceOf;
import static com.facebook.presto.hive.util.SerDeUtils.getBlockSlice;
import static com.facebook.presto.hive.util.SerDeUtils.serializeObject;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getReflectionObjectInspector;
import static org.testng.Assert.assertEquals;

@SuppressWarnings("PackageVisibleField")
public class TestSerDeUtils
{
    private static class ListHolder
    {
        List<InnerStruct> array;
    }

    private static class InnerStruct
    {
        public InnerStruct(Integer intVal, Long longVal)
        {
            this.intVal = intVal;
            this.longVal = longVal;
        }

        Integer intVal;
        Long longVal;
    }

    private static class OuterStruct
    {
        Byte byteVal;
        Short shortVal;
        Integer intVal;
        Long longVal;
        Float floatVal;
        Double doubleVal;
        String stringVal;
        byte[] byteArray;
        List<InnerStruct> structArray;
        Map<String, InnerStruct> map;
        InnerStruct innerStruct;
    }

    private static ObjectInspector getInspector(Type type)
    {
        return getReflectionObjectInspector(type, ObjectInspectorOptions.JAVA);
    }

    @Test
    public void testPrimitiveSlice()
    {
        // boolean
        Slice expectedBoolean = new DynamicSliceOutput(10).appendByte(1).slice();
        Slice actualBoolean = toBinarySlice(BOOLEAN, true, getInspector(Boolean.class));
        assertEquals(actualBoolean, expectedBoolean);

        // byte
        Slice expectedByte = new DynamicSliceOutput(10).appendLong(5).slice();
        Slice actualByte = toBinarySlice(BIGINT, (byte) 5, getInspector(Byte.class));
        assertEquals(actualByte, expectedByte);
        // short
        Slice expectedShort = new DynamicSliceOutput(10).appendLong(2).slice();
        Slice actualShort = toBinarySlice(BIGINT, (short) 2, getInspector(Short.class));
        assertEquals(actualShort, expectedShort);

        // int
        Slice expectedInt = new DynamicSliceOutput(10).appendLong(1).slice();
        Slice actualInt = toBinarySlice(BIGINT, 1, getInspector(Integer.class));
        assertEquals(actualInt, expectedInt);

        // long
        Slice expectedLong = new DynamicSliceOutput(10).appendLong(10).slice();
        Slice actualLong = toBinarySlice(BIGINT, 10L, getInspector(Long.class));
        assertEquals(actualLong, expectedLong);

        // float
        Slice expectedFloat = new DynamicSliceOutput(10).appendDouble(20.0).slice();
        Slice actualFloat = toBinarySlice(DOUBLE, 20.0f, getInspector(Float.class));
        assertEquals(actualFloat, expectedFloat);

        // double
        Slice expectedDouble = new DynamicSliceOutput(10).appendDouble(30.12).slice();
        Slice actualDouble = toBinarySlice(DOUBLE, 30.12d, getInspector(Double.class));
        assertEquals(actualDouble, expectedDouble);

        // string
        Slice expectedString = Slices.utf8Slice("abdd");
        Slice actualString = toBinarySlice(VARCHAR, "abdd", getInspector(String.class));
        assertEquals(actualString, expectedString);

        // timestamp
        DateTime dateTime = new DateTime(2008, 10, 28, 16, 7, 15, 0);
        Slice expectedTimestamp = new DynamicSliceOutput(10).appendLong(dateTime.getMillis()).slice();
        Slice actualTimestamp = toBinarySlice(TIMESTAMP, new Timestamp(dateTime.getMillis()), getInspector(Timestamp.class));
        assertEquals(actualTimestamp, expectedTimestamp);

        // binary
        byte[] byteArray = {81, 82, 84, 85};
        Slice expectedBinary = Slices.wrappedBuffer(byteArray);
        Slice actualBinary = toBinarySlice(VARBINARY, byteArray, getInspector(byte[].class));
        assertEquals(actualBinary, expectedBinary);
    }

    @Test
    public void testListBlock()
    {
        List<InnerStruct> array = new ArrayList<>(2);
        array.add(new InnerStruct(8, 9L));
        array.add(new InnerStruct(10, 11L));
        ListHolder listHolder = new ListHolder();
        listHolder.array = array;

        com.facebook.presto.spi.type.Type rowType = new RowType(ImmutableList.of(BIGINT, BIGINT), Optional.empty());
        com.facebook.presto.spi.type.Type arrayType = new ArrayType(rowType);
        com.facebook.presto.spi.type.Type type = new ArrayType(arrayType);
        Slice value = arraySliceOf(rowType,
                rowSliceOf(rowType.getTypeParameters(), 8, 9L),
                rowSliceOf(rowType.getTypeParameters(), 10, 11L));

        Slice expected = arraySliceOf(arrayType, value);
        Slice actual = toBinarySlice(type, listHolder, getInspector(ListHolder.class));

        assertEquals(actual, expected);
    }

    private static class MapHolder
    {
        Map<String, InnerStruct> map;
    }

    @Test
    public void testMapBlock()
    {
        MapHolder holder = new MapHolder();
        holder.map = new TreeMap<>();
        holder.map.put("twelve", new InnerStruct(13, 14L));
        holder.map.put("fifteen", new InnerStruct(16, 17L));

        com.facebook.presto.spi.type.Type rowType = new RowType(ImmutableList.of(BIGINT, BIGINT), Optional.empty());
        com.facebook.presto.spi.type.Type mapType = new MapType(VARCHAR, rowType);
        com.facebook.presto.spi.type.Type type = new ArrayType(mapType);

        Slice value = mapSliceOf(VARCHAR, rowType,
                "fifteen", rowSliceOf(rowType.getTypeParameters(), 16, 17L),
                "twelve",  rowSliceOf(rowType.getTypeParameters(), 13, 14L));

        Slice expected = arraySliceOf(mapType, value);
        Slice actual = toBinarySlice(type, holder, getInspector(MapHolder.class));

        assertEquals(actual, expected);
    }

    @Test
    public void testStructBlock()
    {
        // test simple structs
        com.facebook.presto.spi.type.Type rowType = new RowType(ImmutableList.of(BIGINT, BIGINT), Optional.empty());

        InnerStruct innerStruct = new InnerStruct(13, 14L);
        Slice actual = toBinarySlice(rowType, innerStruct, getInspector(InnerStruct.class));
        Slice expected = rowSliceOf(ImmutableList.of(BIGINT, BIGINT), 13, 14L);
        assertEquals(actual, expected);

        // test complex structs
        OuterStruct outerStruct = new OuterStruct();
        outerStruct.byteVal = 1;
        outerStruct.shortVal = 2;
        outerStruct.intVal = 3;
        outerStruct.longVal = 4L;
        outerStruct.floatVal = 5.01f;
        outerStruct.doubleVal = 6.001d;
        outerStruct.stringVal = "seven";
        outerStruct.byteArray = new byte[] {'2'};
        InnerStruct is1 = new InnerStruct(2, -5L);
        InnerStruct is2 = new InnerStruct(-10, 0L);
        outerStruct.structArray = new ArrayList<>(2);
        outerStruct.structArray.add(is1);
        outerStruct.structArray.add(is2);
        outerStruct.map = new TreeMap<>();
        outerStruct.map.put("twelve", new InnerStruct(0, 5L));
        outerStruct.map.put("fifteen", new InnerStruct(-5, -10L));
        outerStruct.innerStruct = new InnerStruct(18, 19L);

        com.facebook.presto.spi.type.Type arrayOfInnerRowType = new ArrayType(rowType);
        com.facebook.presto.spi.type.Type mapOfInnerRowType = new MapType(VARCHAR, rowType);
        List<com.facebook.presto.spi.type.Type> outerRowParameterTypes = ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT, DOUBLE, DOUBLE, VARCHAR, VARCHAR, arrayOfInnerRowType, mapOfInnerRowType, rowType);
        com.facebook.presto.spi.type.Type complexType = new RowType(outerRowParameterTypes, Optional.empty());

        Slice arrayValue = arraySliceOf(rowType, rowSliceOf(rowType.getTypeParameters(), 2, -5L), rowSliceOf(rowType.getTypeParameters(), -10, 0));
        Slice mapValue = mapSliceOf(VARCHAR, rowType,
                "fifteen", rowSliceOf(rowType.getTypeParameters(), -5, -10L),
                "twelve",  rowSliceOf(rowType.getTypeParameters(), 0, 5L));
        Slice rowValue = rowSliceOf(rowType.getTypeParameters(), 18, 19);
        expected = rowSliceOf(outerRowParameterTypes,
                1, 2, 3, 4L, 5.01f, 6.001d, "seven", new byte[] {'2'},
                arrayValue, mapValue, rowValue);

        actual = toBinarySlice(complexType, outerStruct, getInspector(OuterStruct.class));

        assertEquals(actual, expected);
    }

    @Test
    public void testReuse()
            throws Exception
    {
        BytesWritable value = new BytesWritable();

        byte[] first = "hello world".getBytes(UTF_8);
        value.set(first, 0, first.length);

        byte[] second = "bye".getBytes(UTF_8);
        value.set(second, 0, second.length);

        Type type = new TypeToken<Map<BytesWritable, Integer>>() {}.getType();
        ObjectInspector inspector = getReflectionObjectInspector(type, ObjectInspectorOptions.JAVA);

        Slice actual = getBlockSlice(ImmutableMap.of(value, 0), inspector, new MapType(VARCHAR, BIGINT));
        Slice expected = mapSliceOf(VARCHAR, BIGINT, "bye", 0);

        assertEquals(actual, expected);
    }

    private static Slice toBinarySlice(com.facebook.presto.spi.type.Type type, Object object, ObjectInspector inspector)
    {
        if (inspector.getCategory() == Category.PRIMITIVE) {
            return getPrimitiveSlice(type, object, inspector);
        }
        return getBlockSlice(object, inspector, type);
    }

    private static Slice getPrimitiveSlice(com.facebook.presto.spi.type.Type type, Object object, ObjectInspector inspector)
    {
        BlockBuilder builder = type.createBlockBuilder(new BlockBuilderStatus(), 1);
        serializeObject(builder, object, inspector, type);
        if (builder.isNull(0)) {
            return Slices.EMPTY_SLICE;
        }
        Block block = builder.build();
        return VarbinaryType.VARBINARY.getSlice(block, 0);
    }
}
