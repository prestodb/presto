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
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
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
import org.joda.time.DateTimeZone;
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
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.TypeUtils.buildStructuralSlice;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getReflectionObjectInspector;
import static org.testng.Assert.assertEquals;

@SuppressWarnings("PackageVisibleField")
public class TestSerDeUtils
{
    private static final DateTimeZone SESSION_TIME_ZONE = DateTimeZone.forID("Europe/Berlin");

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
        Slice actualBoolean = toBinarySlice(true, getInspector(Boolean.class));
        assertEquals(actualBoolean, expectedBoolean);

        // byte
        Slice expectedByte = new DynamicSliceOutput(10).appendLong(5).slice();
        Slice actualByte = toBinarySlice((byte) 5, getInspector(Byte.class));
        assertEquals(actualByte, expectedByte);
        // short
        Slice expectedShort = new DynamicSliceOutput(10).appendLong(2).slice();
        Slice actualShort = toBinarySlice((short) 2, getInspector(Short.class));
        assertEquals(actualShort, expectedShort);

        // int
        Slice expectedInt = new DynamicSliceOutput(10).appendLong(1).slice();
        Slice actualInt = toBinarySlice(1, getInspector(Integer.class));
        assertEquals(actualInt, expectedInt);

        // long
        Slice expectedLong = new DynamicSliceOutput(10).appendLong(10).slice();
        Slice actualLong = toBinarySlice(10L, getInspector(Long.class));
        assertEquals(actualLong, expectedLong);

        // float
        Slice expectedFloat = new DynamicSliceOutput(10).appendDouble(20.0).slice();
        Slice actualFloat = toBinarySlice(20.0f, getInspector(Float.class));
        assertEquals(actualFloat, expectedFloat);

        // double
        Slice expectedDouble = new DynamicSliceOutput(10).appendDouble(30.12).slice();
        Slice actualDouble = toBinarySlice(30.12d, getInspector(Double.class));
        assertEquals(actualDouble, expectedDouble);

        // string
        Slice expectedString = Slices.utf8Slice("abdd");
        Slice actualString = toBinarySlice("abdd", getInspector(String.class));
        assertEquals(actualString, expectedString);

        // timestamp
        DateTime dateTime = new DateTime(2008, 10, 28, 16, 7, 15, 0);
        Slice expectedTimestamp = new DynamicSliceOutput(10).appendLong(dateTime.getMillis()).slice();
        Slice actualTimestamp = toBinarySlice(new Timestamp(dateTime.getMillis()), getInspector(Timestamp.class));
        assertEquals(actualTimestamp, expectedTimestamp);

        // binary
        byte[] byteArray = {81, 82, 84, 85};
        Slice expectedBinary = Slices.wrappedBuffer(byteArray);
        Slice actualBinary = toBinarySlice(byteArray, getInspector(byte[].class));
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

        Slice actual = toBinarySlice(listHolder, getInspector(ListHolder.class));

        com.facebook.presto.spi.type.Type rowType = new RowType(ImmutableList.of(BIGINT, BIGINT), Optional.empty());
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), 1024);
        rowType.writeSlice(blockBuilder, rowSliceOf(ImmutableList.of(BIGINT, BIGINT), 8, 9L));
        rowType.writeSlice(blockBuilder, rowSliceOf(ImmutableList.of(BIGINT, BIGINT), 10, 11L));
        Slice expected = rowSliceOf(ImmutableList.of(new ArrayType(rowType)), buildStructuralSlice(blockBuilder));

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

        Slice actual = toBinarySlice(holder, getInspector(MapHolder.class));

        com.facebook.presto.spi.type.Type rowType = new RowType(ImmutableList.of(BIGINT, BIGINT), Optional.empty());
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), 1024);
        VARCHAR.writeString(blockBuilder, "fifteen");
        rowType.writeSlice(blockBuilder, rowSliceOf(ImmutableList.of(BIGINT, BIGINT), 16, 17L));
        VARCHAR.writeString(blockBuilder, "twelve");
        rowType.writeSlice(blockBuilder, rowSliceOf(ImmutableList.of(BIGINT, BIGINT), 13, 14L));
        Slice expected = rowSliceOf(ImmutableList.of(new MapType(VARCHAR, rowType)), buildStructuralSlice(blockBuilder));

        assertEquals(actual, expected);
    }

    @Test
    public void testStructBlock()
    {
        // test simple structs
        InnerStruct innerStruct = new InnerStruct(13, 14L);
        Slice actual = toBinarySlice(innerStruct, getInspector(InnerStruct.class));
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

        actual = toBinarySlice(outerStruct, getInspector(OuterStruct.class));
        com.facebook.presto.spi.type.Type innerRowType = new RowType(ImmutableList.of(BIGINT, BIGINT), Optional.empty());
        com.facebook.presto.spi.type.Type arrayOfInnerRowType = new ArrayType(innerRowType);
        com.facebook.presto.spi.type.Type mapOfInnerRowType = new MapType(VARCHAR, innerRowType);
        List<com.facebook.presto.spi.type.Type> outerRowParameterTypes = ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT, DOUBLE, DOUBLE, VARCHAR, VARCHAR, arrayOfInnerRowType, mapOfInnerRowType, innerRowType);

        ImmutableList.Builder<Object> outerRowValues = ImmutableList.builder();
        outerRowValues.add(1);
        outerRowValues.add(2);
        outerRowValues.add(3);
        outerRowValues.add(4L);
        outerRowValues.add(5.01f);
        outerRowValues.add(6.001d);
        outerRowValues.add("seven");
        outerRowValues.add(new byte[] {'2'});
        outerRowValues.add(arraySliceOf(innerRowType, rowSliceOf(ImmutableList.of(BIGINT, BIGINT), 2, -5L), rowSliceOf(ImmutableList.of(BIGINT, BIGINT), -10, 0)));
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), 1024);
        VARCHAR.writeString(blockBuilder, "fifteen");
        innerRowType.writeSlice(blockBuilder, rowSliceOf(ImmutableList.of(BIGINT, BIGINT), -5, -10L));
        VARCHAR.writeString(blockBuilder, "twelve");
        innerRowType.writeSlice(blockBuilder, rowSliceOf(ImmutableList.of(BIGINT, BIGINT), 0, 5L));
        outerRowValues.add(buildStructuralSlice(blockBuilder));
        outerRowValues.add(rowSliceOf(ImmutableList.of(BIGINT, BIGINT), 18, 19L));

        assertEquals(actual, rowSliceOf(outerRowParameterTypes, outerRowValues.build().toArray()));
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

        Slice actual = getBlockSlice(SESSION_TIME_ZONE, ImmutableMap.of(value, 0), inspector);
        Slice expected = mapSliceOf(VARCHAR, BIGINT, "bye", 0);

        assertEquals(actual, expected);
    }

    private static Slice toBinarySlice(Object object, ObjectInspector inspector)
    {
        if (inspector.getCategory() == Category.PRIMITIVE) {
            return getPrimitiveSlice(SESSION_TIME_ZONE, object, inspector);
        }
        return getBlockSlice(SESSION_TIME_ZONE, object, inspector);
    }

    private static Slice getPrimitiveSlice(DateTimeZone sessionTimeZone, Object object, ObjectInspector inspector)
    {
        BlockBuilder builder = VarbinaryType.VARBINARY.createBlockBuilder(new BlockBuilderStatus(), 1);
        serializeObject(sessionTimeZone, builder, object, inspector);
        if (builder.isNull(0)) {
            return Slices.EMPTY_SLICE;
        }
        Block block = builder.build();
        return VarbinaryType.VARBINARY.getSlice(block, 0);
    }
}
