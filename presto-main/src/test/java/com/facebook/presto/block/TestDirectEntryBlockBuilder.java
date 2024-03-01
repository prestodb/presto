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

package com.facebook.presto.block;

import com.facebook.presto.common.block.ArrayBlockBuilder;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.block.MapBlockBuilder;
import com.facebook.presto.common.block.MethodHandleUtil;
import com.facebook.presto.common.block.RowBlockBuilder;
import com.facebook.presto.common.block.SingleArrayBlockWriter;
import com.facebook.presto.common.block.SingleRowBlockWriter;
import com.facebook.presto.common.block.TestingBlockEncodingSerde;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestDirectEntryBlockBuilder
{
    private static final int POSITION_COUNT = 100;
    private final BlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();

    @Test
    public void testArrayWithNestedMap()
    {
        MapType mapType = new MapType(
                BIGINT,
                VARCHAR,
                MethodHandleUtil.methodHandle(TestDirectEntryBlockBuilder.class, "throwUnsupportedOperation"),
                MethodHandleUtil.methodHandle(TestDirectEntryBlockBuilder.class, "throwUnsupportedOperation"));

        ArrayBlockBuilder beginEntryBlockBuilder = new ArrayBlockBuilder(mapType, null, POSITION_COUNT);

        // Each array element, consists of map and null alternatively, repeated 10 times..
        // Each map contains 2 entries {i: value1j, i+1: value2j }

        for (int i = 0; i < POSITION_COUNT; i++) {
            SingleArrayBlockWriter singleArrayBlockWriter = beginEntryBlockBuilder.beginBlockEntry();
            for (int j = 0; j < 10; j++) {
                BlockBuilder innerMapWriter = singleArrayBlockWriter.beginBlockEntry();
                BIGINT.writeLong(innerMapWriter, i);
                VARCHAR.writeSlice(innerMapWriter, utf8Slice("Value1" + j));

                BIGINT.writeLong(innerMapWriter, i + 1);
                VARCHAR.writeSlice(innerMapWriter, utf8Slice("Value2" + j));
                singleArrayBlockWriter.closeEntry();

                singleArrayBlockWriter.appendNull();
            }

            beginEntryBlockBuilder.closeEntry();
        }

        ArrayBlockBuilder directEntryBlockBuilder = new ArrayBlockBuilder(mapType, null, POSITION_COUNT);
        for (int i = 0; i < POSITION_COUNT; i++) {
            directEntryBlockBuilder.beginDirectEntry();
            MapBlockBuilder innerBuilder = (MapBlockBuilder) directEntryBlockBuilder.getElementBlockBuilder();

            BlockBuilder keyBuilder = innerBuilder.getKeyBlockBuilder();
            BlockBuilder valueBuilder = innerBuilder.getValueBlockBuilder();
            for (int j = 0; j < 10; j++) {
                innerBuilder.beginDirectEntry();
                BIGINT.writeLong(keyBuilder, i);
                BIGINT.writeLong(keyBuilder, i + 1);

                VARCHAR.writeSlice(valueBuilder, utf8Slice("Value1" + j));
                VARCHAR.writeSlice(valueBuilder, utf8Slice("Value2" + j));
                innerBuilder.closeEntry();

                innerBuilder.appendNull();
            }

            directEntryBlockBuilder.closeEntry();
        }

        Slice beginEntrySlice = getSlice(beginEntryBlockBuilder);
        Slice directEntrySlice = getSlice(directEntryBlockBuilder);

        assertEquals(beginEntrySlice.compareTo(directEntrySlice), 0);
    }

    private Slice getSlice(BlockBuilder beginEntryBlockBuilder)
    {
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(sliceOutput, beginEntryBlockBuilder);
        return sliceOutput.slice();
    }

    public static void throwUnsupportedOperation()
    {
        throw new UnsupportedOperationException();
    }

    @Test
    public void testNestedRow()
    {
        MapType mapType = new MapType(
                BIGINT,
                VARCHAR,
                MethodHandleUtil.methodHandle(TestDirectEntryBlockBuilder.class, "throwUnsupportedOperation"),
                MethodHandleUtil.methodHandle(TestDirectEntryBlockBuilder.class, "throwUnsupportedOperation"));

        ArrayType arrayType = new ArrayType(DOUBLE);

        RowType.Field nestedRowField = new RowType.Field(Optional.of("my_struct"), INTEGER);
        RowType nestedRowType = RowType.from(ImmutableList.of(nestedRowField));

        List<Type> rowType = ImmutableList.of(REAL, mapType, arrayType, nestedRowType);

        RowBlockBuilder beginEntryBlockBuilder = new RowBlockBuilder(rowType, null, 1);

        for (int i = 0; i < POSITION_COUNT; i++) {
            SingleRowBlockWriter singleRowBlockWriter = beginEntryBlockBuilder.beginBlockEntry();

            REAL.writeLong(singleRowBlockWriter, i);

            // Write Map<Bigint, Varchar> with 5 entries.
            BlockBuilder mapWriter = singleRowBlockWriter.beginBlockEntry();
            for (int j = 0; j < 5; j++) {
                BIGINT.writeLong(mapWriter, i + j);
                VARCHAR.writeSlice(mapWriter, utf8Slice("Value1" + j));
            }
            singleRowBlockWriter.closeEntry();

            // Write array .
            BlockBuilder arrayWriter = singleRowBlockWriter.beginBlockEntry();
            for (int j = 0; j < 8; j++) {
                DOUBLE.writeDouble(arrayWriter, i * 3 + j);
            }
            singleRowBlockWriter.closeEntry();

            // Write row type.
            BlockBuilder rowWriter = singleRowBlockWriter.beginBlockEntry();
            if (i % 2 == 0) {
                rowWriter.appendNull();
            }
            else {
                INTEGER.writeLong(rowWriter, i);
            }
            singleRowBlockWriter.closeEntry();
            beginEntryBlockBuilder.closeEntry();

            beginEntryBlockBuilder.appendNull();
        }

        RowBlockBuilder directEntryBlockBuilder = new RowBlockBuilder(rowType, null, 1);

        for (int i = 0; i < POSITION_COUNT; i++) {
            directEntryBlockBuilder.beginDirectEntry();

            REAL.writeLong(directEntryBlockBuilder.getBlockBuilder(0), i);

            // Write Map<Bigint, Varchar> with 5 entries.
            MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) directEntryBlockBuilder.getBlockBuilder(1);
            mapBlockBuilder.beginDirectEntry();
            BlockBuilder keyBuilder = mapBlockBuilder.getKeyBlockBuilder();
            BlockBuilder valueBuilder = mapBlockBuilder.getValueBlockBuilder();
            for (int j = 0; j < 5; j++) {
                BIGINT.writeLong(keyBuilder, i + j);
                VARCHAR.writeSlice(valueBuilder, utf8Slice("Value1" + j));
            }
            mapBlockBuilder.closeEntry();

            // Write array .
            ArrayBlockBuilder arrayBuilder = (ArrayBlockBuilder) directEntryBlockBuilder.getBlockBuilder(2);
            arrayBuilder.beginDirectEntry();
            for (int j = 0; j < 8; j++) {
                DOUBLE.writeDouble(arrayBuilder.getElementBlockBuilder(), i * 3 + j);
            }
            arrayBuilder.closeEntry();

            // Write row type.
            RowBlockBuilder nestedRowBuilder = (RowBlockBuilder) directEntryBlockBuilder.getBlockBuilder(3);
            nestedRowBuilder.beginDirectEntry();
            BlockBuilder nestedRowValueBuilder = nestedRowBuilder.getBlockBuilder(0);
            if (i % 2 == 0) {
                nestedRowValueBuilder.appendNull();
            }
            else {
                INTEGER.writeLong(nestedRowValueBuilder, i);
            }
            nestedRowBuilder.closeEntry();
            directEntryBlockBuilder.closeEntry();

            directEntryBlockBuilder.appendNull();
        }

        Slice beginEntrySlice = getSlice(beginEntryBlockBuilder);
        Slice directEntrySlice = getSlice(directEntryBlockBuilder);
        assertEquals(beginEntrySlice.compareTo(directEntrySlice), 0);
    }

    @Test
    public void testNestedMap()
    {
        MapType innerMap = new MapType(
                BIGINT,
                VARCHAR,
                MethodHandleUtil.methodHandle(TestDirectEntryBlockBuilder.class, "throwUnsupportedOperation"),
                MethodHandleUtil.methodHandle(TestDirectEntryBlockBuilder.class, "throwUnsupportedOperation"));

        ArrayType arrayType = new ArrayType(innerMap);

        MapType mapType = new MapType(
                BIGINT,
                arrayType,
                MethodHandleUtil.methodHandle(TestDirectEntryBlockBuilder.class, "throwUnsupportedOperation"),
                MethodHandleUtil.methodHandle(TestDirectEntryBlockBuilder.class, "throwUnsupportedOperation"));

        MapBlockBuilder beginEntryBlockBuilder = (MapBlockBuilder) mapType.createBlockBuilder(null, POSITION_COUNT);
        for (int i = 0; i < POSITION_COUNT; i++) {
            BlockBuilder mapWriter = beginEntryBlockBuilder.beginBlockEntry();
            for (int j = 0; j < 7; j++) {
                BIGINT.writeLong(mapWriter, i * 7 + j); // Key

                BlockBuilder arrayWriter = mapWriter.beginBlockEntry();
                // Each array contains  a map and null alternatively 10 times.
                // Map contains 4 elements.
                for (int k = 0; k < 10; k++) {
                    BlockBuilder innerMapWriter = arrayWriter.beginBlockEntry();
                    for (int l = 0; l < 3; l++) {
                        BIGINT.writeLong(innerMapWriter, k * 10 + l);
                        VARCHAR.writeSlice(innerMapWriter, utf8Slice("Value" + l));
                    }
                    arrayWriter.closeEntry();

                    arrayWriter.appendNull();
                }
                mapWriter.closeEntry();
            }
            beginEntryBlockBuilder.closeEntry();
        }

        MapBlockBuilder directEntryBlockBuilder = (MapBlockBuilder) mapType.createBlockBuilder(null, POSITION_COUNT);
        for (int i = 0; i < POSITION_COUNT; i++) {
            directEntryBlockBuilder.beginDirectEntry();
            BlockBuilder keyBuilder = directEntryBlockBuilder.getKeyBlockBuilder();
            ArrayBlockBuilder arrayBuilder = (ArrayBlockBuilder) directEntryBlockBuilder.getValueBlockBuilder();
            MapBlockBuilder innerMapBuilder = (MapBlockBuilder) arrayBuilder.getElementBlockBuilder();
            BlockBuilder innerMapKeyBuilder = innerMapBuilder.getKeyBlockBuilder();
            BlockBuilder innerMapValueBuilder = innerMapBuilder.getValueBlockBuilder();

            for (int j = 0; j < 7; j++) {
                BIGINT.writeLong(keyBuilder, i * 7 + j); // Key

                arrayBuilder.beginDirectEntry();
                // Each array contains  a map and null alternatively 10 times.
                // Map contains 4 elements.
                for (int k = 0; k < 10; k++) {
                    innerMapBuilder.beginDirectEntry();
                    for (int l = 0; l < 3; l++) {
                        BIGINT.writeLong(innerMapKeyBuilder, k * 10 + l);
                        VARCHAR.writeSlice(innerMapValueBuilder, utf8Slice("Value" + l));
                    }
                    innerMapBuilder.closeEntry();

                    innerMapBuilder.appendNull();
                }
                arrayBuilder.closeEntry();
            }
            directEntryBlockBuilder.closeEntry();
        }

        Slice beginEntrySlice = getSlice(beginEntryBlockBuilder);
        Slice directEntrySlice = getSlice(directEntryBlockBuilder);
        assertEquals(beginEntrySlice.compareTo(directEntrySlice), 0);
    }
}
