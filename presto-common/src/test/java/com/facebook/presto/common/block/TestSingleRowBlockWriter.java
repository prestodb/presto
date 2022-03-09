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
package com.facebook.presto.common.block;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.google.common.collect.ImmutableList;
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

public class TestSingleRowBlockWriter
{
    @Test
    public void testSizeInBytes()
    {
        MapType mapType = new MapType(
                BIGINT,
                VARCHAR,
                MethodHandleUtil.methodHandle(TestSingleRowBlockWriter.class, "throwUnsupportedOperation"),
                MethodHandleUtil.methodHandle(TestSingleRowBlockWriter.class, "throwUnsupportedOperation"));

        ArrayType arrayType = new ArrayType(DOUBLE);

        RowType.Field rowField = new RowType.Field(Optional.of("my_struct"), INTEGER);
        RowType rowType = RowType.from(ImmutableList.of(rowField));

        List<Type> fieldTypes = ImmutableList.of(REAL, mapType, arrayType, rowType);

        RowBlockBuilder rowBlockBuilder = new RowBlockBuilder(fieldTypes, null, 1);

        for (int i = 0; i < 100; i++) {
            SingleRowBlockWriter singleRowBlockWriter = rowBlockBuilder.beginBlockEntry();
            int expectedSize = 0;
            assertEquals(singleRowBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

            // Write real (4byte value + 1 byte for null)
            REAL.writeLong(singleRowBlockWriter, i);
            expectedSize += 5;
            assertEquals(singleRowBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

            // Write Map<Bigint, Varchar>
            BlockBuilder mapWriter = singleRowBlockWriter.beginBlockEntry();
            // Opening entry, does not account for size.
            assertEquals(singleRowBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

            // Each entry is of 28 bytes, with 6 byte value.
            BIGINT.writeLong(mapWriter, i * 2);
            VARCHAR.writeSlice(mapWriter, utf8Slice("Value1"));
            expectedSize += 28;
            assertEquals(singleRowBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

            // Another entry with 28 bytes.
            BIGINT.writeLong(mapWriter, i * 2 + 1);
            VARCHAR.writeSlice(mapWriter, utf8Slice("Value2"));
            expectedSize += 28;
            assertEquals(singleRowBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

            // closing the entry increases by 5 (offset + null)
            singleRowBlockWriter.closeEntry();
            expectedSize += 5;
            assertEquals(singleRowBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

            // Write array entry.
            BlockBuilder arrayWriter = singleRowBlockWriter.beginBlockEntry();
            assertEquals(singleRowBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

            // Each entry is 9 bytes ( 8 bytes for double , 1 byte for null)
            DOUBLE.writeDouble(arrayWriter, i * 3);
            expectedSize += 9;
            assertEquals(singleRowBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);
            DOUBLE.writeDouble(arrayWriter, i * 3 + 1);
            expectedSize += 9;
            assertEquals(singleRowBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);
            singleRowBlockWriter.closeEntry();
            // closing the entry increases by 5 (offset + null)
            expectedSize += 5;
            assertEquals(singleRowBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

            // Write row type.
            BlockBuilder rowWriter = singleRowBlockWriter.beginBlockEntry();
            assertEquals(singleRowBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

            rowWriter.appendNull();
            expectedSize += 5;
            assertEquals(singleRowBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

            singleRowBlockWriter.closeEntry();
            expectedSize += 5;
            assertEquals(singleRowBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

            rowBlockBuilder.closeEntry();
            assertEquals(singleRowBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

            rowBlockBuilder.appendNull();
        }
    }

    @Test
    public void testSizeInBytesForNulls()
    {
        MapType mapType = new MapType(
                BIGINT,
                VARCHAR,
                MethodHandleUtil.methodHandle(TestSingleRowBlockWriter.class, "throwUnsupportedOperation"),
                MethodHandleUtil.methodHandle(TestSingleRowBlockWriter.class, "throwUnsupportedOperation"));

        ArrayType arrayType = new ArrayType(DOUBLE);

        RowType.Field rowField = new RowType.Field(Optional.of("my_struct"), INTEGER);
        RowType rowType = RowType.from(ImmutableList.of(rowField));

        List<Type> fieldTypes = ImmutableList.of(REAL, mapType, arrayType, rowType);

        RowBlockBuilder rowBlockBuilder = new RowBlockBuilder(fieldTypes, null, 1);

        for (int i = 0; i < 100; i++) {
            SingleRowBlockWriter singleRowBlockWriter = rowBlockBuilder.beginBlockEntry();
            int expectedSize = 0;
            assertEquals(singleRowBlockWriter.getSizeInBytes(), expectedSize);

            // Write real (4byte value + 1 byte for null)
            singleRowBlockWriter.appendNull();
            expectedSize += 5;
            assertEquals(singleRowBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

            // Write Map<Bigint, Varchar>
            singleRowBlockWriter.appendNull();
            expectedSize += 5;
            assertEquals(singleRowBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

            // Write array entry.
            singleRowBlockWriter.appendNull();
            expectedSize += 5;
            assertEquals(singleRowBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

            // Write row type.
            singleRowBlockWriter.appendNull();
            expectedSize += 5;
            assertEquals(singleRowBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

            rowBlockBuilder.closeEntry();
            assertEquals(singleRowBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

            rowBlockBuilder.appendNull();
        }
    }

    public static void throwUnsupportedOperation()
    {
        throw new UnsupportedOperationException();
    }
}
