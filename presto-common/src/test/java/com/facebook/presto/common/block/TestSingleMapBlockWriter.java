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

import com.facebook.presto.common.type.MapType;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestSingleMapBlockWriter
{
    private static final int MAP_POSITIONS = 100;

    @Test
    public void testSizeInBytes()
    {
        MapType innerMapType = new MapType(
                BIGINT,
                VARCHAR,
                MethodHandleUtil.methodHandle(TestSingleMapBlockWriter.class, "throwUnsupportedOperation"),
                MethodHandleUtil.methodHandle(TestSingleMapBlockWriter.class, "throwUnsupportedOperation"));

        MapType mapType = new MapType(
                BIGINT,
                innerMapType,
                MethodHandleUtil.methodHandle(TestSingleMapBlockWriter.class, "throwUnsupportedOperation"),
                MethodHandleUtil.methodHandle(TestSingleMapBlockWriter.class, "throwUnsupportedOperation"));

        MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) mapType.createBlockBuilder(null, MAP_POSITIONS);

        for (int i = 0; i < MAP_POSITIONS; i++) {
            SingleMapBlockWriter singleMapBlockWriter = mapBlockBuilder.beginBlockEntry();
            int expectedSize = 0;
            for (int j = 0; j < 10; j++) {
                assertEquals(singleMapBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

                // Write Map's Key (long + isNull)
                BIGINT.writeLong(singleMapBlockWriter, j);
                expectedSize += 9;
                assertEquals(singleMapBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

                // Write Map<Bigint, Varchar>
                BlockBuilder innerMapWriter = singleMapBlockWriter.beginBlockEntry();
                // Opening entry, does not account for size.
                assertEquals(singleMapBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

                // Each entry is of 28 bytes, with 6 byte value.
                BIGINT.writeLong(innerMapWriter, i * 2);
                VARCHAR.writeSlice(innerMapWriter, utf8Slice("Value1"));
                expectedSize += 28;
                assertEquals(singleMapBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

                // Another entry with 28 bytes.
                BIGINT.writeLong(innerMapWriter, i * 2 + 1);
                VARCHAR.writeSlice(innerMapWriter, utf8Slice("Value2"));
                expectedSize += 28;
                assertEquals(singleMapBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

                // closing the entry increases by 5 (offset + null)
                singleMapBlockWriter.closeEntry();
                expectedSize += 5;
                assertEquals(singleMapBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);
            }
            mapBlockBuilder.closeEntry();
            assertEquals(singleMapBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

            // Introduce some null elements in Map and the size should still work.
            mapBlockBuilder.appendNull();
        }
    }

    @Test
    public void testSizeInBytesForNulls()
    {
        MapType innerMapType = new MapType(
                BIGINT,
                VARCHAR,
                MethodHandleUtil.methodHandle(TestSingleMapBlockWriter.class, "throwUnsupportedOperation"),
                MethodHandleUtil.methodHandle(TestSingleMapBlockWriter.class, "throwUnsupportedOperation"));

        MapType mapType = new MapType(
                BIGINT,
                innerMapType,
                MethodHandleUtil.methodHandle(TestSingleMapBlockWriter.class, "throwUnsupportedOperation"),
                MethodHandleUtil.methodHandle(TestSingleMapBlockWriter.class, "throwUnsupportedOperation"));

        MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) mapType.createBlockBuilder(null, MAP_POSITIONS);

        for (int i = 0; i < MAP_POSITIONS; i++) {
            SingleMapBlockWriter singleMapBlockWriter = mapBlockBuilder.beginBlockEntry();
            int expectedSize = 0;
            for (int j = 0; j < 10; j++) {
                assertEquals(singleMapBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

                // Write Map's Key (long + isNull)
                BIGINT.writeLong(singleMapBlockWriter, j);
                expectedSize += 9;
                assertEquals(singleMapBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

                // Write Map<Bigint, Varchar>
                BlockBuilder innerMapWriter = singleMapBlockWriter.beginBlockEntry();
                // Opening entry, does not account for size.
                assertEquals(singleMapBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

                // Each entry is of 22 bytes, with 6 byte value.
                BIGINT.writeLong(innerMapWriter, i * 2);
                innerMapWriter.appendNull();
                expectedSize += 22;
                assertEquals(singleMapBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

                // Another entry with 22 bytes.
                BIGINT.writeLong(innerMapWriter, i * 2 + 1);
                innerMapWriter.appendNull();
                expectedSize += 22;
                assertEquals(singleMapBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

                // closing the entry increases by 5 (offset + null)
                singleMapBlockWriter.closeEntry();
                expectedSize += 5;
                assertEquals(singleMapBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);
            }
            mapBlockBuilder.closeEntry();
            assertEquals(singleMapBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);
        }
    }

    public static void throwUnsupportedOperation()
    {
        throw new UnsupportedOperationException();
    }
}
