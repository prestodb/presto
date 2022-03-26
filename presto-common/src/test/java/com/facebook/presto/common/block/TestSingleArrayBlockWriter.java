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

public class TestSingleArrayBlockWriter
{
    private static final int ARRAY_POSITIONS = 100;

    @Test
    public void testSizeInBytes()
    {
        MapType mapType = new MapType(
                BIGINT,
                VARCHAR,
                MethodHandleUtil.methodHandle(TestSingleArrayBlockWriter.class, "throwUnsupportedOperation"),
                MethodHandleUtil.methodHandle(TestSingleArrayBlockWriter.class, "throwUnsupportedOperation"));

        ArrayBlockBuilder arrayBlockBuilder = new ArrayBlockBuilder(mapType, null, ARRAY_POSITIONS);

        for (int i = 0; i < ARRAY_POSITIONS; i++) {
            SingleArrayBlockWriter singleArrayBlockWriter = arrayBlockBuilder.beginBlockEntry();
            int expectedSize = 0;
            for (int j = 0; j < 10; j++) {
                assertEquals(singleArrayBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

                // Write Map<Bigint, Varchar>
                BlockBuilder innerMapWriter = singleArrayBlockWriter.beginBlockEntry();
                // Opening entry, does not account for size.
                assertEquals(singleArrayBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

                // Each entry is of 28 bytes, with 6 byte value.
                BIGINT.writeLong(innerMapWriter, i * 2);
                VARCHAR.writeSlice(innerMapWriter, utf8Slice("Value1"));
                expectedSize += 28;
                assertEquals(singleArrayBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

                // Another entry with 28 bytes.
                BIGINT.writeLong(innerMapWriter, i * 2 + 1);
                VARCHAR.writeSlice(innerMapWriter, utf8Slice("Value2"));
                expectedSize += 28;
                assertEquals(singleArrayBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

                // closing the entry increases by 5 (offset + null)
                singleArrayBlockWriter.closeEntry();
                expectedSize += 5;
                assertEquals(singleArrayBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);
            }

            singleArrayBlockWriter.appendNull();
            expectedSize += 5;
            assertEquals(singleArrayBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

            arrayBlockBuilder.closeEntry();
            assertEquals(singleArrayBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);
        }
    }

    @Test
    public void testSizeInBytesForNulls()
    {
        MapType mapType = new MapType(
                BIGINT,
                VARCHAR,
                MethodHandleUtil.methodHandle(TestSingleArrayBlockWriter.class, "throwUnsupportedOperation"),
                MethodHandleUtil.methodHandle(TestSingleArrayBlockWriter.class, "throwUnsupportedOperation"));

        ArrayBlockBuilder arrayBlockBuilder = new ArrayBlockBuilder(mapType, null, ARRAY_POSITIONS);

        for (int i = 0; i < ARRAY_POSITIONS; i++) {
            SingleArrayBlockWriter singleArrayBlockWriter = arrayBlockBuilder.beginBlockEntry();
            int expectedSize = 0;
            for (int j = 0; j < 10; j++) {
                assertEquals(singleArrayBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);

                singleArrayBlockWriter.appendNull();
                expectedSize += 5;
                assertEquals(singleArrayBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);
            }

            arrayBlockBuilder.closeEntry();
            assertEquals(singleArrayBlockWriter.getSizeInBytes(), expectedSize, "For Index: " + i);
        }
    }

    public static void throwUnsupportedOperation()
    {
        throw new UnsupportedOperationException();
    }
}
