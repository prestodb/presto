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
package io.prestosql.block;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.type.TypeRegistry;

import java.lang.reflect.Array;
import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

final class ColumnarTestUtils
{
    private static final TypeManager TYPE_MANAGER = new TypeRegistry();
    private static final BlockEncodingSerde BLOCK_ENCODING_SERDE = new BlockEncodingManager(TYPE_MANAGER);

    static {
        // associate TYPE_MANAGER with a function registry
        new FunctionRegistry(TYPE_MANAGER, new BlockEncodingManager(TYPE_MANAGER), new FeaturesConfig());
    }

    private ColumnarTestUtils() {}

    public static <T> void assertBlock(Block block, T[] expectedValues)
    {
        assertBlockPositions(block, expectedValues);
        assertBlockPositions(copyBlock(block), expectedValues);
    }

    private static <T> void assertBlockPositions(Block block, T[] expectedValues)
    {
        assertEquals(block.getPositionCount(), expectedValues.length);
        for (int position = 0; position < block.getPositionCount(); position++) {
            assertBlockPosition(block, position, expectedValues[position]);
        }
    }

    public static <T> void assertBlockPosition(Block block, int position, T expectedValue)
    {
        assertPositionValue(block, position, expectedValue);
        assertPositionValue(block.getSingleValueBlock(position), 0, expectedValue);
    }

    private static <T> void assertPositionValue(Block block, int position, T expectedValue)
    {
        if (expectedValue == null) {
            assertTrue(block.isNull(position));
            return;
        }
        assertFalse(block.isNull(position));

        if (expectedValue instanceof Slice) {
            Slice expectedSliceValue = (Slice) expectedValue;
            int length = block.getSliceLength(position);
            assertEquals(length, expectedSliceValue.length());

            Slice actual = block.getSlice(position, 0, length);
            assertEquals(actual, expectedSliceValue);
        }
        else if (expectedValue instanceof Slice[]) {
            // array or row
            Block actual = block.getObject(position, Block.class);
            assertBlock(actual, (Slice[]) expectedValue);
        }
        else if (expectedValue instanceof Slice[][]) {
            // map
            Block actual = block.getObject(position, Block.class);
            // a map is exposed as a block alternating key and value entries, so we need to flatten the expected values array
            assertBlock(actual, flattenMapEntries((Slice[][]) expectedValue));
        }
        else {
            throw new IllegalArgumentException(expectedValue.getClass().getName());
        }
    }

    private static Slice[] flattenMapEntries(Slice[][] mapEntries)
    {
        Slice[] flattened = new Slice[mapEntries.length * 2];
        for (int i = 0; i < mapEntries.length; i++) {
            Slice[] mapEntry = mapEntries[i];
            assertEquals(mapEntry.length, 2);
            flattened[i * 2] = mapEntry[0];
            flattened[i * 2 + 1] = mapEntry[1];
        }
        return flattened;
    }

    public static <T> T[] alternatingNullValues(T[] objects)
    {
        T[] objectsWithNulls = (T[]) Array.newInstance(objects.getClass().getComponentType(), objects.length * 2 + 1);
        for (int i = 0; i < objects.length; i++) {
            objectsWithNulls[i * 2] = null;
            objectsWithNulls[i * 2 + 1] = objects[i];
        }
        objectsWithNulls[objectsWithNulls.length - 1] = null;
        return objectsWithNulls;
    }

    private static Block copyBlock(Block block)
    {
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        BLOCK_ENCODING_SERDE.writeBlock(sliceOutput, block);
        return BLOCK_ENCODING_SERDE.readBlock(sliceOutput.slice().getInput());
    }

    public static DictionaryBlock createTestDictionaryBlock(Block block)
    {
        int[] dictionaryIndexes = createTestDictionaryIndexes(block.getPositionCount());
        return new DictionaryBlock(dictionaryIndexes.length, block, dictionaryIndexes);
    }

    public static <T> T[] createTestDictionaryExpectedValues(T[] expectedValues)
    {
        int[] dictionaryIndexes = createTestDictionaryIndexes(expectedValues.length);
        T[] expectedDictionaryValues = Arrays.copyOf(expectedValues, dictionaryIndexes.length);
        for (int i = 0; i < dictionaryIndexes.length; i++) {
            int dictionaryIndex = dictionaryIndexes[i];
            T expectedValue = expectedValues[dictionaryIndex];
            expectedDictionaryValues[i] = expectedValue;
        }
        return expectedDictionaryValues;
    }

    private static int[] createTestDictionaryIndexes(int valueCount)
    {
        int[] dictionaryIndexes = new int[valueCount * 2];
        for (int i = 0; i < valueCount; i++) {
            dictionaryIndexes[i] = valueCount - i - 1;
            dictionaryIndexes[i + valueCount] = i;
        }
        return dictionaryIndexes;
    }

    public static <T> T[] createTestRleExpectedValues(T[] expectedValues, int position)
    {
        T[] expectedDictionaryValues = Arrays.copyOf(expectedValues, 10);
        for (int i = 0; i < 10; i++) {
            expectedDictionaryValues[i] = expectedValues[position];
        }
        return expectedDictionaryValues;
    }

    public static RunLengthEncodedBlock createTestRleBlock(Block block, int position)
    {
        return new RunLengthEncodedBlock(block.getRegion(position, 1), 10);
    }
}
