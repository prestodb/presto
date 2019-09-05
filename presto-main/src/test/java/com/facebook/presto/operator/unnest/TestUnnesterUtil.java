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
package com.facebook.presto.operator.unnest;

import com.facebook.presto.spi.block.ArrayBlockBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.RowType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestUnnesterUtil
{
    private TestUnnesterUtil() {}

    public static Block createSimpleBlock(Slice[] values)
    {
        BlockBuilder elementBlockBuilder = VARCHAR.createBlockBuilder(null, values.length);
        for (Slice v : values) {
            if (v == null) {
                elementBlockBuilder.appendNull();
            }
            else {
                VARCHAR.writeSlice(elementBlockBuilder, v);
            }
        }
        return elementBlockBuilder.build();
    }

    public static Block createArrayBlock(Slice[][] values)
    {
        BlockBuilder blockBuilder = new ArrayBlockBuilder(VARCHAR, null, 100, 100);
        for (Slice[] expectedValue : values) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                Block elementBlock = createSimpleBlock(expectedValue);
                blockBuilder.appendStructure(elementBlock);
            }
        }
        return blockBuilder.build();
    }

    public static Block createArrayBlockOfRowBlocks(Slice[][][] elements, RowType rowType)
    {
        BlockBuilder arrayBlockBuilder = new ArrayBlockBuilder(rowType, null, 100, 100);
        for (int i = 0; i < elements.length; i++) {
            if (elements[i] == null) {
                arrayBlockBuilder.appendNull();
            }
            else {
                Slice[][] expectedValues = elements[i];
                BlockBuilder elementBlockBuilder = rowType.createBlockBuilder(null, elements[i].length);
                for (Slice[] expectedValue : expectedValues) {
                    if (expectedValue == null) {
                        elementBlockBuilder.appendNull();
                    }
                    else {
                        BlockBuilder entryBuilder = elementBlockBuilder.beginBlockEntry();
                        for (Slice v : expectedValue) {
                            if (v == null) {
                                entryBuilder.appendNull();
                            }
                            else {
                                VARCHAR.writeSlice(entryBuilder, v);
                            }
                        }
                        elementBlockBuilder.closeEntry();
                    }
                }
                arrayBlockBuilder.appendStructure(elementBlockBuilder.build());
            }
        }
        return arrayBlockBuilder.build();
    }

    public static boolean nullExists(Slice[][] elements)
    {
        for (int i = 0; i < elements.length; i++) {
            if (elements[i] != null) {
                for (int j = 0; j < elements[i].length; j++) {
                    if (elements[i][j] == null) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public static Slice[] computeExpectedUnnestedOutput(Slice[][] elements, int[] requiredOutputCounts, int startPosition, int length)
    {
        checkArgument(startPosition >= 0 && length >= 0);
        checkArgument(startPosition + length - 1 < requiredOutputCounts.length);
        checkArgument(elements.length == requiredOutputCounts.length);

        int outputCount = 0;
        for (int i = 0; i < length; i++) {
            int position = startPosition + i;
            int arrayLength = elements[position] == null ? 0 : elements[position].length;
            checkArgument(requiredOutputCounts[position] >= arrayLength);
            outputCount += requiredOutputCounts[position];
        }

        Slice[] expectedOutput = new Slice[outputCount];
        int offset = 0;

        for (int i = 0; i < length; i++) {
            int position = startPosition + i;
            int arrayLength = elements[position] == null ? 0 : elements[position].length;

            int requiredCount = requiredOutputCounts[position];

            for (int j = 0; j < arrayLength; j++) {
                expectedOutput[offset++] = elements[position][j];
            }
            for (int j = 0; j < (requiredCount - arrayLength); j++) {
                expectedOutput[offset++] = null;
            }
        }

        return expectedOutput;
    }

    /**
     * Extract elements corresponding to a specific field from 3D slices
     */
    public static Slice[][] getFieldElements(Slice[][][] slices, int fieldNo)
    {
        Slice[][] output = new Slice[slices.length][];

        for (int i = 0; i < slices.length; i++) {
            if (slices[i] != null) {
                output[i] = new Slice[slices[i].length];

                for (int j = 0; j < slices[i].length; j++) {
                    if (slices[i][j] != null) {
                        output[i][j] = slices[i][j][fieldNo];
                    }
                    else {
                        output[i][j] = null;
                    }
                }
            }
            else {
                output[i] = null;
            }
        }

        return output;
    }

    public static void validateTestInput(int[] requiredOutputCounts, int[] unnestedLengths, Slice[][][] slices, int fieldCount)
    {
        requireNonNull(requiredOutputCounts, "output counts array is null");
        requireNonNull(unnestedLengths, "unnested lengths is null");
        requireNonNull(slices, "slices array is null");

        // verify lengths
        int positionCount = slices.length;
        assertEquals(requiredOutputCounts.length, positionCount);
        assertEquals(unnestedLengths.length, positionCount);

        // Unnested array lengths must be <= required output count
        for (int i = 0; i < requiredOutputCounts.length; i++) {
            assertTrue(unnestedLengths[i] <= requiredOutputCounts[i]);
        }

        // Elements should have the right shape for every field
        for (int index = 0; index < positionCount; index++) {
            Slice[][] entry = slices[index];

            int entryLength = entry != null ? entry.length : 0;
            assertEquals(entryLength, unnestedLengths[index]);

            // Verify number of fields
            for (int i = 0; i < entryLength; i++) {
                if (entry[i] != null) {
                    assertEquals(entry[i].length, fieldCount);
                }
            }
        }
    }

    public static Slice[] createReplicatedOutputSlice(Slice[] input, int[] counts)
    {
        assertEquals(input.length, counts.length);

        int outputLength = 0;
        for (int i = 0; i < input.length; i++) {
            outputLength += counts[i];
        }

        Slice[] output = new Slice[outputLength];
        int offset = 0;
        for (int i = 0; i < input.length; i++) {
            for (int j = 0; j < counts[i]; j++) {
                output[offset++] = input[i];
            }
        }

        return output;
    }

    static Slice[][][] column(Slice[][]... arraysOfRow)
    {
        return arraysOfRow;
    }

    static Slice[][] array(Slice[]... rows)
    {
        return rows;
    }

    static Slice[] toSlices(String... values)
    {
        if (values == null) {
            return null;
        }

        Slice[] slices = new Slice[values.length];
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                slices[i] = Slices.utf8Slice(values[i]);
            }
        }

        return slices;
    }
}
