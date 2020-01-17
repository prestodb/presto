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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.PageBuilderStatus;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.Arrays;

import static com.facebook.presto.block.ColumnarTestUtils.assertBlock;
import static com.facebook.presto.block.TestColumnarMap.createBlockBuilderWithValues;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.array;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.column;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.computeExpectedUnnestedOutput;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.getFieldElements;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.nullExists;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.toSlices;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.validateTestInput;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMapUnnester
{
    @Test
    public void testSimple()
    {
        int[] unnestedLengths = {1, 2, 0};
        int[] requiredOutputCounts = unnestedLengths;

        Slice[][][] elements = column(
                array(toSlices("0.0.0", "0.0.1")),
                array(toSlices("1.0.0", "1.0.1"), toSlices("1.1.0", "1.1.1")),
                null);

        Block[] blocks = testMapUnnester(unnestedLengths, requiredOutputCounts, elements);

        // Check final state. Both the blocks should be dictionary blocks, since there is no misalignment.
        assertEquals(blocks.length, 2);
        assertTrue(blocks[0] instanceof DictionaryBlock);
        assertTrue(blocks[1] instanceof DictionaryBlock);
    }

    @Test
    public void testMisaligned()
    {
        int[] unnestedLengths = {1, 2, 0, 0};
        int[] requiredOutputCounts = {1, 3, 0, 1};

        Slice[][][] elements = column(
                array(toSlices("0.0.0", "0.0.1")),
                array(toSlices("1.0.0", "1.0.1"), toSlices("1.1.0", null)),
                null,
                null);

        Block[] blocks = testMapUnnester(requiredOutputCounts, unnestedLengths, elements);

        // Check final state. Misalignment has occurred.
        assertEquals(blocks.length, 2);
        // Misaligned, does not have a null entry.
        assertFalse(blocks[0] instanceof DictionaryBlock);
        // Misaligned, but has a null entry elements[1][1][1]
        assertTrue(blocks[1] instanceof DictionaryBlock);
    }

    private static Block[] testMapUnnester(int[] requiredOutputCounts, int[] unnestedLengths, Slice[][][] elements)
    {
        validateTestInput(requiredOutputCounts, unnestedLengths, elements, 2);
        int positionCount = unnestedLengths.length;

        for (int index = 0; index < positionCount; index++) {
            if (elements[index] != null) {
                for (int i = 0; i < elements[index].length; i++) {
                    assertTrue(elements[index][i] != null, "entry cannot be null");
                    assertEquals(elements[index][i].length, 2);
                }
            }
        }

        // Check for null elements in individual input fields
        boolean[] nullsPresent = new boolean[2];
        nullsPresent[0] = nullExists(elements[0]);
        nullsPresent[1] = nullExists(elements[1]);
        assertFalse(nullsPresent[0]);

        // Create the unnester and input block
        Unnester mapUnnester = new MapUnnester(VARCHAR, VARCHAR);
        Block mapBlock = createBlockBuilderWithValues(elements).build();

        Block[] blocks = null;

        // Verify output being produced after processing every position. (quadratic)
        for (int inputTestCount = 1; inputTestCount <= elements.length; inputTestCount++) {
            // Reset input and prepare for new output
            PageBuilderStatus status = new PageBuilderStatus();
            mapUnnester.resetInput(mapBlock);
            assertEquals(mapUnnester.getInputEntryCount(), elements.length);

            mapUnnester.startNewOutput(status, 10);
            boolean misAligned = false;

            // Process inputTestCount positions
            for (int i = 0; i < inputTestCount; i++) {
                mapUnnester.processCurrentAndAdvance(requiredOutputCounts[i]);
                int elementsSize = (elements[i] != null ? elements[i].length : 0);

                // Check for misalignment, caused by required output count being greater than unnested length
                if ((requiredOutputCounts[i] > elementsSize)) {
                    misAligned = true;
                }
            }

            // Build output
            blocks = mapUnnester.buildOutputBlocksAndFlush();
            assertEquals(blocks.length, 2);

            // Verify output blocks for individual fields
            for (int field = 0; field < blocks.length; field++) {
                assertTrue((blocks[field] instanceof DictionaryBlock) || (!nullsPresent[field] && misAligned));
                assertFalse((blocks[field] instanceof DictionaryBlock) && (!nullsPresent[field] && misAligned));
                Slice[][] fieldElements = getFieldElements(elements, field);
                Slice[] expectedOutput = computeExpectedUnnestedOutput(fieldElements, requiredOutputCounts, 0, inputTestCount);
                assertBlock(blocks[field], expectedOutput);
            }
        }

        return blocks;
    }

    @Test
    public void testTrimmedBlocks()
    {
        int[] unnestedLengths = {1, 2, 1};

        Slice[][][] elements = column(
                array(toSlices("0.0.0", "0.0.1")),
                array(toSlices("1.0.0", "1.0.1"), toSlices("1.1.0", "1.1.1")),
                array(toSlices("2.0.0", "2.0.1")));

        Block mapBlock = createBlockBuilderWithValues(elements).build();

        // Remove the first element from the arrayBlock for testing
        int startElement = 1;

        Slice[][][] truncatedSlices = Arrays.copyOfRange(elements, startElement, elements.length - startElement + 1);
        int[] truncatedUnnestedLengths = Arrays.copyOfRange(unnestedLengths, startElement, elements.length - startElement + 1);
        Block truncatedBlock = mapBlock.getRegion(startElement, elements.length - startElement);
        assertBlock(truncatedBlock, truncatedSlices);

        Unnester mapUnnester = new MapUnnester(VARCHAR, VARCHAR);
        mapUnnester.resetInput(truncatedBlock);

        mapUnnester.startNewOutput(new PageBuilderStatus(), 20);

        // Process all input entries in the truncated block
        for (int i = 0; i < truncatedBlock.getPositionCount(); i++) {
            mapUnnester.processCurrentAndAdvance(truncatedUnnestedLengths[i]);
        }

        Block[] output = mapUnnester.buildOutputBlocksAndFlush();
        assertEquals(Arrays.asList(truncatedSlices).stream().mapToInt(slice -> slice.length).sum(), output[0].getPositionCount());

        Slice[] expectedOutput0 = computeExpectedUnnestedOutput(getFieldElements(truncatedSlices, 0), truncatedUnnestedLengths, 0, truncatedSlices.length);
        assertBlock(output[0], expectedOutput0);

        Slice[] expectedOutput1 = computeExpectedUnnestedOutput(getFieldElements(truncatedSlices, 1), truncatedUnnestedLengths, 0, truncatedSlices.length);
        assertBlock(output[1], expectedOutput1);
    }
}
