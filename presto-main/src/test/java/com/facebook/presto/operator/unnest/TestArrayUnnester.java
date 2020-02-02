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
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.array;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.column;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.computeExpectedUnnestedOutput;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.createArrayBlock;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.getFieldElements;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.nullExists;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.toSlices;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.validateTestInput;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestArrayUnnester
{
    @Test
    public void testWithNullElement()
    {
        int[] unnestedLengths = {2, 1, 0, 1, 0};
        int[] requiredOutputCounts = {2, 2, 1, 1, 1};

        Slice[][][] elements = column(
                array(toSlices("0.0.0"), toSlices("0.1.0")),
                array(toSlices("1.0.0")),
                null,
                array(toSlices(null)),
                array());

        Block[] blocks = testArrayUnnester(requiredOutputCounts, unnestedLengths, elements);
        // Misalignment occurs, but a null element exists.
        assertTrue(blocks[0] instanceof DictionaryBlock);
    }

    @Test
    public void testWithoutNullElement()
    {
        int[] unnestedLengths = {1, 1, 1, 0, 0};
        int[] requiredOutputCounts = {2, 2, 1, 1, 1};

        Slice[][][] elements = column(
                array(toSlices("0.0.0")),
                array(toSlices("1.0.0")),
                array(toSlices("2.0.0")),
                null,
                array());

        Block[] blocks = testArrayUnnester(requiredOutputCounts, unnestedLengths, elements);
        // Misalignment occurs, and there is no null element
        assertFalse(blocks[0] instanceof DictionaryBlock);
    }

    private static Block[] testArrayUnnester(int[] requiredOutputCounts, int[] unnestedLengths, Slice[][][] elements)
    {
        Slice[][] slices = getFieldElements(elements, 0);
        validateTestInput(requiredOutputCounts, unnestedLengths, elements, 1);

        // Check if there is a null element in the input
        boolean nullPresent = nullExists(slices);

        // Initialize unnester
        Unnester arrayUnnester = new ArrayUnnester(VARCHAR);
        Block arrayBlock = createArrayBlock(slices);

        Block[] blocks = null;

        // Verify output being produced after processing every position. (quadratic)
        for (int inputTestCount = 1; inputTestCount <= elements.length; inputTestCount++) {
            // Reset input
            arrayUnnester.resetInput(arrayBlock);

            // Prepare for new output
            PageBuilderStatus status = new PageBuilderStatus();
            arrayUnnester.startNewOutput(status, 10);
            boolean misAligned = false;

            // Process inputTestCount positions
            for (int i = 0; i < inputTestCount; i++) {
                int elementsSize = (elements[i] != null ? elements[i].length : 0);
                assertEquals(arrayUnnester.getCurrentUnnestedLength(), elementsSize);
                arrayUnnester.processCurrentAndAdvance(requiredOutputCounts[i]);

                if (requiredOutputCounts[i] > elementsSize) {
                    misAligned = true;
                }
            }

            // Verify output
            blocks = arrayUnnester.buildOutputBlocksAndFlush();
            assertEquals(blocks.length, 1);
            assertTrue((blocks[0] instanceof DictionaryBlock) || (!nullPresent && misAligned));
            assertFalse((blocks[0] instanceof DictionaryBlock) && (!nullPresent && misAligned));
            Slice[] expectedOutput = computeExpectedUnnestedOutput(slices, requiredOutputCounts, 0, inputTestCount);
            assertBlock(blocks[0], expectedOutput);
        }

        return blocks;
    }

    @Test
    public void testTrimmedBlocks()
    {
        int[] unnestedLengths = {2, 1, 2, 3, 1};

        Slice[][][] elements = column(
                array(toSlices("0.0.0"), toSlices("0.1.0")),
                array(toSlices("1.0.0")),
                array(toSlices("2.0.0"), toSlices("2.1.0")),
                array(toSlices("3.0.0"), toSlices("3.1.0"), toSlices("3.2.0")),
                array(toSlices("4.0.0")));

        Slice[][] slices = getFieldElements(elements, 0);
        Block arrayBlock = createArrayBlock(slices);

        // Remove the first element from the arrayBlock for testing
        int startElement = 1;

        Slice[][] truncatedSlices = Arrays.copyOfRange(slices, startElement, slices.length - startElement + 1);
        int[] truncatedUnnestedLengths = Arrays.copyOfRange(unnestedLengths, startElement, slices.length - startElement + 1);
        Block truncatedBlock = arrayBlock.getRegion(startElement, truncatedSlices.length);
        assertBlock(truncatedBlock, truncatedSlices);

        Unnester arrayUnnester = new ArrayUnnester(VARCHAR);
        arrayUnnester.resetInput(truncatedBlock);

        arrayUnnester.startNewOutput(new PageBuilderStatus(), 20);

        // Process all input entries in the truncated block
        for (int i = 0; i < truncatedBlock.getPositionCount(); i++) {
            arrayUnnester.processCurrentAndAdvance(truncatedUnnestedLengths[i]);
        }

        Block[] output = arrayUnnester.buildOutputBlocksAndFlush();
        assertEquals(Arrays.asList(truncatedSlices).stream().mapToInt(slice -> slice.length).sum(), output[0].getPositionCount());

        Slice[] expectedOutput = computeExpectedUnnestedOutput(truncatedSlices, truncatedUnnestedLengths, 0, truncatedSlices.length);
        assertBlock(output[0], expectedOutput);
    }
}
