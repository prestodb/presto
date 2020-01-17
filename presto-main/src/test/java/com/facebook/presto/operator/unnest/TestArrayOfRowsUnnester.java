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
import com.facebook.presto.spi.type.RowType;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.facebook.presto.block.ColumnarTestUtils.assertBlock;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.array;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.column;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.computeExpectedUnnestedOutput;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.createArrayBlockOfRowBlocks;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.getFieldElements;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.nullExists;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.toSlices;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.validateTestInput;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestArrayOfRowsUnnester
{
    @Test
    public void testWithoutMisAlignments()
    {
        int fieldCount = 2;
        int[] unnestedLengths = {2, 1};

        Slice[][][] elements = column(
                array(toSlices("0.0.0", "0.0.1"), toSlices("0.1.0", "0.1.1")),
                array(toSlices("1.0.0", "1.0.1")));

        // Test output building incrementally and get final output.
        Block[] blocks = testArrayOfRowsUnnester(unnestedLengths, unnestedLengths, elements, fieldCount);
        // Check final state. No misalignment, so all blocks should be DictionaryBlocks.
        assertEquals(blocks.length, 2);
        assertTrue(blocks[0] instanceof DictionaryBlock);
        assertTrue(blocks[1] instanceof DictionaryBlock);
    }

    @Test
    public void testWithMisAlignments()
    {
        int fieldCount = 2;
        int[] unnestedLengths = {2, 1};

        Slice[][][] elements = column(
                array(toSlices("0.0.0", "0.0.1"), toSlices(null, "0.1.1")),
                array(toSlices("1.0.0", "1.0.1")));

        // requiredCounts[1] causes a misalignment
        int[] requiredCounts = {2, 2};

        // Test output building incrementally and get final output blocks
        Block[] blocks = testArrayOfRowsUnnester(requiredCounts, unnestedLengths, elements, fieldCount);

        // Check final state. A misalignment has occurred.
        assertEquals(blocks.length, 2);
        // Misaligned, and has a null entry (elements[0][1][0])
        assertTrue(blocks[0] instanceof DictionaryBlock);
        // Misaligned, and does not have any null entries
        assertFalse(blocks[1] instanceof DictionaryBlock);
    }

    @Test
    public void testWithNullRowElement()
    {
        int fieldCount = 3;
        int[] requiredOutputCounts = {3, 1, 1, 0, 0};
        int[] unnestedLengths = {3, 1, 1, 0, 0};

        Slice[][][] elements = column(array(toSlices("0.0.0", "0.0.1", null), null, toSlices("0.2.0", "0.2.1", "0.2.2")),
                array(toSlices("1.0.0", "1.0.1", "1.0.2")),
                array(toSlices(null, "2.0.1", "2.0.2")),
                null,
                array());

        // Test output building incrementally and get final output blocks
        Block[] blocks = testArrayOfRowsUnnester(requiredOutputCounts, unnestedLengths, elements, fieldCount);

        // Check final state. All blocks are misaligned because of NULL row object elements[0][1].
        assertEquals(blocks.length, 3);
        // Misaligned, but has a null entry. (elements[0][0][1])
        assertTrue(blocks[0] instanceof DictionaryBlock);
        // Misaligned, and doesn't have a null entry.
        assertFalse(blocks[1] instanceof DictionaryBlock);
        // Misaligned, but has a null entry. (elements[0][0][1])
        assertTrue(blocks[2] instanceof DictionaryBlock);
    }

    /**
     * Test operations of ArrayOfRowUnnester incrementally on the input.
     * Output final blocks after the whole input has been processed.
     *
     * Input 3d array {@code elements} stores values from a column with type <array<row<varchar, varchar, ... {@code fieldCount} times> >.
     * elements[i] corresponds to a position in this column, represents one array of row(....).
     * elements[i][j] represents one row(....) object in the array.
     * elements[i][j][k] represents value of kth field in row(...) object.
     */
    private static Block[] testArrayOfRowsUnnester(int[] requiredOutputCounts, int[] unnestedLengths, Slice[][][] elements, int fieldCount)
    {
        validateTestInput(requiredOutputCounts, unnestedLengths, elements, fieldCount);

        int positionCount = requiredOutputCounts.length;

        // True if there is a null Row element inside the array at this position
        boolean[] containsNullRowElement = new boolean[positionCount];

        // Populate containsNullRowElement
        for (int i = 0; i < positionCount; i++) {
            containsNullRowElement[i] = false;
            if (elements[i] != null) {
                for (int j = 0; j < elements[i].length; j++) {
                    if (elements[i][j] == null) {
                        containsNullRowElement[i] = true;
                    }
                }
            }
        }

        // Check for null elements in individual input fields
        boolean[] nullsPresent = new boolean[fieldCount];
        for (int field = 0; field < fieldCount; field++) {
            nullsPresent[field] = nullExists(elements[field]);
        }

        // Create the unnester and input block
        RowType rowType = RowType.anonymous(Collections.nCopies(fieldCount, VARCHAR));
        Unnester arrayofRowsUnnester = new ArrayOfRowsUnnester(rowType);
        Block arrayBlockOfRows = createArrayBlockOfRowBlocks(elements, rowType);

        Block[] blocks = null;

        // Verify output being produced after processing every position. (quadratic)
        for (int inputTestCount = 1; inputTestCount <= elements.length; inputTestCount++) {
            // Reset input and prepare for new output
            PageBuilderStatus status = new PageBuilderStatus();
            arrayofRowsUnnester.resetInput(arrayBlockOfRows);
            assertEquals(arrayofRowsUnnester.getInputEntryCount(), elements.length);

            arrayofRowsUnnester.startNewOutput(status, 10);
            boolean misAligned = false;

            // Process inputTestCount positions
            for (int i = 0; i < inputTestCount; i++) {
                arrayofRowsUnnester.processCurrentAndAdvance(requiredOutputCounts[i]);
                int elementsSize = (elements[i] != null ? elements[i].length : 0);

                // Check for misalignment, caused by either
                // (1) required output count being greater than unnested length OR
                // (2) null Row element
                if ((requiredOutputCounts[i] > elementsSize) || containsNullRowElement[i]) {
                    misAligned = true;
                }
            }

            // Build output block and verify
            blocks = arrayofRowsUnnester.buildOutputBlocksAndFlush();
            assertEquals(blocks.length, rowType.getFields().size());

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
        int fieldCount = 3;
        int[] unnestedLengths = {1, 2, 1};

        Slice[][][] elements = column(
                array(toSlices("0.0.0", "0.0.1", "0.0.2")),
                array(toSlices("1.0.0", "1.0.1", "1.0.2"), toSlices("1.1.0", "1.1.1", "1.1.2")),
                array(toSlices("2.0.0", "2.0.1", "2.0.2")));

        RowType rowType = RowType.anonymous(Collections.nCopies(fieldCount, VARCHAR));
        Block arrayOfRowBlock = createArrayBlockOfRowBlocks(elements, rowType);

        // Remove the first element from the arrayBlock for testing
        int startElement = 1;

        Slice[][][] truncatedSlices = Arrays.copyOfRange(elements, startElement, elements.length - startElement + 1);
        int[] truncatedUnnestedLengths = Arrays.copyOfRange(unnestedLengths, startElement, elements.length - startElement + 1);
        Block truncatedBlock = arrayOfRowBlock.getRegion(startElement, truncatedSlices.length);

        Unnester arrayOfRowsUnnester = new ArrayOfRowsUnnester(rowType);
        arrayOfRowsUnnester.resetInput(truncatedBlock);

        arrayOfRowsUnnester.startNewOutput(new PageBuilderStatus(), 20);

        // Process all input entries in the truncated block
        for (int i = 0; i < truncatedBlock.getPositionCount(); i++) {
            arrayOfRowsUnnester.processCurrentAndAdvance(truncatedUnnestedLengths[i]);
        }

        Block[] output = arrayOfRowsUnnester.buildOutputBlocksAndFlush();
        assertEquals(Arrays.asList(truncatedSlices).stream().mapToInt(slice -> slice.length).sum(), output[0].getPositionCount());

        for (int i = 0; i < fieldCount; i++) {
            Slice[] expectedOutput = computeExpectedUnnestedOutput(getFieldElements(truncatedSlices, i), truncatedUnnestedLengths, 0, truncatedSlices.length);
            assertBlock(output[i], expectedOutput);
        }
    }
}
