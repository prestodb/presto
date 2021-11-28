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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.ByteArrayBlock;
import com.facebook.presto.common.block.RowBlockBuilder;
import com.facebook.presto.common.block.SingleRowBlock;
import com.facebook.presto.common.type.Type;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.block.BlockAssertions.createLongDictionaryBlock;
import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.block.BlockAssertions.createRandomDictionaryBlock;
import static com.facebook.presto.block.BlockAssertions.createRandomLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createRleBlockWithRandomValue;
import static com.facebook.presto.common.block.RowBlock.fromFieldBlocks;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Fail.fail;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestRowBlock
        extends AbstractTestBlock
{
    @Test
    void testWithVarcharBigint()
    {
        List<Type> fieldTypes = ImmutableList.of(VARCHAR, BIGINT);
        List<Object>[] testRows = generateTestRows(fieldTypes, 100);

        testWith(fieldTypes, testRows);
        testWith(fieldTypes, alternatingNullValues(testRows));
    }

    @Test
    public void testEstimatedDataSizeForStats()
    {
        List<Type> fieldTypes = ImmutableList.of(VARCHAR, BIGINT);
        List<Object>[] expectedValues = alternatingNullValues(generateTestRows(fieldTypes, 100));
        BlockBuilder blockBuilder = createBlockBuilderWithValues(fieldTypes, expectedValues);
        Block block = blockBuilder.build();
        assertEquals(block.getPositionCount(), expectedValues.length);
        for (int i = 0; i < block.getPositionCount(); i++) {
            int expectedSize = getExpectedEstimatedDataSize(expectedValues[i]);
            assertEquals(blockBuilder.getEstimatedDataSizeForStats(i), expectedSize);
            assertEquals(block.getEstimatedDataSizeForStats(i), expectedSize);
        }
    }

    @Test
    public void testFromFieldBlocksNoNullsDetection()
    {
        Block emptyBlock = new ByteArrayBlock(0, Optional.empty(), new byte[0]);
        Block fieldBlock = new ByteArrayBlock(5, Optional.empty(), createExpectedValue(5).getBytes());

        boolean[] rowIsNull = new boolean[fieldBlock.getPositionCount()];
        Arrays.fill(rowIsNull, false);

        // Blocks may discard the null mask during creation if no values are null
        assertFalse(fromFieldBlocks(5, Optional.of(rowIsNull), new Block[]{fieldBlock}).mayHaveNull());
        // Last position is null must retain the nulls mask
        rowIsNull[rowIsNull.length - 1] = true;
        assertTrue(fromFieldBlocks(5, Optional.of(rowIsNull), new Block[]{fieldBlock}).mayHaveNull());
        // Empty blocks have no nulls and can also discard their null mask
        assertFalse(fromFieldBlocks(0, Optional.of(new boolean[0]), new Block[]{emptyBlock}).mayHaveNull());

        // Normal blocks should have null masks preserved
        List<Type> fieldTypes = ImmutableList.of(VARCHAR, BIGINT);
        Block hasNullsBlock = createBlockBuilderWithValues(fieldTypes, alternatingNullValues(generateTestRows(fieldTypes, 100))).build();
        assertTrue(hasNullsBlock.mayHaveNull());
    }

    private int getExpectedEstimatedDataSize(List<Object> row)
    {
        if (row == null) {
            return 0;
        }
        int size = 0;
        size += row.get(0) == null ? 0 : ((String) row.get(0)).length();
        size += row.get(1) == null ? 0 : Long.BYTES;
        return size;
    }

    @Test
    public void testCompactBlock()
    {
        Block emptyBlock = new ByteArrayBlock(0, Optional.empty(), new byte[0]);
        Block compactFieldBlock1 = new ByteArrayBlock(5, Optional.empty(), createExpectedValue(5).getBytes());
        Block compactFieldBlock2 = new ByteArrayBlock(5, Optional.empty(), createExpectedValue(5).getBytes());
        Block incompactFiledBlock1 = new ByteArrayBlock(5, Optional.empty(), createExpectedValue(6).getBytes());
        Block incompactFiledBlock2 = new ByteArrayBlock(5, Optional.empty(), createExpectedValue(6).getBytes());
        boolean[] rowIsNull = {false, true, false, false, false, false};

        assertCompact(fromFieldBlocks(0, Optional.empty(), new Block[] {emptyBlock, emptyBlock}));
        assertCompact(fromFieldBlocks(rowIsNull.length, Optional.of(rowIsNull), new Block[] {compactFieldBlock1, compactFieldBlock2}));
        // TODO: add test case for a sliced RowBlock

        // underlying field blocks are not compact
        testIncompactBlock(fromFieldBlocks(rowIsNull.length, Optional.of(rowIsNull), new Block[] {incompactFiledBlock1, incompactFiledBlock2}));
        testIncompactBlock(fromFieldBlocks(rowIsNull.length, Optional.of(rowIsNull), new Block[] {incompactFiledBlock1, incompactFiledBlock2}));
    }

    @Test
    public void testLogicalSizeInBytes()
    {
        int positionCount = 100;
        int[] offsets = IntStream.rangeClosed(0, positionCount).toArray();
        boolean[] nulls = new boolean[positionCount];

        // Row(LongArrayBlock, LongArrayBlock)
        Block fieldBlock11 = createRandomLongsBlock(positionCount, 0);
        Block fieldBlock12 = createRandomLongsBlock(positionCount, 0);
        Block rowOfLongAndLong = fromFieldBlocks(positionCount, Optional.of(nulls), new Block[] {fieldBlock11, fieldBlock12});
        assertEquals(rowOfLongAndLong.getLogicalSizeInBytes(), 2300);

        // Row(RLE(LongArrayBlock), RLE(LongArrayBlock))
        Block fieldBlock21 = createRLEBlock(1, positionCount);
        Block fieldBlock22 = createRLEBlock(2, positionCount);
        Block rowOfRleOfLongAndRleOfLong = fromFieldBlocks(positionCount, Optional.of(nulls), new Block[] {fieldBlock21, fieldBlock22});
        assertEquals(rowOfRleOfLongAndRleOfLong.getLogicalSizeInBytes(), 2300);

        // Row(RLE(LongArrayBlock), RLE(Row(LongArrayBlock, LongArrayBlock)))
        Block fieldBlock31 = createRLEBlock(1, positionCount);
        Block fieldBlock32 = createRleBlockWithRandomValue(fromFieldBlocks(positionCount, Optional.of(nulls), new Block[] {createRandomLongsBlock(positionCount, 0),
                createRandomLongsBlock(positionCount, 0)}), positionCount); //createRleBlockWithRandomValue(fromElementBlock(positionCount, Optional.of(nulls), arrayOffsets, createRandomLongsBlock(positionCount * 2, 0)), positionCount);
        Block rowOfRleOfLongAndRleOfRowOfLongAndLong = fromFieldBlocks(positionCount, Optional.of(nulls), new Block[] {fieldBlock31, fieldBlock32});
        assertEquals(rowOfRleOfLongAndRleOfRowOfLongAndLong.getLogicalSizeInBytes(), 3700);

        // Row(Dictionary(LongArrayBlock), Dictionary(LongArrayBlock))
        Block fieldBlock41 = createLongDictionaryBlock(0, positionCount);
        Block fieldBlock42 = createLongDictionaryBlock(0, positionCount);
        Block rowOfDictionaryOfLongAndDictionaryOfLong = fromFieldBlocks(positionCount, Optional.of(nulls), new Block[] {fieldBlock41, fieldBlock42});
        assertEquals(rowOfDictionaryOfLongAndDictionaryOfLong.getLogicalSizeInBytes(), 2300);

        // Row(Dictionary(LongArrayBlock), Dictionary(Row(LongArrayBlock, LongArrayBlock)))
        Block fieldBlock51 = createLongDictionaryBlock(1, positionCount);
        Block fieldBlock52 = createRandomDictionaryBlock(fromFieldBlocks(positionCount, Optional.of(nulls), new Block[] {createRandomLongsBlock(positionCount, 0),
                createRandomLongsBlock(positionCount, 0)}), positionCount, false); //createRleBlockWithRandomValue(fromElementBlock(positionCount, Optional.of(nulls), arrayOffsets, createRandomLongsBlock(positionCount * 2, 0)), positionCount);
        Block rowOfDictionaryOfLongAndDictionaryOfRowOfLongAndLong = fromFieldBlocks(positionCount, Optional.of(nulls), new Block[] {fieldBlock51, fieldBlock52});
        assertEquals(rowOfDictionaryOfLongAndDictionaryOfRowOfLongAndLong.getLogicalSizeInBytes(), 3700);
    }

    private void testWith(List<Type> fieldTypes, List<Object>[] expectedValues)
    {
        BlockBuilder blockBuilder = createBlockBuilderWithValues(fieldTypes, expectedValues);

        assertBlock(blockBuilder, () -> blockBuilder.newBlockBuilderLike(null), expectedValues);
        assertBlock(blockBuilder.build(), () -> blockBuilder.newBlockBuilderLike(null), expectedValues);

        IntArrayList positionList = generatePositionList(expectedValues.length, expectedValues.length / 2);
        assertBlockFilteredPositions(expectedValues, blockBuilder, () -> blockBuilder.newBlockBuilderLike(null), positionList.toIntArray());
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), () -> blockBuilder.newBlockBuilderLike(null), positionList.toIntArray());
    }

    private BlockBuilder createBlockBuilderWithValues(List<Type> fieldTypes, List<Object>[] rows)
    {
        BlockBuilder rowBlockBuilder = new RowBlockBuilder(fieldTypes, null, 1);
        for (List<Object> row : rows) {
            if (row == null) {
                rowBlockBuilder.appendNull();
            }
            else {
                BlockBuilder singleRowBlockWriter = rowBlockBuilder.beginBlockEntry();
                for (Object fieldValue : row) {
                    if (fieldValue == null) {
                        singleRowBlockWriter.appendNull();
                    }
                    else {
                        if (fieldValue instanceof Long) {
                            BIGINT.writeLong(singleRowBlockWriter, ((Long) fieldValue).longValue());
                        }
                        else if (fieldValue instanceof String) {
                            VARCHAR.writeSlice(singleRowBlockWriter, utf8Slice((String) fieldValue));
                        }
                        else {
                            throw new IllegalArgumentException();
                        }
                    }
                }
                rowBlockBuilder.closeEntry();
            }
        }

        return rowBlockBuilder;
    }

    @Override
    protected <T> void assertCheckedPositionValue(Block block, int position, T expectedValue)
    {
        if (expectedValue instanceof List) {
            assertValue(block, position, (List<Object>) expectedValue);
            return;
        }
        super.assertCheckedPositionValue(block, position, expectedValue);
    }

    @Override
    protected <T> void assertPositionValueUnchecked(Block block, int internalPosition, T expectedValue)
    {
        if (expectedValue instanceof List) {
            assertValueUnchecked(block, internalPosition, (List<Object>) expectedValue);
            return;
        }
        super.assertPositionValueUnchecked(block, internalPosition, expectedValue);
    }

    private void assertValue(Block rowBlock, int position, List<Object> row)
    {
        // null rows are handled by assertPositionValue
        requireNonNull(row, "row is null");

        assertFalse(rowBlock.isNull(position));
        SingleRowBlock singleRowBlock = (SingleRowBlock) rowBlock.getBlock(position);
        assertEquals(singleRowBlock.getPositionCount(), row.size());

        for (int i = 0; i < row.size(); i++) {
            Object fieldValue = row.get(i);
            if (fieldValue == null) {
                assertTrue(singleRowBlock.isNull(i));
            }
            else {
                if (fieldValue instanceof Long) {
                    assertEquals(BIGINT.getLong(singleRowBlock, i), ((Long) fieldValue).longValue());
                }
                else if (fieldValue instanceof String) {
                    assertEquals(VARCHAR.getSlice(singleRowBlock, i), utf8Slice((String) fieldValue));
                }
                else {
                    throw new IllegalArgumentException();
                }
            }
        }
    }

    private void assertValueUnchecked(Block rowBlock, int internalPosition, List<Object> row)
    {
        // null rows are handled by assertPositionValue
        requireNonNull(row, "row is null");

        assertFalse(rowBlock.mayHaveNull() && rowBlock.isNullUnchecked(internalPosition));
        SingleRowBlock singleRowBlock = (SingleRowBlock) rowBlock.getBlockUnchecked(internalPosition);
        assertEquals(singleRowBlock.getPositionCount(), row.size());

        for (int i = 0; i < row.size(); i++) {
            Object fieldValue = row.get(i);
            if (fieldValue == null) {
                assertTrue(singleRowBlock.isNullUnchecked(i + singleRowBlock.getOffsetBase()));
            }
            else {
                if (fieldValue instanceof Long) {
                    assertEquals(BIGINT.getLongUnchecked(singleRowBlock, i + singleRowBlock.getOffsetBase()), ((Long) fieldValue).longValue());
                }
                else if (fieldValue instanceof String) {
                    assertEquals(VARCHAR.getSliceUnchecked(singleRowBlock, i + singleRowBlock.getOffsetBase()), utf8Slice((String) fieldValue));
                }
                else {
                    fail("Unexpected type: " + fieldValue.getClass().getSimpleName());
                }
            }
        }
    }

    private List<Object>[] generateTestRows(List<Type> fieldTypes, int numRows)
    {
        List<Object>[] testRows = new List[numRows];
        for (int i = 0; i < numRows; i++) {
            List<Object> testRow = new ArrayList<>(fieldTypes.size());
            for (int j = 0; j < fieldTypes.size(); j++) {
                int cellId = i * fieldTypes.size() + j;
                if (cellId % 7 == 3) {
                    // Put null value for every 7 cells
                    testRow.add(null);
                }
                else {
                    if (fieldTypes.get(j) == BIGINT) {
                        testRow.add(i * 100L + j);
                    }
                    else if (fieldTypes.get(j) == VARCHAR) {
                        testRow.add(format("field(%s, %s)", i, j));
                    }
                    else {
                        throw new IllegalArgumentException();
                    }
                }
            }
            testRows[i] = testRow;
        }
        return testRows;
    }

    private IntArrayList generatePositionList(int numRows, int numPositions)
    {
        IntArrayList positions = new IntArrayList(numPositions);
        for (int i = 0; i < numPositions; i++) {
            positions.add((7 * i + 3) % numRows);
        }
        Collections.sort(positions);
        return positions;
    }
}
