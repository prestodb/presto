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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.ByteArrayBlock;
import com.facebook.presto.spi.block.RowBlock;
import com.facebook.presto.spi.block.RowBlockBuilder;
import com.facebook.presto.spi.block.SingleRowBlock;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
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
        testWith(fieldTypes, (List<Object>[]) alternatingNullValues(testRows));
    }

    public void testCompactBlock()
    {
        // Test Constructor
        Block emptyBlock = new ByteArrayBlock(0, new boolean[0], new byte[0]);
        Block fieldBlock1 = new ByteArrayBlock(16, new boolean[16], createExpectedValue(16).getBytes());
        Block fieldBlock2 = new ByteArrayBlock(16, new boolean[16], createExpectedValue(16).getBytes());
        Block largerFieldBlock1 = new ByteArrayBlock(20, new boolean[20], createExpectedValue(20).getBytes());
        Block largerFieldBlock2 = new ByteArrayBlock(20, new boolean[20], createExpectedValue(20).getBytes());
        int[] offsets = {0, 1, 1, 2, 4, 8, 16};
        boolean[] valueIsNull = {false, true, false, false, false, false};

        assertCompact(new RowBlock(0, 0, new boolean[0], new int[1], new Block[] {emptyBlock, emptyBlock}));
        assertCompact(new RowBlock(0, valueIsNull.length, valueIsNull, offsets, new Block[] {fieldBlock1, fieldBlock2}));

        assertNotCompact(new RowBlock(0, valueIsNull.length - 1, valueIsNull, offsets, new Block[] {fieldBlock1, fieldBlock2}));
        assertNotCompact(new RowBlock(1, valueIsNull.length - 1, valueIsNull, offsets, new Block[] {fieldBlock1, fieldBlock2}));
        assertNotCompact(new RowBlock(0, valueIsNull.length, valueIsNull, offsets, new Block[] {largerFieldBlock1, largerFieldBlock2}));
        assertNotCompact(new RowBlock(
                0,
                valueIsNull.length,
                valueIsNull, offsets,
                new Block[] {largerFieldBlock1.getRegion(0, 16), largerFieldBlock2.getRegion(0, 16)}));

        // Test getRegion and copyRegion
        Block block = new RowBlock(0, valueIsNull.length, valueIsNull, offsets, new Block[] {fieldBlock1, fieldBlock2});
        assertGetRegionCompactness(block);
        assertCopyRegionCompactness(block);
        assertCopyRegionCompactness(new RowBlock(0, valueIsNull.length, valueIsNull, offsets, new Block[] {largerFieldBlock1, largerFieldBlock2}));
        assertCopyRegionCompactness(new RowBlock(
                0,
                valueIsNull.length,
                valueIsNull, offsets,
                new Block[] {largerFieldBlock1.getRegion(0, 16), largerFieldBlock2.getRegion(0, 16)}));

        // Test BlockBuilder
        BlockBuilder emptyBlockBuilder = new RowBlockBuilder(
                ImmutableList.of(TINYINT, TINYINT),
                new BlockBuilderStatus(),
                0);
        assertNotCompact(emptyBlockBuilder);
        assertCompact(emptyBlockBuilder.build());

        List<Object>[] rows = new List[17];
        for (int i = 0; i < 17; i++) {
            rows[i] = ImmutableList.of(Byte.valueOf((byte) i), Byte.valueOf((byte) (i * 2)));
        }

        BlockBuilder nonFullBlockBuilder = createBlockBuilderWithValues(ImmutableList.of(TINYINT, TINYINT), rows, false);
        assertNotCompact(nonFullBlockBuilder);
        assertNotCompact(nonFullBlockBuilder.build());
        assertCopyRegionCompactness(nonFullBlockBuilder);

        BlockBuilder fullBlockBuilder = createBlockBuilderWithValues(ImmutableList.of(TINYINT, TINYINT), rows, true);
        assertNotCompact(fullBlockBuilder);
        assertCompact(fullBlockBuilder.build());
        assertCopyRegionCompactness(fullBlockBuilder);

        // NOTE: RowBlockBuilder will return itself if getRegion() is called to slice the whole block.
        // assertCompact(fullBlockBuilder.getRegion(0, fullBlockBuilder.getPositionCount()));
        assertNotCompact(fullBlockBuilder.getRegion(0, fullBlockBuilder.getPositionCount() - 1));
        assertNotCompact(fullBlockBuilder.getRegion(1, fullBlockBuilder.getPositionCount() - 1));
    }

    private void testWith(List<Type> fieldTypes, List<Object>[] expectedValues)
    {
        BlockBuilder blockBuilder = createBlockBuilderWithValues(fieldTypes, expectedValues, false);

        assertBlock(blockBuilder, expectedValues);
        assertBlock(blockBuilder.build(), expectedValues);

        IntArrayList positionList = generatePositionList(expectedValues.length, expectedValues.length / 2);
        assertBlockFilteredPositions(expectedValues, blockBuilder, positionList.toIntArray());
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), positionList.toIntArray());
    }

    private BlockBuilder createBlockBuilderWithValues(List<Type> fieldTypes, List<Object>[] rows, boolean useAccurateCapacityEstimation)
    {
        BlockBuilder rowBlockBuilder = new RowBlockBuilder(fieldTypes, new BlockBuilderStatus(), useAccurateCapacityEstimation ? rows.length : rows.length * 2);
        for (List<Object> row : rows) {
            if (row == null) {
                rowBlockBuilder.appendNull();
            }
            else {
                BlockBuilder singleRowBlockWriter = rowBlockBuilder.beginBlockEntry();
                for (int i = 0; i < row.size(); i++) {
                    Object fieldValue = row.get(i);
                    if (fieldValue == null) {
                        singleRowBlockWriter.appendNull();
                    }
                    else {
                        if (fieldValue instanceof Long) {
                            BIGINT.writeLong(singleRowBlockWriter, ((Long) fieldValue).longValue());
                        }
                        else if (fieldValue instanceof Byte && fieldTypes.get(i) == TINYINT) {
                            TINYINT.writeLong(singleRowBlockWriter, ((Byte) fieldValue).byteValue());
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
    protected <T> void assertPositionValue(Block block, int position, T expectedValue)
    {
        if (expectedValue instanceof List) {
            assertValue(block, position, (List<Object>) expectedValue);
            return;
        }
        super.assertPositionValue(block, position, expectedValue);
    }

    private void assertValue(Block rowBlock, int position, List<Object> row)
    {
        // null rows are handled by assertPositionValue
        requireNonNull(row, "row is null");

        assertFalse(rowBlock.isNull(position));
        SingleRowBlock singleRowBlock = (SingleRowBlock) rowBlock.getObject(position, Block.class);
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
