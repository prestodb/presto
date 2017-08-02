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
import com.facebook.presto.spi.block.RowBlockBuilder;
import com.facebook.presto.spi.block.SingleRowBlock;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
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

    private void testWith(List<Type> fieldTypes, List<Object>[] expectedValues)
    {
        BlockBuilder blockBuilder = createBlockBuilderWithValues(fieldTypes, expectedValues);

        assertBlock(blockBuilder, expectedValues);
        assertBlock(blockBuilder.build(), expectedValues);

        List<Integer> positionList = generatePositionList(expectedValues.length, expectedValues.length / 2);
        assertBlockFilteredPositions(expectedValues, blockBuilder, positionList);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), positionList);
    }

    private BlockBuilder createBlockBuilderWithValues(List<Type> fieldTypes, List<Object>[] rows)
    {
        BlockBuilder rowBlockBuilder = new RowBlockBuilder(fieldTypes, new BlockBuilderStatus(), 1);
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
        }
        return testRows;
    }

    private List<Integer> generatePositionList(int numRows, int numPositions)
    {
        List<Integer> positions = new ArrayList<>(numPositions);
        for (int i = 0; i < numPositions; i++) {
            positions.add((7 * i + 3) % numRows);
        }
        Collections.sort(positions);
        return positions;
    }
}
