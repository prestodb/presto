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
package io.prestosql.plugin.thrift.api.datatypes;

import io.prestosql.plugin.thrift.api.PrestoThriftBlock;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static io.prestosql.plugin.thrift.api.PrestoThriftBlock.bigintData;
import static io.prestosql.plugin.thrift.api.PrestoThriftBlock.integerData;
import static io.prestosql.plugin.thrift.api.datatypes.PrestoThriftBigint.fromBlock;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static java.util.Collections.unmodifiableList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestPrestoThriftBigint
{
    @Test
    public void testReadBlock()
    {
        PrestoThriftBlock columnsData = longColumn(
                new boolean[] {false, true, false, false, false, false, true},
                new long[] {2, 0, 1, 3, 8, 4, 0});
        Block actual = columnsData.toBlock(BIGINT);
        assertBlockEquals(actual, list(2L, null, 1L, 3L, 8L, 4L, null));
    }

    @Test
    public void testReadBlockAllNullsOption1()
    {
        PrestoThriftBlock columnsData = longColumn(
                new boolean[] {true, true, true, true, true, true, true},
                null);
        Block actual = columnsData.toBlock(BIGINT);
        assertBlockEquals(actual, list(null, null, null, null, null, null, null));
    }

    @Test
    public void testReadBlockAllNullsOption2()
    {
        PrestoThriftBlock columnsData = longColumn(
                new boolean[] {true, true, true, true, true, true, true},
                new long[] {0, 0, 0, 0, 0, 0, 0});
        Block actual = columnsData.toBlock(BIGINT);
        assertBlockEquals(actual, list(null, null, null, null, null, null, null));
    }

    @Test
    public void testReadBlockAllNonNullOption1()
    {
        PrestoThriftBlock columnsData = longColumn(
                null,
                new long[] {2, 7, 1, 3, 8, 4, 5});
        Block actual = columnsData.toBlock(BIGINT);
        assertBlockEquals(actual, list(2L, 7L, 1L, 3L, 8L, 4L, 5L));
    }

    @Test
    public void testReadBlockAllNonNullOption2()
    {
        PrestoThriftBlock columnsData = longColumn(
                new boolean[] {false, false, false, false, false, false, false},
                new long[] {2, 7, 1, 3, 8, 4, 5});
        Block actual = columnsData.toBlock(BIGINT);
        assertBlockEquals(actual, list(2L, 7L, 1L, 3L, 8L, 4L, 5L));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testReadBlockWrongActualType()
    {
        PrestoThriftBlock columnsData = integerData(new PrestoThriftInteger(null, null));
        columnsData.toBlock(BIGINT);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testReadBlockWrongDesiredType()
    {
        PrestoThriftBlock columnsData = longColumn(null, null);
        columnsData.toBlock(INTEGER);
    }

    @Test
    public void testWriteBlockAlternating()
    {
        Block source = longBlock(1, null, 2, null, 3, null, 4, null, 5, null, 6, null, 7, null);
        PrestoThriftBlock column = fromBlock(source);
        assertNotNull(column.getBigintData());
        assertEquals(column.getBigintData().getNulls(),
                new boolean[] {false, true, false, true, false, true, false, true, false, true, false, true, false, true});
        assertEquals(column.getBigintData().getLongs(),
                new long[] {1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 6, 0, 7, 0});
    }

    @Test
    public void testWriteBlockAllNulls()
    {
        Block source = longBlock(null, null, null, null, null);
        PrestoThriftBlock column = fromBlock(source);
        assertNotNull(column.getBigintData());
        assertEquals(column.getBigintData().getNulls(), new boolean[] {true, true, true, true, true});
        assertNull(column.getBigintData().getLongs());
    }

    @Test
    public void testWriteBlockAllNonNull()
    {
        Block source = longBlock(1, 2, 3, 4, 5);
        PrestoThriftBlock column = fromBlock(source);
        assertNotNull(column.getBigintData());
        assertNull(column.getBigintData().getNulls());
        assertEquals(column.getBigintData().getLongs(), new long[] {1, 2, 3, 4, 5});
    }

    @Test
    public void testWriteBlockEmpty()
    {
        PrestoThriftBlock column = fromBlock(longBlock());
        assertNotNull(column.getBigintData());
        assertNull(column.getBigintData().getNulls());
        assertNull(column.getBigintData().getLongs());
    }

    @Test
    public void testWriteBlockSingleValue()
    {
        PrestoThriftBlock column = fromBlock(longBlock(1));
        assertNotNull(column.getBigintData());
        assertNull(column.getBigintData().getNulls());
        assertEquals(column.getBigintData().getLongs(), new long[] {1});
    }

    private void assertBlockEquals(Block block, List<Long> expected)
    {
        assertEquals(block.getPositionCount(), expected.size());
        for (int i = 0; i < expected.size(); i++) {
            if (expected.get(i) == null) {
                assertTrue(block.isNull(i));
            }
            else {
                assertEquals(block.getLong(i, 0), expected.get(i).longValue());
            }
        }
    }

    private static Block longBlock(Integer... values)
    {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, values.length);
        for (Integer value : values) {
            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeLong(value).closeEntry();
            }
        }
        return blockBuilder.build();
    }

    private static PrestoThriftBlock longColumn(boolean[] nulls, long[] longs)
    {
        return bigintData(new PrestoThriftBigint(nulls, longs));
    }

    private static List<Long> list(Long... values)
    {
        return unmodifiableList(Arrays.asList(values));
    }
}
