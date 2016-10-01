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
package com.facebook.presto.operator;

import com.facebook.presto.Session;
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.testing.TestingSession;
import com.facebook.presto.type.TypeUtils;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.operator.GroupByHash.createGroupByHash;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.TypeUtils.getHashBlock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestGroupByHash
{
    private static final int MAX_GROUP_ID = 500;
    private static final int[] CONTAINS_CHANNELS = new int[] { 0 };
    private static final Session TEST_SESSION = TestingSession.testSessionBuilder().build();

    @Test
    public void testAddPage()
            throws Exception
    {
        GroupByHash groupByHash = createGroupByHash(TEST_SESSION, ImmutableList.of(BIGINT), new int[] { 0}, Optional.of(1), 100);
        for (int tries = 0; tries < 2; tries++) {
            for (int value = 0; value < MAX_GROUP_ID; value++) {
                Block block = BlockAssertions.createLongsBlock(value);
                Block hashBlock = TypeUtils.getHashBlock(ImmutableList.of(BIGINT), block);
                Page page = new Page(block, hashBlock);
                for (int addValuesTries = 0; addValuesTries < 10; addValuesTries++) {
                    groupByHash.addPage(page);
                    assertEquals(groupByHash.getGroupCount(), tries == 0 ? value + 1 : MAX_GROUP_ID);

                    // add the page again using get group ids and make sure the group count didn't change
                    GroupByIdBlock groupIds = groupByHash.getGroupIds(page);
                    assertEquals(groupByHash.getGroupCount(), tries == 0 ? value + 1 : MAX_GROUP_ID);
                    assertEquals(groupIds.getGroupCount(), tries == 0 ? value + 1 : MAX_GROUP_ID);

                    // verify the first position
                    assertEquals(groupIds.getPositionCount(), 1);
                    long groupId = groupIds.getGroupId(0);
                    assertEquals(groupId, value);
                }
            }
        }
    }

    @Test
    public void testNullGroup()
            throws Exception
    {
        GroupByHash groupByHash = createGroupByHash(TEST_SESSION, ImmutableList.of(BIGINT), new int[] { 0}, Optional.of(1), 100);

        Block block = createLongsBlock((Long) null);
        Block hashBlock = getHashBlock(ImmutableList.of(BIGINT), block);
        Page page = new Page(block, hashBlock);
        groupByHash.addPage(page);

        // Add enough values to force a rehash
        block = createLongSequenceBlock(1, 132748);
        hashBlock = getHashBlock(ImmutableList.of(BIGINT), block);
        page = new Page(block, hashBlock);
        groupByHash.addPage(page);

        block = createLongsBlock(0);
        hashBlock = getHashBlock(ImmutableList.of(BIGINT), block);
        page = new Page(block, hashBlock);
        assertFalse(groupByHash.contains(0, page, CONTAINS_CHANNELS));
    }

    @Test
    public void testGetGroupIds()
            throws Exception
    {
        GroupByHash groupByHash = createGroupByHash(TEST_SESSION, ImmutableList.of(BIGINT), new int[] { 0 }, Optional.of(1), 100);
        for (int tries = 0; tries < 2; tries++) {
            for (int value = 0; value < MAX_GROUP_ID; value++) {
                Block block = BlockAssertions.createLongsBlock(value);
                Block hashBlock = TypeUtils.getHashBlock(ImmutableList.of(BIGINT), block);
                Page page = new Page(block, hashBlock);
                for (int addValuesTries = 0; addValuesTries < 10; addValuesTries++) {
                    GroupByIdBlock groupIds = groupByHash.getGroupIds(page);
                    assertEquals(groupIds.getGroupCount(), tries == 0 ? value + 1 : MAX_GROUP_ID);
                    assertEquals(groupIds.getPositionCount(), 1);
                    long groupId = groupIds.getGroupId(0);
                    assertEquals(groupId, value);
                }
            }
        }
    }

    @Test
    public void testTypes()
            throws Exception
    {
        GroupByHash groupByHash = createGroupByHash(TEST_SESSION, ImmutableList.of(VARCHAR), new int[] { 0 }, Optional.of(1), 100);
        // Additional bigint channel for hash
        assertEquals(groupByHash.getTypes(), ImmutableList.of(VARCHAR, BIGINT));
    }

    @Test
    public void testAppendTo()
            throws Exception
    {
        Block valuesBlock = BlockAssertions.createStringSequenceBlock(0, 100);
        Block hashBlock = TypeUtils.getHashBlock(ImmutableList.of(VARCHAR), valuesBlock);
        GroupByHash groupByHash = createGroupByHash(TEST_SESSION, ImmutableList.of(VARCHAR), new int[] { 0 }, Optional.of(1), 100);

        GroupByIdBlock groupIds = groupByHash.getGroupIds(new Page(valuesBlock, hashBlock));
        for (int i = 0; i < groupIds.getPositionCount(); i++) {
            assertEquals(groupIds.getGroupId(i), i);
        }
        assertEquals(groupByHash.getGroupCount(), 100);

        PageBuilder pageBuilder = new PageBuilder(groupByHash.getTypes());
        for (int i = 0; i < groupByHash.getGroupCount(); i++) {
            pageBuilder.declarePosition();
            groupByHash.appendValuesTo(i, pageBuilder, 0);
        }
        Page page = pageBuilder.build();
        // Ensure that all blocks have the same positionCount
        for (int i = 0; i < groupByHash.getTypes().size(); i++) {
            assertEquals(page.getBlock(i).getPositionCount(), 100);
        }
        assertEquals(page.getPositionCount(), 100);
        BlockAssertions.assertBlockEquals(VARCHAR, page.getBlock(0), valuesBlock);
        BlockAssertions.assertBlockEquals(BIGINT, page.getBlock(1), hashBlock);
    }

    @Test
    public void testAppendToMultipleTuplesPerGroup()
            throws Exception
    {
        List<Long> values = new ArrayList<>();
        for (long i = 0; i < 100; i++) {
            values.add(i % 50);
        }
        Block valuesBlock = BlockAssertions.createLongsBlock(values);
        Block hashBlock = TypeUtils.getHashBlock(ImmutableList.of(BIGINT), valuesBlock);

        GroupByHash groupByHash = createGroupByHash(TEST_SESSION, ImmutableList.of(BIGINT), new int[] { 0 }, Optional.of(1), 100);
        groupByHash.getGroupIds(new Page(valuesBlock, hashBlock));
        assertEquals(groupByHash.getGroupCount(), 50);

        PageBuilder pageBuilder = new PageBuilder(groupByHash.getTypes());
        for (int i = 0; i < groupByHash.getGroupCount(); i++) {
            pageBuilder.declarePosition();
            groupByHash.appendValuesTo(i, pageBuilder, 0);
        }
        Page outputPage = pageBuilder.build();
        assertEquals(outputPage.getPositionCount(), 50);
        BlockAssertions.assertBlockEquals(BIGINT, outputPage.getBlock(0), BlockAssertions.createLongSequenceBlock(0, 50));
    }

    @Test
    public void testContains()
            throws Exception
    {
        Block valuesBlock = BlockAssertions.createDoubleSequenceBlock(0, 10);
        Block hashBlock = TypeUtils.getHashBlock(ImmutableList.of(DOUBLE), valuesBlock);
        GroupByHash groupByHash = createGroupByHash(TEST_SESSION, ImmutableList.of(DOUBLE), new int[] { 0 }, Optional.of(1), 100);
        groupByHash.getGroupIds(new Page(valuesBlock, hashBlock));

        Block testBlock = BlockAssertions.createDoublesBlock((double) 3);
        Block testHashBlock = TypeUtils.getHashBlock(ImmutableList.of(DOUBLE), testBlock);
        assertTrue(groupByHash.contains(0, new Page(testBlock, testHashBlock), CONTAINS_CHANNELS));

        testBlock = BlockAssertions.createDoublesBlock(11.0);
        testHashBlock = TypeUtils.getHashBlock(ImmutableList.of(DOUBLE), testBlock);
        assertFalse(groupByHash.contains(0, new Page(testBlock, testHashBlock), CONTAINS_CHANNELS));
    }

    @Test
    public void testContainsMultipleColumns()
            throws Exception
    {
        Block valuesBlock = BlockAssertions.createDoubleSequenceBlock(0, 10);
        Block stringValuesBlock = BlockAssertions.createStringSequenceBlock(0, 10);
        Block hashBlock = TypeUtils.getHashBlock(ImmutableList.of(DOUBLE, VARCHAR), valuesBlock, stringValuesBlock);
        int[] hashChannels = { 0, 1 };
        GroupByHash groupByHash = createGroupByHash(TEST_SESSION, ImmutableList.of(DOUBLE, VARCHAR), hashChannels, Optional.of(2), 100);
        groupByHash.getGroupIds(new Page(valuesBlock, stringValuesBlock, hashBlock));

        Block testValuesBlock = BlockAssertions.createDoublesBlock((double) 3);
        Block testStringValuesBlock = BlockAssertions.createStringsBlock("3");
        Block testHashBlock = TypeUtils.getHashBlock(ImmutableList.of(DOUBLE, VARCHAR), testValuesBlock, testStringValuesBlock);
        assertTrue(groupByHash.contains(0, new Page(testValuesBlock, testStringValuesBlock, testHashBlock), hashChannels));
    }

    @Test
    public void testForceRehash()
            throws Exception
    {
        // Create a page with positionCount >> expected size of groupByHash
        Block valuesBlock = BlockAssertions.createStringSequenceBlock(0, 100);
        Block hashBlock = TypeUtils.getHashBlock(ImmutableList.of(VARCHAR), valuesBlock);

        // Create group by hash with extremely small size
        GroupByHash groupByHash = createGroupByHash(TEST_SESSION, ImmutableList.of(VARCHAR), new int[] { 0 }, Optional.of(1), 4);
        groupByHash.getGroupIds(new Page(valuesBlock, hashBlock));

        // Ensure that all groups are present in group by hash
        for (int i = 0; i < valuesBlock.getPositionCount(); i++) {
            assertTrue(groupByHash.contains(i, new Page(valuesBlock, hashBlock), CONTAINS_CHANNELS));
        }
    }
}
