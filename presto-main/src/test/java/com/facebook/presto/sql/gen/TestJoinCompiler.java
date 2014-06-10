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
package com.facebook.presto.sql.gen;

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.operator.PagesHashStrategy;
import com.facebook.presto.operator.SimplePagesHashStrategy;
import com.facebook.presto.sql.gen.JoinCompiler.PagesHashStrategyFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.block.BlockAssertions.assertBlockEquals;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestJoinCompiler
{
    @Test
    public void testSingleChannel()
            throws Exception
    {
        // compile a single channel hash strategy
        JoinCompiler joinCompiler = new JoinCompiler();
        PagesHashStrategyFactory pagesHashStrategyFactory = joinCompiler.compilePagesHashStrategy(1, Ints.asList(0));

        // crate hash strategy with a single channel blocks -- make sure there is some overlap in values
        List<Block> channel = ImmutableList.of(
                BlockAssertions.createStringSequenceBlock(10, 20),
                BlockAssertions.createStringSequenceBlock(20, 30),
                BlockAssertions.createStringSequenceBlock(15, 25));
        PagesHashStrategy hashStrategy = pagesHashStrategyFactory.createPagesHashStrategy(ImmutableList.of(channel));

        // verify channel count
        assertEquals(hashStrategy.getChannelCount(), 1);

        // verify hashStrategy is consistent with equals and hash code from block
        for (int leftBlockIndex = 0; leftBlockIndex < channel.size(); leftBlockIndex++) {
            Block leftBlock = channel.get(leftBlockIndex);

            PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(VARCHAR));

            for (int leftBlockPosition = 0; leftBlockPosition < leftBlock.getPositionCount(); leftBlockPosition++) {
                // hash code of position must match block hash
                assertEquals(hashStrategy.hashPosition(leftBlockIndex, leftBlockPosition), leftBlock.hash(leftBlockPosition));

                // position must be equal to itself
                assertTrue(hashStrategy.positionEqualsPosition(leftBlockIndex, leftBlockPosition, leftBlockIndex, leftBlockPosition));

                // check equality of every position against every other position in the block
                for (int rightBlockIndex = 0; rightBlockIndex < channel.size(); rightBlockIndex++) {
                    Block rightBlock = channel.get(rightBlockIndex);
                    for (int rightBlockPosition = 0; rightBlockPosition < rightBlock.getPositionCount(); rightBlockPosition++) {
                        assertEquals(
                                hashStrategy.positionEqualsPosition(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition),
                                leftBlock.equalTo(leftBlockPosition, rightBlock, rightBlockPosition));
                    }
                }

                // check equality of every position against every other position in the block cursor
                for (Block rightBlock : channel) {
                    BlockCursor rightCursor = rightBlock.cursor();
                    BlockCursor[] rightCursors = new BlockCursor[]{rightCursor};
                    while (rightCursor.advanceNextPosition()) {
                        assertEquals(
                                hashStrategy.positionEqualsCursors(leftBlockIndex, leftBlockPosition, rightCursors),
                                leftBlock.equalTo(leftBlockPosition, rightCursor));
                    }
                }

                // write position to output block
                hashStrategy.appendTo(leftBlockIndex, leftBlockPosition, pageBuilder, 0);
            }

            // verify output block matches
            assertBlockEquals(pageBuilder.build().getBlock(0), leftBlock);
        }
    }

    @Test
    public void testMultiChannel()
            throws Exception
    {
        // compile a single channel hash strategy
        JoinCompiler joinCompiler = new JoinCompiler();
        PagesHashStrategyFactory pagesHashStrategyFactory = joinCompiler.compilePagesHashStrategy(5, Ints.asList(1, 2, 3, 4));

        // crate hash strategy with a single channel blocks -- make sure there is some overlap in values
        List<Block> extraChannel = ImmutableList.of(
                BlockAssertions.createStringSequenceBlock(10, 20),
                BlockAssertions.createStringSequenceBlock(20, 30),
                BlockAssertions.createStringSequenceBlock(15, 25));
        List<Block> varcharChannel = ImmutableList.of(
                BlockAssertions.createStringSequenceBlock(10, 20),
                BlockAssertions.createStringSequenceBlock(20, 30),
                BlockAssertions.createStringSequenceBlock(15, 25));
        List<Block> longChannel = ImmutableList.of(
                BlockAssertions.createLongSequenceBlock(10, 20),
                BlockAssertions.createLongSequenceBlock(20, 30),
                BlockAssertions.createLongSequenceBlock(15, 25));
        List<Block> doubleChannel = ImmutableList.of(
                BlockAssertions.createDoubleSequenceBlock(10, 20),
                BlockAssertions.createDoubleSequenceBlock(20, 30),
                BlockAssertions.createDoubleSequenceBlock(15, 25));
        List<Block> booleanChannel = ImmutableList.of(
                BlockAssertions.createBooleanSequenceBlock(10, 20),
                BlockAssertions.createBooleanSequenceBlock(20, 30),
                BlockAssertions.createBooleanSequenceBlock(15, 25));
        ImmutableList<List<Block>> channels = ImmutableList.of(extraChannel, varcharChannel, longChannel, doubleChannel, booleanChannel);
        PagesHashStrategy hashStrategy = pagesHashStrategyFactory.createPagesHashStrategy(channels);

        // verify channel count
        assertEquals(hashStrategy.getChannelCount(), 5);

        PagesHashStrategy expectedHashStrategy = new SimplePagesHashStrategy(channels, Ints.asList(1, 2, 3, 4));

        // verify hashStrategy is consistent with equals and hash code from block
        for (int leftBlockIndex = 0; leftBlockIndex < varcharChannel.size(); leftBlockIndex++) {
            PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(VARCHAR, VARCHAR, BIGINT, DOUBLE, BOOLEAN));

            int leftPositionCount = varcharChannel.get(leftBlockIndex).getPositionCount();
            for (int leftBlockPosition = 0; leftBlockPosition < leftPositionCount; leftBlockPosition++) {
                // hash code of position must match block hash
                assertEquals(
                        hashStrategy.hashPosition(leftBlockIndex, leftBlockPosition),
                        expectedHashStrategy.hashPosition(leftBlockIndex, leftBlockPosition));

                // position must be equal to itself
                assertTrue(hashStrategy.positionEqualsPosition(leftBlockIndex, leftBlockPosition, leftBlockIndex, leftBlockPosition));

                // check equality of every position against every other position in the block
                for (int rightBlockIndex = 0; rightBlockIndex < varcharChannel.size(); rightBlockIndex++) {
                    Block rightBlock = varcharChannel.get(rightBlockIndex);
                    for (int rightBlockPosition = 0; rightBlockPosition < rightBlock.getPositionCount(); rightBlockPosition++) {
                        assertEquals(
                                hashStrategy.positionEqualsPosition(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition),
                                expectedHashStrategy.positionEqualsPosition(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition));
                    }
                }

                // check equality of every position against every other position in the block cursor
                for (int rightBlockIndex = 0; rightBlockIndex < varcharChannel.size(); rightBlockIndex++) {
                    BlockCursor[] rightCursors = new BlockCursor[4];
                    rightCursors[0] = varcharChannel.get(rightBlockIndex).cursor();
                    rightCursors[1] = longChannel.get(rightBlockIndex).cursor();
                    rightCursors[2] = doubleChannel.get(rightBlockIndex).cursor();
                    rightCursors[3] = booleanChannel.get(rightBlockIndex).cursor();

                    int rightPositionCount = varcharChannel.get(rightBlockIndex).getPositionCount();
                    for (int rightPosition = 0; rightPosition < rightPositionCount; rightPosition++) {
                        for (BlockCursor rightCursor : rightCursors) {
                            assertTrue(rightCursor.advanceNextPosition());
                        }

                        assertEquals(
                                hashStrategy.positionEqualsCursors(leftBlockIndex, leftBlockPosition, rightCursors),
                                expectedHashStrategy.positionEqualsCursors(leftBlockIndex, leftBlockPosition, rightCursors));
                    }
                }

                // write position to output block
                hashStrategy.appendTo(leftBlockIndex, leftBlockPosition, pageBuilder, 0);
            }

            // verify output block matches
            Page page = pageBuilder.build();
            assertPageEquals(page, new Page(
                    extraChannel.get(leftBlockIndex),
                    varcharChannel.get(leftBlockIndex),
                    longChannel.get(leftBlockIndex),
                    doubleChannel.get(leftBlockIndex),
                    booleanChannel.get(leftBlockIndex)));
        }
    }
}
