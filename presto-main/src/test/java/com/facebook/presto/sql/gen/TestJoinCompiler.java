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
import com.facebook.presto.operator.PagesHashStrategy;
import com.facebook.presto.operator.SimplePagesHashStrategy;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.JoinCompiler.PagesHashStrategyFactory;
import com.facebook.presto.type.TypeUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.block.BlockAssertions.assertBlockEquals;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.TypeUtils.hashPosition;
import static com.facebook.presto.type.TypeUtils.positionEqualsPosition;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestJoinCompiler
{
    private static final JoinCompiler joinCompiler = new JoinCompiler();

    @DataProvider(name = "hashEnabledValues")
    public static Object[][] hashEnabledValuesProvider()
    {
        return new Object[][] { { true }, { false } };
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testSingleChannel(boolean hashEnabled)
            throws Exception
    {
        List<Type> joinTypes = ImmutableList.<Type>of(VARCHAR);
        List<Integer> joinChannels = Ints.asList(0);

        // compile a single channel hash strategy
        PagesHashStrategyFactory pagesHashStrategyFactory = joinCompiler.compilePagesHashStrategyFactory(joinTypes, joinChannels);

        // create hash strategy with a single channel blocks -- make sure there is some overlap in values
        List<Block> channel = ImmutableList.of(
                BlockAssertions.createStringSequenceBlock(10, 20),
                BlockAssertions.createStringSequenceBlock(20, 30),
                BlockAssertions.createStringSequenceBlock(15, 25));

        Optional<Integer> hashChannel = Optional.empty();
        List<List<Block>> channels = ImmutableList.of(channel);
        if (hashEnabled) {
            ImmutableList.Builder<Block> hashChannelBuilder = ImmutableList.builder();
            for (Block block : channel) {
                hashChannelBuilder.add(TypeUtils.getHashBlock(joinTypes, block));
            }
            hashChannel = Optional.of(1);
            channels = ImmutableList.of(channel, hashChannelBuilder.build());
        }
        PagesHashStrategy hashStrategy = pagesHashStrategyFactory.createPagesHashStrategy(channels, hashChannel);

        // verify channel count
        assertEquals(hashStrategy.getChannelCount(), 1);

        // verify hashStrategy is consistent with equals and hash code from block
        for (int leftBlockIndex = 0; leftBlockIndex < channel.size(); leftBlockIndex++) {
            Block leftBlock = channel.get(leftBlockIndex);

            PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(VARCHAR));

            for (int leftBlockPosition = 0; leftBlockPosition < leftBlock.getPositionCount(); leftBlockPosition++) {
                // hash code of position must match block hash
                assertEquals(hashStrategy.hashPosition(leftBlockIndex, leftBlockPosition), hashPosition(VARCHAR, leftBlock, leftBlockPosition));

                // position must be equal to itself
                assertTrue(hashStrategy.positionEqualsPosition(leftBlockIndex, leftBlockPosition, leftBlockIndex, leftBlockPosition));

                // check equality of every position against every other position in the block
                for (int rightBlockIndex = 0; rightBlockIndex < channel.size(); rightBlockIndex++) {
                    Block rightBlock = channel.get(rightBlockIndex);
                    for (int rightBlockPosition = 0; rightBlockPosition < rightBlock.getPositionCount(); rightBlockPosition++) {
                        boolean expected = positionEqualsPosition(VARCHAR, leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
                        assertEquals(hashStrategy.positionEqualsRow(leftBlockIndex, leftBlockPosition, rightBlockPosition, rightBlock), expected);
                        assertEquals(hashStrategy.rowEqualsRow(leftBlockPosition, new Block[] {leftBlock}, rightBlockPosition, new Block[] {rightBlock}), expected);
                        assertEquals(hashStrategy.positionEqualsPosition(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition), expected);
                    }
                }

                // check equality of every position against every other position in the block cursor
                for (int rightBlockIndex = 0; rightBlockIndex < channel.size(); rightBlockIndex++) {
                    Block rightBlock = channel.get(rightBlockIndex);
                    for (int rightBlockPosition = 0; rightBlockPosition < rightBlock.getPositionCount(); rightBlockPosition++) {
                        boolean expected = positionEqualsPosition(VARCHAR, leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
                        assertEquals(hashStrategy.positionEqualsRow(leftBlockIndex, leftBlockPosition, rightBlockPosition, rightBlock), expected);
                        assertEquals(hashStrategy.rowEqualsRow(leftBlockPosition, new Block[] {leftBlock}, rightBlockPosition, new Block[] {rightBlock}), expected);
                        assertEquals(hashStrategy.positionEqualsPosition(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition), expected);
                    }
                }

                // write position to output block
                pageBuilder.declarePosition();
                hashStrategy.appendTo(leftBlockIndex, leftBlockPosition, pageBuilder, 0);
            }

            // verify output block matches
            assertBlockEquals(VARCHAR, pageBuilder.build().getBlock(0), leftBlock);
        }
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testMultiChannel(boolean hashEnabled)
            throws Exception
    {
        // compile a single channel hash strategy
        JoinCompiler joinCompiler = new JoinCompiler();
        List<Type> types = ImmutableList.<Type>of(VARCHAR, VARCHAR, BIGINT, DOUBLE, BOOLEAN);
        List<Type> joinTypes = ImmutableList.<Type>of(VARCHAR, BIGINT, DOUBLE, BOOLEAN);
        List<Integer> joinChannels = Ints.asList(1, 2, 3, 4);

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

        Optional<Integer> hashChannel = Optional.empty();
        ImmutableList<List<Block>> channels = ImmutableList.of(extraChannel, varcharChannel, longChannel, doubleChannel, booleanChannel);
        List<Block> precomputedHash = ImmutableList.of();
        if (hashEnabled) {
            ImmutableList.Builder<Block> hashChannelBuilder = ImmutableList.builder();
            for (int i = 0; i < 3; i++) {
                hashChannelBuilder.add(TypeUtils.getHashBlock(joinTypes, varcharChannel.get(i), longChannel.get(i), doubleChannel.get(i), booleanChannel.get(i)));
            }
            hashChannel = Optional.of(5);
            precomputedHash = hashChannelBuilder.build();
            channels = ImmutableList.of(extraChannel, varcharChannel, longChannel, doubleChannel, booleanChannel, precomputedHash);
            types = ImmutableList.<Type>of(VARCHAR, VARCHAR, BIGINT, DOUBLE, BOOLEAN, BIGINT);
        }

        PagesHashStrategyFactory pagesHashStrategyFactory = joinCompiler.compilePagesHashStrategyFactory(types, joinChannels);
        PagesHashStrategy hashStrategy = pagesHashStrategyFactory.createPagesHashStrategy(channels, hashChannel);
        PagesHashStrategy expectedHashStrategy = new SimplePagesHashStrategy(types, channels, joinChannels, hashChannel);

        // verify channel count
        assertEquals(hashStrategy.getChannelCount(), types.size());
        // verify size
        long sizeInBytes = channels.stream()
                .flatMap(List::stream)
                .mapToLong(Block::getRetainedSizeInBytes)
                .sum();
        assertEquals(hashStrategy.getSizeInBytes(), sizeInBytes);

        // verify hashStrategy is consistent with equals and hash code from block
        for (int leftBlockIndex = 0; leftBlockIndex < varcharChannel.size(); leftBlockIndex++) {
            PageBuilder pageBuilder = new PageBuilder(types);

            Block[] leftBlocks = new Block[4];
            leftBlocks[0] = varcharChannel.get(leftBlockIndex);
            leftBlocks[1] = longChannel.get(leftBlockIndex);
            leftBlocks[2] = doubleChannel.get(leftBlockIndex);
            leftBlocks[3] = booleanChannel.get(leftBlockIndex);

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
                    Block[] rightBlocks = new Block[4];
                    rightBlocks[0] = varcharChannel.get(rightBlockIndex);
                    rightBlocks[1] = longChannel.get(rightBlockIndex);
                    rightBlocks[2] = doubleChannel.get(rightBlockIndex);
                    rightBlocks[3] = booleanChannel.get(rightBlockIndex);

                    int rightPositionCount = varcharChannel.get(rightBlockIndex).getPositionCount();
                    for (int rightPosition = 0; rightPosition < rightPositionCount; rightPosition++) {
                        boolean expected = expectedHashStrategy.positionEqualsRow(leftBlockIndex, leftBlockPosition, rightPosition, rightBlocks);

                        assertEquals(hashStrategy.positionEqualsRow(leftBlockIndex, leftBlockPosition, rightPosition, rightBlocks), expected);
                        assertEquals(hashStrategy.rowEqualsRow(leftBlockPosition, leftBlocks, rightPosition, rightBlocks), expected);
                        assertEquals(hashStrategy.positionEqualsRow(leftBlockIndex, leftBlockPosition, rightPosition, rightBlocks), expected);
                    }
                }

                // write position to output block
                pageBuilder.declarePosition();
                hashStrategy.appendTo(leftBlockIndex, leftBlockPosition, pageBuilder, 0);
            }

            // verify output block matches
            Page page = pageBuilder.build();
            if (hashEnabled) {
                assertPageEquals(types, page, new Page(
                        extraChannel.get(leftBlockIndex),
                        varcharChannel.get(leftBlockIndex),
                        longChannel.get(leftBlockIndex),
                        doubleChannel.get(leftBlockIndex),
                        booleanChannel.get(leftBlockIndex),
                        precomputedHash.get(leftBlockIndex)));
            }
            else {
                assertPageEquals(types, page, new Page(
                        extraChannel.get(leftBlockIndex),
                        varcharChannel.get(leftBlockIndex),
                        longChannel.get(leftBlockIndex),
                        doubleChannel.get(leftBlockIndex),
                        booleanChannel.get(leftBlockIndex)));
            }
        }
    }
}
