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

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockFlattener;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.Random;
import java.util.stream.IntStream;

import static com.facebook.presto.block.BlockAssertions.assertBlockEquals;
import static com.facebook.presto.block.BlockSerdeUtil.readBlock;
import static com.facebook.presto.operator.BlockEncodingBuffers.createBlockEncodingBuffers;
import static com.facebook.presto.operator.OptimizedPartitionedOutputOperator.decodeBlock;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.Decimals.MAX_SHORT_PRECISION;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.testing.TestingEnvironment.TYPE_MANAGER;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestBlockEncodingBuffers
{
    private static final int POSITIONS_PER_BLOCK = 1000;
    private static final TestingBlockBuilders TESTING_BLOCK_BUILDERS = new TestingBlockBuilders();

    private Random random = new Random(0);

    @Test
    public void testBigint()
    {
        testBlock(BIGINT, TESTING_BLOCK_BUILDERS.buildBigintBlock(POSITIONS_PER_BLOCK, true));
    }

    @Test
    public void testLongDecimal()
    {
        testBlock(createDecimalType(MAX_SHORT_PRECISION + 1), TESTING_BLOCK_BUILDERS.buildLongDecimalBlock(POSITIONS_PER_BLOCK, true));
    }

    @Test
    public void testInteger()
    {
        testBlock(INTEGER, TESTING_BLOCK_BUILDERS.buildIntegerBlock(POSITIONS_PER_BLOCK, true));
    }

    @Test
    public void testSmallint()
    {
        testBlock(SMALLINT, TESTING_BLOCK_BUILDERS.buildSmallintBlock(POSITIONS_PER_BLOCK, true));
    }

    private void testBlock(Type type, Block block)
    {
        requireNonNull(type);

        assertSerialized(type, TESTING_BLOCK_BUILDERS.buildNullBlock(type, POSITIONS_PER_BLOCK));
        assertSerialized(type, block);

        assertSerialized(type, TESTING_BLOCK_BUILDERS.buildDictRleBlock(block, POSITIONS_PER_BLOCK, Optional.of("D")));
        assertSerialized(type, TESTING_BLOCK_BUILDERS.buildDictRleBlock(block, POSITIONS_PER_BLOCK, Optional.of("R")));
        assertSerialized(type, TESTING_BLOCK_BUILDERS.buildDictRleBlock(block, POSITIONS_PER_BLOCK, Optional.of("DDRR")));
        assertSerialized(type, TESTING_BLOCK_BUILDERS.buildDictRleBlock(block, POSITIONS_PER_BLOCK, Optional.of("RRDD")));
        assertSerialized(type, TESTING_BLOCK_BUILDERS.buildDictRleBlock(block, POSITIONS_PER_BLOCK, Optional.of("DRDR")));
        assertSerialized(type, TESTING_BLOCK_BUILDERS.buildDictRleBlock(block, POSITIONS_PER_BLOCK, Optional.of("RDRD")));

        assertSerialized(type, block.getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3));

        assertSerialized(type, TESTING_BLOCK_BUILDERS.buildDictRleBlock(block, POSITIONS_PER_BLOCK, Optional.of("D")).getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3));
        assertSerialized(type, TESTING_BLOCK_BUILDERS.buildDictRleBlock(block, POSITIONS_PER_BLOCK, Optional.of("R")).getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3));
        assertSerialized(type, TESTING_BLOCK_BUILDERS.buildDictRleBlock(block, POSITIONS_PER_BLOCK, Optional.of("DDRR")).getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3));
        assertSerialized(type, TESTING_BLOCK_BUILDERS.buildDictRleBlock(block, POSITIONS_PER_BLOCK, Optional.of("RRDD")).getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3));
        assertSerialized(type, TESTING_BLOCK_BUILDERS.buildDictRleBlock(block, POSITIONS_PER_BLOCK, Optional.of("DRDR")).getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3));
        assertSerialized(type, TESTING_BLOCK_BUILDERS.buildDictRleBlock(block, POSITIONS_PER_BLOCK, Optional.of("RDRD")).getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3));

        assertSerialized(type, TESTING_BLOCK_BUILDERS.buildDictRleBlock(block.getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3), POSITIONS_PER_BLOCK, Optional.of("D")));
        assertSerialized(type, TESTING_BLOCK_BUILDERS.buildDictRleBlock(block.getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3), POSITIONS_PER_BLOCK, Optional.of("R")));
        assertSerialized(type, TESTING_BLOCK_BUILDERS.buildDictRleBlock(block.getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3), POSITIONS_PER_BLOCK, Optional.of("DDRR")));
        assertSerialized(type, TESTING_BLOCK_BUILDERS.buildDictRleBlock(block.getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3), POSITIONS_PER_BLOCK, Optional.of("RRDD")));
    }

    private static void assertSerialized(Type type, Block block)
    {
        assertSerialized(type, block, null);
    }

    private static void assertSerialized(Type type, Block block, int[] expectedRowSizes)
    {
        // The number 5 was chosen to be greater than the maximum Dictionary/RLE nested level
        BlockFlattener flattener = new BlockFlattener(new SimpleArrayAllocator(5));
        DecodedBlockNode decodedBlock = decodeBlock(flattener, block);

        BlockEncodingBuffers buffers = createBlockEncodingBuffers(decodedBlock, 100);

        int[] positions = IntStream.range(0, block.getPositionCount() / 2).toArray();
        copyPositions(decodedBlock, buffers, positions, expectedRowSizes);

        positions = IntStream.range(block.getPositionCount() / 2, block.getPositionCount()).toArray();
        copyPositions(decodedBlock, buffers, positions, expectedRowSizes);

        assertBlockEquals(type, serialize(buffers), block);

        buffers.resetBuffers();

        positions = IntStream.range(0, block.getPositionCount()).filter(n -> n % 2 == 0).toArray();
        Block expectedBlock = block.copyPositions(positions, 0, positions.length);
        copyPositions(decodedBlock, buffers, positions, expectedRowSizes);

        assertBlockEquals(type, serialize(buffers), expectedBlock);
    }

    private static void copyPositions(DecodedBlockNode decodedBlock, BlockEncodingBuffers buffers, int[] positions, int[] expectedRowSizes)
    {
        buffers.setupDecodedBlocksAndPositions(decodedBlock, positions, positions.length);

        if (expectedRowSizes != null) {
            int[] actualSizes = new int[positions.length];
            buffers.accumulateRowSizes(actualSizes);

            int[] expectedSizes = Arrays.stream(positions).map(i -> expectedRowSizes[i]).toArray();
            assertEquals(actualSizes, expectedSizes);
        }

        buffers.setNextBatch(0, positions.length);
        buffers.copyValues();
    }

    private static Block serialize(BlockEncodingBuffers buffers)
    {
        SliceOutput output = new DynamicSliceOutput(toIntExact(buffers.getSerializedSizeInBytes()));
        buffers.serializeTo(output);

        BlockEncodingManager blockEncodingSerde = new BlockEncodingManager(TYPE_MANAGER);
        return readBlock(blockEncodingSerde, output.slice().getInput());
    }
}
