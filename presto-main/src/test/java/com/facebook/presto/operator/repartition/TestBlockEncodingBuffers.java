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
package com.facebook.presto.operator.repartition;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.operator.SimpleArrayAllocator;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockFlattener;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.stream.IntStream;

import static com.facebook.presto.block.BlockAssertions.Encoding.DICTIONARY;
import static com.facebook.presto.block.BlockAssertions.Encoding.RUN_LENGTH;
import static com.facebook.presto.block.BlockAssertions.assertBlockEquals;
import static com.facebook.presto.block.BlockAssertions.createAllNullsBlock;
import static com.facebook.presto.block.BlockAssertions.createRandomIntsBlock;
import static com.facebook.presto.block.BlockAssertions.createRandomLongDecimalsBlock;
import static com.facebook.presto.block.BlockAssertions.createRandomLongsBlock;
import static com.facebook.presto.block.BlockAssertions.wrapBlock;
import static com.facebook.presto.block.BlockSerdeUtil.readBlock;
import static com.facebook.presto.operator.repartition.AbstractBlockEncodingBuffer.createBlockEncodingBuffers;
import static com.facebook.presto.operator.repartition.OptimizedPartitionedOutputOperator.decodeBlock;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.Decimals.MAX_SHORT_PRECISION;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.testing.TestingEnvironment.TYPE_MANAGER;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestBlockEncodingBuffers
{
    private static final int POSITIONS_PER_BLOCK = 1000;

    @Test
    public void testBigint()
    {
        testBlock(BIGINT, createRandomLongsBlock(POSITIONS_PER_BLOCK, true));
    }

    @Test
    public void testLongDecimal()
    {
        testBlock(createDecimalType(MAX_SHORT_PRECISION + 1), createRandomLongDecimalsBlock(POSITIONS_PER_BLOCK, true));
    }

    @Test
    public void testInteger()
    {
        testBlock(INTEGER, createRandomIntsBlock(POSITIONS_PER_BLOCK, true));
    }

    private void testBlock(Type type, Block block)
    {
        requireNonNull(type);

        assertSerialized(type, createAllNullsBlock(type, POSITIONS_PER_BLOCK));
        assertSerialized(type, block);

        assertSerialized(type, wrapBlock(block, POSITIONS_PER_BLOCK, ImmutableList.of(DICTIONARY)));
        assertSerialized(type, wrapBlock(block, POSITIONS_PER_BLOCK, ImmutableList.of(RUN_LENGTH)));
        assertSerialized(type, wrapBlock(block, POSITIONS_PER_BLOCK, ImmutableList.of(DICTIONARY, DICTIONARY, RUN_LENGTH)));
        assertSerialized(type, wrapBlock(block, POSITIONS_PER_BLOCK, ImmutableList.of(RUN_LENGTH, DICTIONARY, DICTIONARY)));
        assertSerialized(type, wrapBlock(block, POSITIONS_PER_BLOCK, ImmutableList.of(DICTIONARY, RUN_LENGTH, DICTIONARY, RUN_LENGTH)));
        assertSerialized(type, wrapBlock(block, POSITIONS_PER_BLOCK, ImmutableList.of(RUN_LENGTH, DICTIONARY, RUN_LENGTH, DICTIONARY)));

        assertSerialized(type, block.getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3));

        assertSerialized(type, wrapBlock(block, POSITIONS_PER_BLOCK, ImmutableList.of(DICTIONARY)).getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3));
        assertSerialized(type, wrapBlock(block, POSITIONS_PER_BLOCK, ImmutableList.of(RUN_LENGTH)).getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3));
        assertSerialized(type, wrapBlock(block, POSITIONS_PER_BLOCK, ImmutableList.of(DICTIONARY, DICTIONARY, RUN_LENGTH)).getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3));
        assertSerialized(type, wrapBlock(block, POSITIONS_PER_BLOCK, ImmutableList.of(RUN_LENGTH, DICTIONARY, DICTIONARY)).getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3));
        assertSerialized(type, wrapBlock(block, POSITIONS_PER_BLOCK, ImmutableList.of(DICTIONARY, RUN_LENGTH, DICTIONARY, RUN_LENGTH)).getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3));
        assertSerialized(type, wrapBlock(block, POSITIONS_PER_BLOCK, ImmutableList.of(RUN_LENGTH, DICTIONARY, RUN_LENGTH, DICTIONARY)).getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3));

        assertSerialized(type, wrapBlock(block.getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3), POSITIONS_PER_BLOCK, ImmutableList.of(DICTIONARY)));
        assertSerialized(type, wrapBlock(block.getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3), POSITIONS_PER_BLOCK, ImmutableList.of(RUN_LENGTH)));
        assertSerialized(type, wrapBlock(block.getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3), POSITIONS_PER_BLOCK, ImmutableList.of(DICTIONARY, DICTIONARY, RUN_LENGTH)));
        assertSerialized(type, wrapBlock(block.getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3), POSITIONS_PER_BLOCK, ImmutableList.of(RUN_LENGTH, DICTIONARY, DICTIONARY)));
        assertSerialized(type, wrapBlock(block.getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3), POSITIONS_PER_BLOCK, ImmutableList.of(DICTIONARY, RUN_LENGTH, DICTIONARY, RUN_LENGTH)));
        assertSerialized(type, wrapBlock(block.getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3), POSITIONS_PER_BLOCK, ImmutableList.of(RUN_LENGTH, DICTIONARY, RUN_LENGTH, DICTIONARY)));
    }

    private static void assertSerialized(Type type, Block block)
    {
        assertSerialized(type, block, null);
    }

    private static void assertSerialized(Type type, Block block, int[] expectedRowSizes)
    {
        Closer blockLeaseCloser = Closer.create();

        BlockFlattener flattener = new BlockFlattener(new SimpleArrayAllocator());
        DecodedBlockNode decodedBlock = decodeBlock(flattener, blockLeaseCloser, block);

        BlockEncodingBuffer buffers = createBlockEncodingBuffers(decodedBlock);

        int[] positions = IntStream.range(0, block.getPositionCount() / 2).toArray();
        copyPositions(decodedBlock, buffers, positions, expectedRowSizes);

        positions = IntStream.range(block.getPositionCount() / 2, block.getPositionCount()).toArray();
        copyPositions(decodedBlock, buffers, positions, expectedRowSizes);

        assertBlockEquals(type, serialize(buffers), block);

        buffers.resetBuffers();

        positions = IntStream.range(0, block.getPositionCount()).filter(n -> n % 2 == 0).toArray();
        Block expectedBlock = block.copyPositions(positions, 0, positions.length);
        copyPositions(decodedBlock, buffers, positions, expectedRowSizes);

        try {
            blockLeaseCloser.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        assertBlockEquals(type, serialize(buffers), expectedBlock);
    }

    private static void copyPositions(DecodedBlockNode decodedBlock, BlockEncodingBuffer buffer, int[] positions, int[] expectedRowSizes)
    {
        buffer.setupDecodedBlocksAndPositions(decodedBlock, positions, positions.length);

        ((AbstractBlockEncodingBuffer) buffer).checkValidPositions();

        if (expectedRowSizes != null) {
            int[] actualSizes = new int[positions.length];
            buffer.accumulateSerializedRowSizes(actualSizes);

            int[] expectedSizes = Arrays.stream(positions).map(i -> expectedRowSizes[i]).toArray();
            assertEquals(actualSizes, expectedSizes);
        }

        buffer.setNextBatch(0, positions.length);
        buffer.appendDataInBatch();
    }

    private static Block serialize(BlockEncodingBuffer buffer)
    {
        SliceOutput output = new DynamicSliceOutput(toIntExact(buffer.getSerializedSizeInBytes()));
        buffer.serializeTo(output);

        BlockEncodingManager blockEncodingSerde = new BlockEncodingManager(TYPE_MANAGER);
        return readBlock(blockEncodingSerde, output.slice().getInput());
    }
}
