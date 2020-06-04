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

import com.facebook.presto.block.BlockAssertions.Encoding;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockFlattener;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.UncheckedStackArrayAllocator;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static com.facebook.presto.block.BlockAssertions.Encoding.DICTIONARY;
import static com.facebook.presto.block.BlockAssertions.Encoding.RUN_LENGTH;
import static com.facebook.presto.block.BlockAssertions.assertBlockEquals;
import static com.facebook.presto.block.BlockAssertions.createAllNullsBlock;
import static com.facebook.presto.block.BlockAssertions.createMapType;
import static com.facebook.presto.block.BlockAssertions.createRandomBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createRandomDictionaryBlock;
import static com.facebook.presto.block.BlockAssertions.createRandomIntsBlock;
import static com.facebook.presto.block.BlockAssertions.createRandomLongDecimalsBlock;
import static com.facebook.presto.block.BlockAssertions.createRandomLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createRandomShortDecimalsBlock;
import static com.facebook.presto.block.BlockAssertions.createRandomSmallintsBlock;
import static com.facebook.presto.block.BlockAssertions.createRandomStringBlock;
import static com.facebook.presto.block.BlockAssertions.createRleBlockWithRandomValue;
import static com.facebook.presto.block.BlockAssertions.wrapBlock;
import static com.facebook.presto.common.block.ArrayBlock.fromElementBlock;
import static com.facebook.presto.common.block.BlockSerdeUtil.readBlock;
import static com.facebook.presto.common.block.MapBlock.fromKeyValueBlock;
import static com.facebook.presto.common.block.MethodHandleUtil.compose;
import static com.facebook.presto.common.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.common.block.RowBlock.fromFieldBlocks;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.Decimals.MAX_SHORT_PRECISION;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RowType.withDefaultFieldNames;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.repartition.AbstractBlockEncodingBuffer.createBlockEncodingBuffers;
import static com.facebook.presto.operator.repartition.OptimizedPartitionedOutputOperator.decodeBlock;
import static com.facebook.presto.testing.TestingEnvironment.TYPE_MANAGER;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestBlockEncodingBuffers
{
    private static final int POSITIONS_PER_BLOCK = 1000;

    @Test
    public void testBigint()
    {
        testBlock(BIGINT, createRandomLongsBlock(POSITIONS_PER_BLOCK, 0.2f));
    }

    @Test
    public void testLongDecimal()
    {
        testBlock(createDecimalType(MAX_SHORT_PRECISION + 1), createRandomLongDecimalsBlock(POSITIONS_PER_BLOCK, 0.2f));
    }

    @Test
    public void testInteger()
    {
        testBlock(INTEGER, createRandomIntsBlock(POSITIONS_PER_BLOCK, 0.2f));
    }

    @Test
    public void testSmallint()
    {
        testBlock(SMALLINT, createRandomSmallintsBlock(POSITIONS_PER_BLOCK, 0.2f));
    }

    @Test
    public void testBoolean()
    {
        testBlock(BOOLEAN, createRandomBooleansBlock(POSITIONS_PER_BLOCK, 0.2f));
    }

    @Test
    public void testVarchar()
    {
        testBlock(VARCHAR, createRandomStringBlock(POSITIONS_PER_BLOCK, 0.2f, 10));
        testBlock(VARCHAR, createRandomStringBlock(POSITIONS_PER_BLOCK, 0.2f, 0));
        testBlock(VARCHAR, createRleBlockWithRandomValue(createRandomStringBlock(POSITIONS_PER_BLOCK, 0.2f, 0), POSITIONS_PER_BLOCK));
    }

    @Test
    public void testArray()
    {
        testNestedBlock(new ArrayType(BIGINT));
        testNestedBlock(new ArrayType(createDecimalType(MAX_SHORT_PRECISION)));
        testNestedBlock(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)));
        testNestedBlock(new ArrayType(INTEGER));
        testNestedBlock(new ArrayType(SMALLINT));
        testNestedBlock(new ArrayType(BOOLEAN));
        testNestedBlock(new ArrayType(VARCHAR));

        testNestedBlock(new ArrayType(new ArrayType(BIGINT)));
        testNestedBlock(new ArrayType(new ArrayType(createDecimalType(MAX_SHORT_PRECISION))));
        testNestedBlock(new ArrayType(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1))));
        testNestedBlock(new ArrayType(new ArrayType(INTEGER)));
        testNestedBlock(new ArrayType(new ArrayType(SMALLINT)));
        testNestedBlock(new ArrayType(new ArrayType(BOOLEAN)));
        testNestedBlock(new ArrayType(new ArrayType(VARCHAR)));

        testNestedBlock(new ArrayType(new ArrayType(new ArrayType(BIGINT))));
        testNestedBlock(new ArrayType(new ArrayType(new ArrayType(createDecimalType(MAX_SHORT_PRECISION)))));
        testNestedBlock(new ArrayType(new ArrayType(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)))));
        testNestedBlock(new ArrayType(new ArrayType(new ArrayType(INTEGER))));
        testNestedBlock(new ArrayType(new ArrayType(new ArrayType(SMALLINT))));
        testNestedBlock(new ArrayType(new ArrayType(new ArrayType(BOOLEAN))));
        testNestedBlock(new ArrayType(new ArrayType(new ArrayType(VARCHAR))));
    }

    @Test
    public void testMap()
    {
        testNestedBlock(createMapType(BIGINT, BIGINT));
        testNestedBlock(createMapType(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1)));
        testNestedBlock(createMapType(INTEGER, INTEGER));
        testNestedBlock(createMapType(SMALLINT, SMALLINT));
        testNestedBlock(createMapType(BOOLEAN, BOOLEAN));
        testNestedBlock(createMapType(VARCHAR, VARCHAR));

        testNestedBlock(createMapType(BIGINT, createMapType(BIGINT, BIGINT)));
        testNestedBlock(createMapType(createDecimalType(MAX_SHORT_PRECISION + 1), createMapType(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1))));
        testNestedBlock(createMapType(INTEGER, createMapType(INTEGER, INTEGER)));
        testNestedBlock(createMapType(SMALLINT, createMapType(SMALLINT, SMALLINT)));
        testNestedBlock(createMapType(BOOLEAN, createMapType(BOOLEAN, BOOLEAN)));
        testNestedBlock(createMapType(VARCHAR, createMapType(VARCHAR, VARCHAR)));

        testNestedBlock(createMapType(createMapType(createMapType(BIGINT, BIGINT), BIGINT), createMapType(BIGINT, BIGINT)));
        testNestedBlock(createMapType(
                createMapType(createMapType(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1)), createDecimalType(MAX_SHORT_PRECISION + 1)),
                createMapType(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1))));
        testNestedBlock(createMapType(createMapType(createMapType(INTEGER, INTEGER), INTEGER), createMapType(INTEGER, INTEGER)));
        testNestedBlock(createMapType(createMapType(createMapType(SMALLINT, SMALLINT), SMALLINT), createMapType(SMALLINT, SMALLINT)));
        testNestedBlock(createMapType(createMapType(createMapType(BOOLEAN, BOOLEAN), BOOLEAN), createMapType(BOOLEAN, BOOLEAN)));
        testNestedBlock(createMapType(createMapType(createMapType(VARCHAR, VARCHAR), VARCHAR), createMapType(VARCHAR, VARCHAR)));

        testNestedBlock(createMapType(BIGINT, new ArrayType(BIGINT)));
        testNestedBlock(createMapType(createDecimalType(MAX_SHORT_PRECISION + 1), new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1))));
        testNestedBlock(createMapType(INTEGER, new ArrayType(INTEGER)));
        testNestedBlock(createMapType(SMALLINT, new ArrayType(SMALLINT)));
        testNestedBlock(createMapType(BOOLEAN, new ArrayType(BOOLEAN)));
        testNestedBlock(createMapType(VARCHAR, new ArrayType(VARCHAR)));
    }

    @Test
    public void testRow()
    {
        testNestedBlock(withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT, BIGINT)));
        testNestedBlock(withDefaultFieldNames(ImmutableList.of(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1))));
        testNestedBlock(withDefaultFieldNames(ImmutableList.of(INTEGER, INTEGER, INTEGER)));
        testNestedBlock(withDefaultFieldNames(ImmutableList.of(SMALLINT, SMALLINT, SMALLINT)));
        testNestedBlock(withDefaultFieldNames(ImmutableList.of(BOOLEAN, BOOLEAN, BOOLEAN)));
        testNestedBlock(withDefaultFieldNames(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR)));

        testNestedBlock(withDefaultFieldNames(ImmutableList.of(BIGINT, withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT)))));
        testNestedBlock(withDefaultFieldNames(ImmutableList.of(createDecimalType(MAX_SHORT_PRECISION + 1), withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT)))));
        testNestedBlock(withDefaultFieldNames(ImmutableList.of(INTEGER, withDefaultFieldNames(ImmutableList.of(INTEGER, INTEGER)))));
        testNestedBlock(withDefaultFieldNames(ImmutableList.of(SMALLINT, withDefaultFieldNames(ImmutableList.of(SMALLINT, SMALLINT)))));
        testNestedBlock(withDefaultFieldNames(ImmutableList.of(BOOLEAN, withDefaultFieldNames(ImmutableList.of(BOOLEAN, BOOLEAN, BOOLEAN)))));
        testNestedBlock(withDefaultFieldNames(ImmutableList.of(VARCHAR, withDefaultFieldNames(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR)))));

        testNestedBlock(withDefaultFieldNames(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(BIGINT)), BIGINT)), BIGINT)));
        testNestedBlock(withDefaultFieldNames(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(createDecimalType(MAX_SHORT_PRECISION + 1))), createDecimalType(MAX_SHORT_PRECISION))), createDecimalType(MAX_SHORT_PRECISION - 1))));
        testNestedBlock(withDefaultFieldNames(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(INTEGER)), INTEGER)), INTEGER)));
        testNestedBlock(withDefaultFieldNames(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(SMALLINT)), SMALLINT)), SMALLINT)));
        testNestedBlock(withDefaultFieldNames(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(BOOLEAN)), BOOLEAN)), BOOLEAN)));
        testNestedBlock(withDefaultFieldNames(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(VARCHAR)), VARCHAR)), VARCHAR)));

        testNestedBlock(withDefaultFieldNames(ImmutableList.of(new ArrayType(BIGINT), new ArrayType(BIGINT))));
        testNestedBlock(withDefaultFieldNames(ImmutableList.of(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)), new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)))));
        testNestedBlock(withDefaultFieldNames(ImmutableList.of(new ArrayType(INTEGER), new ArrayType(INTEGER))));
        testNestedBlock(withDefaultFieldNames(ImmutableList.of(new ArrayType(SMALLINT), new ArrayType(SMALLINT))));
        testNestedBlock(withDefaultFieldNames(ImmutableList.of(new ArrayType(BOOLEAN), new ArrayType(BOOLEAN))));
        testNestedBlock(withDefaultFieldNames(ImmutableList.of(new ArrayType(VARCHAR), new ArrayType(VARCHAR))));

        testNestedBlock(withDefaultFieldNames(ImmutableList.of(new ArrayType(BIGINT), createMapType(BIGINT, BIGINT))));
        testNestedBlock(withDefaultFieldNames(ImmutableList.of(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)), createMapType(BIGINT, createDecimalType(MAX_SHORT_PRECISION + 1)))));
        testNestedBlock(withDefaultFieldNames(ImmutableList.of(new ArrayType(INTEGER), createMapType(INTEGER, INTEGER))));
        testNestedBlock(withDefaultFieldNames(ImmutableList.of(new ArrayType(SMALLINT), createMapType(SMALLINT, SMALLINT))));
        testNestedBlock(withDefaultFieldNames(ImmutableList.of(new ArrayType(BOOLEAN), createMapType(BOOLEAN, BOOLEAN))));
        testNestedBlock(withDefaultFieldNames(ImmutableList.of(new ArrayType(VARCHAR), createMapType(VARCHAR, VARCHAR))));
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

    private void testNestedBlock(Type type)
    {
        requireNonNull(type);

        assertSerialized(type, createAllNullsBlock(type, POSITIONS_PER_BLOCK));

        BlockStatus blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, false, ImmutableList.of());
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);

        blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, false, ImmutableList.of(DICTIONARY));
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);

        blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, false, ImmutableList.of(RUN_LENGTH));
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);

        blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, false, ImmutableList.of(DICTIONARY, DICTIONARY, RUN_LENGTH));
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);

        blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, false, ImmutableList.of(RUN_LENGTH, DICTIONARY, DICTIONARY));
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);

        blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, false, ImmutableList.of(DICTIONARY, RUN_LENGTH, DICTIONARY, RUN_LENGTH));
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);

        blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, false, ImmutableList.of(RUN_LENGTH, DICTIONARY, RUN_LENGTH, DICTIONARY));
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);

        blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, true, ImmutableList.of());
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);

        blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, true, ImmutableList.of(DICTIONARY));
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);

        blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, true, ImmutableList.of(RUN_LENGTH));
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);

        blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, true, ImmutableList.of(DICTIONARY, DICTIONARY, RUN_LENGTH));
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);

        blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, true, ImmutableList.of(RUN_LENGTH, DICTIONARY, DICTIONARY));
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);

        blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, true, ImmutableList.of(DICTIONARY, RUN_LENGTH, DICTIONARY, RUN_LENGTH));
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);

        blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, true, ImmutableList.of(RUN_LENGTH, DICTIONARY, RUN_LENGTH, DICTIONARY));
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);
    }

    private static void assertSerialized(Type type, Block block)
    {
        assertSerialized(type, block, null);
    }

    private static void assertSerialized(Type type, Block block, int[] expectedRowSizes)
    {
        Closer blockLeaseCloser = Closer.create();

        BlockFlattener flattener = new BlockFlattener(new UncheckedStackArrayAllocator());
        DecodedBlockNode decodedBlock = decodeBlock(flattener, blockLeaseCloser, block);

        BlockEncodingBuffer buffers = createBlockEncodingBuffers(decodedBlock, new UncheckedStackArrayAllocator(1000), false);

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
        buffer.setupDecodedBlocksAndPositions(decodedBlock, positions, positions.length, (int) decodedBlock.getRetainedSizeInBytes(), decodedBlock.getEstimatedSerializedSizeInBytes());

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

    private BlockStatus buildBlockStatusWithType(Type type, int positionCount, boolean isView, List<Encoding> wrappings)
    {
        return buildBlockStatusWithType(type, positionCount, isView, 0.2f, 0.2f, wrappings);
    }

    private BlockStatus buildBlockStatusWithType(Type type, int positionCount, boolean isView, float primitiveNullRate, float nestedNullRate, List<Encoding> wrappings)
    {
        BlockStatus blockStatus = null;

        if (isView) {
            positionCount *= 2;
        }

        if (type == BIGINT) {
            blockStatus = buildBigintBlockStatus(positionCount, primitiveNullRate);
        }
        else if (type instanceof DecimalType) {
            if (!((DecimalType) type).isShort()) {
                blockStatus = buildLongDecimalBlockStatus(positionCount, primitiveNullRate);
            }
            else {
                blockStatus = buildShortDecimalBlockStatus(positionCount, primitiveNullRate);
            }
        }
        else if (type == INTEGER) {
            blockStatus = buildIntegerBlockStatus(positionCount, primitiveNullRate);
        }
        else if (type == SMALLINT) {
            blockStatus = buildSmallintBlockStatus(positionCount, primitiveNullRate);
        }
        else if (type == BOOLEAN) {
            blockStatus = buildBooleanBlockStatus(positionCount, primitiveNullRate);
        }
        else if (type == VARCHAR) {
            blockStatus = buildVarcharBlockStatus(positionCount, primitiveNullRate, 10);
        }
        else {
            // Nested types
            // Build isNull and offsets of size positionCount
            boolean[] isNull = null;
            if (nestedNullRate > 0) {
                isNull = new boolean[positionCount];
            }
            int[] offsets = new int[positionCount + 1];

            for (int position = 0; position < positionCount; position++) {
                if (nestedNullRate > 0 && ThreadLocalRandom.current().nextDouble(1) < nestedNullRate) {
                    isNull[position] = true;
                    offsets[position + 1] = offsets[position];
                }
                else {
                    offsets[position + 1] = offsets[position] + (type instanceof RowType ? 1 : ThreadLocalRandom.current().nextInt(10) + 1);
                }
            }

            // Build the nested block of size offsets[positionCount].
            if (type instanceof ArrayType) {
                blockStatus = buildArrayBlockStatus((ArrayType) type, positionCount, isView, Optional.ofNullable(isNull), offsets, primitiveNullRate, nestedNullRate, wrappings);
            }
            else if (type instanceof MapType) {
                blockStatus = buildMapBlockStatus((MapType) type, positionCount, isView, Optional.ofNullable(isNull), offsets, primitiveNullRate, nestedNullRate, wrappings);
            }
            else if (type instanceof RowType) {
                blockStatus = buildRowBlockStatus((RowType) type, positionCount, isView, Optional.ofNullable(isNull), offsets, primitiveNullRate, nestedNullRate, wrappings);
            }
            else {
                throw new UnsupportedOperationException(format("type %s is not supported.", type));
            }
        }

        if (isView) {
            positionCount /= 2;
            int offset = positionCount / 2;
            Block blockView = blockStatus.block.getRegion(offset, positionCount);
            int[] expectedRowSizesView = Arrays.stream(blockStatus.expectedRowSizes, offset, offset + positionCount).toArray();
            blockStatus = new BlockStatus(blockView, expectedRowSizesView);
        }

        blockStatus = buildDictRleBlockStatus(blockStatus, positionCount, wrappings);

        return blockStatus;
    }

    private static BlockStatus buildBigintBlockStatus(int positionCount, float nullRate)
    {
        Block block = createRandomLongsBlock(positionCount, nullRate);
        int[] expectedRowSizes = IntStream.generate(() -> LongArrayBlockEncodingBuffer.POSITION_SIZE).limit(positionCount).toArray();

        return new BlockStatus(block, expectedRowSizes);
    }

    private static BlockStatus buildShortDecimalBlockStatus(int positionCount, float nullRate)
    {
        Block block = createRandomShortDecimalsBlock(positionCount, nullRate);
        int[] expectedRowSizes = IntStream.generate(() -> LongArrayBlockEncodingBuffer.POSITION_SIZE).limit(positionCount).toArray();

        return new BlockStatus(block, expectedRowSizes);
    }

    private static BlockStatus buildLongDecimalBlockStatus(int positionCount, float nullRate)
    {
        Block block = createRandomLongDecimalsBlock(positionCount, nullRate);
        int[] expectedRowSizes = IntStream.generate(() -> Int128ArrayBlockEncodingBuffer.POSITION_SIZE).limit(positionCount).toArray();

        return new BlockStatus(block, expectedRowSizes);
    }

    private BlockStatus buildIntegerBlockStatus(int positionCount, float nullRate)
    {
        Block block = createRandomIntsBlock(positionCount, nullRate);
        int[] expectedRowSizes = IntStream.generate(() -> IntArrayBlockEncodingBuffer.POSITION_SIZE).limit(positionCount).toArray();

        return new BlockStatus(block, expectedRowSizes);
    }

    private BlockStatus buildSmallintBlockStatus(int positionCount, float nullRate)
    {
        Block block = createRandomSmallintsBlock(positionCount, nullRate);
        int[] expectedRowSizes = IntStream.generate(() -> ShortArrayBlockEncodingBuffer.POSITION_SIZE).limit(positionCount).toArray();

        return new BlockStatus(block, expectedRowSizes);
    }

    private BlockStatus buildBooleanBlockStatus(int positionCount, float nullRate)
    {
        Block block = createRandomBooleansBlock(positionCount, nullRate);
        int[] expectedRowSizes = IntStream.generate(() -> ByteArrayBlockEncodingBuffer.POSITION_SIZE).limit(positionCount).toArray();

        return new BlockStatus(block, expectedRowSizes);
    }

    private BlockStatus buildVarcharBlockStatus(int positionCount, float nullRate, int maxStringLength)
    {
        Block block = createRandomStringBlock(positionCount, nullRate, maxStringLength);

        int[] expectedRowSizes = IntStream
                .range(0, positionCount)
                .map(i -> block.getSliceLength(i) + VariableWidthBlockEncodingBuffer.POSITION_SIZE)
                .toArray();

        return new BlockStatus(block, expectedRowSizes);
    }

    private BlockStatus buildDictRleBlockStatus(BlockStatus blockStatus, int positionCount, List<Encoding> wrappings)
    {
        checkArgument(blockStatus.block.getPositionCount() == positionCount);

        if (wrappings.isEmpty()) {
            return blockStatus;
        }

        BlockStatus wrappedBlockStatus = blockStatus;
        for (int i = wrappings.size() - 1; i >= 0; i--) {
            switch (wrappings.get(i)) {
                case DICTIONARY:
                    wrappedBlockStatus = buildDictionaryBlockStatus(blockStatus, positionCount);
                    break;
                case RUN_LENGTH:
                    wrappedBlockStatus = buildRleBlockStatus(blockStatus, positionCount);
                    break;
                default:
                    throw new IllegalArgumentException(format("wrappings %s is incorrect", wrappings));
            }
        }
        return wrappedBlockStatus;
    }

    private BlockStatus buildDictionaryBlockStatus(BlockStatus dictionary, int positionCount)
    {
        DictionaryBlock dictionaryBlock = createRandomDictionaryBlock(dictionary.block, positionCount);
        int[] mappedExpectedRowSizes = IntStream.range(0, positionCount).map(i -> dictionary.expectedRowSizes[dictionaryBlock.getId(i)]).toArray();
        return new BlockStatus(dictionaryBlock, mappedExpectedRowSizes);
    }

    private BlockStatus buildRleBlockStatus(BlockStatus blockStatus, int positionCount)
    {
        int[] expectedRowSizes = new int[positionCount];
        // When we contructed the Rle block, we chose the row at the middle.
        Arrays.setAll(expectedRowSizes, i -> blockStatus.expectedRowSizes[blockStatus.block.getPositionCount() / 2]);
        return new BlockStatus(
                createRleBlockWithRandomValue(blockStatus.block, positionCount),
                expectedRowSizes);
    }

    private BlockStatus buildArrayBlockStatus(
            ArrayType arrayType,
            int positionCount,
            boolean isView,
            Optional<boolean[]> isNull,
            int[] offsets,
            float primitiveNullRate,
            float nestedNullRate,
            List<Encoding> wrappings)
    {
        requireNonNull(isNull);
        requireNonNull(offsets);

        BlockStatus blockStatus;

        BlockStatus valuesBlockStatus = buildBlockStatusWithType(
                arrayType.getElementType(),
                offsets[positionCount],
                isView,
                primitiveNullRate,
                nestedNullRate,
                wrappings);

        int[] expectedRowSizes = IntStream.range(0, positionCount)
                .map(i -> ArrayBlockEncodingBuffer.POSITION_SIZE + Arrays.stream(valuesBlockStatus.expectedRowSizes, offsets[i], offsets[i + 1]).sum())
                .toArray();

        blockStatus = new BlockStatus(
                fromElementBlock(positionCount, isNull, offsets, valuesBlockStatus.block),
                expectedRowSizes);
        return blockStatus;
    }

    private BlockStatus buildMapBlockStatus(
            MapType mapType,
            int positionCount,
            boolean isView,
            Optional<boolean[]> isNull,
            int[] offsets,
            float primitiveNullRate,
            float nestedNullRate,
            List<Encoding> wrappings)
    {
        BlockStatus blockStatus;

        BlockStatus keyBlockStatus = buildBlockStatusWithType(
                mapType.getKeyType(),
                offsets[positionCount],
                isView,
                0.0f,
                0.0f,
                wrappings);
        BlockStatus valueBlockStatus = buildBlockStatusWithType(
                mapType.getValueType(),
                offsets[positionCount],
                isView,
                primitiveNullRate,
                nestedNullRate,
                wrappings);

        int[] expectedKeySizes = keyBlockStatus.expectedRowSizes;
        int[] expectedValueSizes = valueBlockStatus.expectedRowSizes;
        // Use expectedKeySizes for the total size for both key and values
        Arrays.setAll(expectedKeySizes, i -> expectedKeySizes[i] + expectedValueSizes[i]);
        int[] expectedRowSizes = IntStream.range(0, positionCount)
                .map(i -> MapBlockEncodingBuffer.POSITION_SIZE + Arrays.stream(expectedKeySizes, offsets[i], offsets[i + 1]).sum())
                .toArray();

        Type keyType = mapType.getKeyType();
        MethodHandle keyNativeEquals = TYPE_MANAGER.resolveOperator(OperatorType.EQUAL, ImmutableList.of(keyType, keyType));
        MethodHandle keyBlockNativeEquals = compose(keyNativeEquals, nativeValueGetter(keyType));
        MethodHandle keyNativeHashCode = TYPE_MANAGER.resolveOperator(OperatorType.HASH_CODE, ImmutableList.of(keyType));
        MethodHandle keyBlockHashCode = compose(keyNativeHashCode, nativeValueGetter(keyType));

        blockStatus = new BlockStatus(
                fromKeyValueBlock(
                        positionCount,
                        isNull,
                        offsets,
                        keyBlockStatus.block,
                        valueBlockStatus.block,
                        mapType,
                        keyBlockNativeEquals,
                        keyNativeHashCode,
                        keyBlockHashCode),
                expectedRowSizes);
        return blockStatus;
    }

    private BlockStatus buildRowBlockStatus(
            RowType rowType,
            int positionCount,
            boolean isView,
            Optional<boolean[]> isNull,
            int[] offsets,
            float primitiveNullRate,
            float nestedNullRate,
            List<Encoding> wrappings)
    {
        requireNonNull(isNull);

        BlockStatus blockStatus;
        int[] expectedTotalFieldSizes = new int[positionCount];

        List<Type> fieldTypes = rowType.getTypeParameters();
        Block[] fieldBlocks = new Block[fieldTypes.size()];

        for (int i = 0; i < fieldBlocks.length; i++) {
            BlockStatus fieldBlockStatus = buildBlockStatusWithType(
                    fieldTypes.get(i),
                    positionCount,
                    isView,
                    primitiveNullRate,
                    nestedNullRate,
                    wrappings);
            fieldBlocks[i] = fieldBlockStatus.block;
            Arrays.setAll(expectedTotalFieldSizes, j -> expectedTotalFieldSizes[j] + fieldBlockStatus.expectedRowSizes[j]);
        }

        int[] expectedRowSizes = IntStream.range(0, positionCount)
                .map(i -> RowBlockEncodingBuffer.POSITION_SIZE + Arrays.stream(expectedTotalFieldSizes, offsets[i], offsets[i + 1]).sum())
                .toArray();

        blockStatus = new BlockStatus(fromFieldBlocks(positionCount, isNull, fieldBlocks), expectedRowSizes);
        return blockStatus;
    }

    private static class BlockStatus
    {
        private final Block block;
        private final int[] expectedRowSizes;

        BlockStatus(Block block, int[] expectedRowSizes)
        {
            this.block = requireNonNull(block, "block is null");
            this.expectedRowSizes = requireNonNull(expectedRowSizes, "expectedRowSizes is null");
        }
    }
}
