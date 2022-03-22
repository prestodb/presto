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

import com.facebook.presto.common.block.AbstractMapBlock.HashTables;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockBuilderStatus;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.block.DictionaryId;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import static com.facebook.airlift.testing.Assertions.assertBetweenInclusive;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Arrays.fill;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test
public abstract class AbstractTestBlock
{
    private static final BlockEncodingSerde BLOCK_ENCODING_SERDE = new BlockEncodingManager();

    protected <T> void assertBlock(Block block, Supplier<BlockBuilder> newBlockBuilder, T[] expectedValues)
    {
        assertBlockSize(block);
        assertRetainedSize(block);

        assertBlockPositions(block, newBlockBuilder, expectedValues);
        assertBlockPositions(copyBlockViaBlockSerde(block), newBlockBuilder, expectedValues);
        assertBlockPositions(copyBlockViaWritePositionTo(block, newBlockBuilder), newBlockBuilder, expectedValues);
        if (expectedValues.getClass().getComponentType().isArray() ||
                expectedValues.getClass().getComponentType() == List.class ||
                expectedValues.getClass().getComponentType() == Map.class) {
            assertBlockPositions(copyBlockViaWriteStructure(block, newBlockBuilder), newBlockBuilder, expectedValues);
        }

        Block blockWithNull = copyBlockViaBlockSerde(block).appendNull();
        T[] expectedValuesWithNull = Arrays.copyOf(expectedValues, expectedValues.length + 1);
        assertBlockPositions(blockWithNull, newBlockBuilder, expectedValuesWithNull);

        assertBlockSize(block);
        assertRetainedSize(block);

        try {
            block.isNull(-1);
            fail("expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) {
        }
        try {
            block.isNull(block.getPositionCount());
            fail("expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) {
        }
    }

    private void assertRetainedSize(Block block)
    {
        long retainedSize = ClassLayout.parseClass(block.getClass()).instanceSize();
        Field[] fields = block.getClass().getDeclaredFields();
        try {
            for (Field field : fields) {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                Class<?> type = field.getType();
                if (type.isPrimitive()) {
                    continue;
                }

                field.setAccessible(true);

                if (type == Slice.class) {
                    Slice slice = (Slice) field.get(block);
                    if (slice != null) {
                        retainedSize += slice.getRetainedSize();
                    }
                }
                else if (type == BlockBuilderStatus.class) {
                    if (field.get(block) != null) {
                        retainedSize += BlockBuilderStatus.INSTANCE_SIZE;
                    }
                }
                else if (type == BlockBuilder.class || type == Block.class) {
                    retainedSize += ((Block) field.get(block)).getRetainedSizeInBytes();
                }
                else if (type == BlockBuilder[].class || type == Block[].class) {
                    Block[] blocks = (Block[]) field.get(block);
                    for (Block innerBlock : blocks) {
                        assertRetainedSize(innerBlock);
                        retainedSize += innerBlock.getRetainedSizeInBytes();
                    }
                }
                else if (type == SliceOutput.class) {
                    retainedSize += ((SliceOutput) field.get(block)).getRetainedSize();
                }
                else if (type == int[].class) {
                    retainedSize += sizeOf((int[]) field.get(block));
                }
                else if (type == boolean[].class) {
                    retainedSize += sizeOf((boolean[]) field.get(block));
                }
                else if (type == byte[].class) {
                    retainedSize += sizeOf((byte[]) field.get(block));
                }
                else if (type == long[].class) {
                    retainedSize += sizeOf((long[]) field.get(block));
                }
                else if (type == short[].class) {
                    retainedSize += sizeOf((short[]) field.get(block));
                }
                else if (type == DictionaryId.class) {
                    retainedSize += ClassLayout.parseClass(DictionaryId.class).instanceSize();
                }
                else if (type == HashTables.class) {
                    retainedSize += ((HashTables) field.get(block)).getRetainedSizeInBytes();
                }
                else if (type == MethodHandle.class) {
                    // MethodHandles are only used in MapBlock/MapBlockBuilder,
                    // and they are shared among blocks created by the same MapType.
                    // So we don't account for the memory held onto by MethodHandle instances.
                    // Otherwise, we will be counting it multiple times.
                }
                else {
                    throw new IllegalArgumentException(format("Unknown type encountered: %s", type));
                }
            }
        }
        catch (IllegalAccessException t) {
            throw new RuntimeException(t);
        }
        assertEquals(block.getRetainedSizeInBytes(), retainedSize);
    }

    protected <T> void assertBlockFilteredPositions(T[] expectedValues, Block block, Supplier<BlockBuilder> newBlockBuilder, int... positions)
    {
        Block filteredBlock = block.copyPositions(positions, 0, positions.length);
        T[] filteredExpectedValues = filter(expectedValues, positions);
        assertEquals(filteredBlock.getPositionCount(), positions.length);
        assertBlock(filteredBlock, newBlockBuilder, filteredExpectedValues);
    }

    private static <T> T[] filter(T[] expectedValues, int[] positions)
    {
        @SuppressWarnings("unchecked")
        T[] prunedExpectedValues = (T[]) Array.newInstance(expectedValues.getClass().getComponentType(), positions.length);
        for (int i = 0; i < prunedExpectedValues.length; i++) {
            prunedExpectedValues[i] = expectedValues[positions[i]];
        }
        return prunedExpectedValues;
    }

    private <T> void assertBlockPositions(Block block, Supplier<BlockBuilder> newBlockBuilder, T[] expectedValues)
    {
        assertEquals(block.getPositionCount(), expectedValues.length);
        for (int position = 0; position < block.getPositionCount(); position++) {
            assertBlockPosition(block, newBlockBuilder, position, expectedValues[position], expectedValues.getClass().getComponentType());
        }
    }

    protected List<Block> splitBlock(Block block, int count)
    {
        double sizePerSplit = block.getPositionCount() * 1.0 / count;
        ImmutableList.Builder<Block> result = ImmutableList.builder();
        for (int i = 0; i < count; i++) {
            int startPosition = toIntExact(Math.round(sizePerSplit * i));
            int endPosition = toIntExact(Math.round(sizePerSplit * (i + 1)));
            result.add(block.getRegion(startPosition, endPosition - startPosition));
        }
        return result.build();
    }

    private void assertBlockSize(Block block)
    {
        // Asserting on `block` is not very effective because most blocks passed to this method is compact.
        // Therefore, we split the `block` into two and assert again.
        //------------------Test Whole Block Sizes---------------------------------------------------
        // Assert sizeInBytes for the whole block.
        long expectedBlockSize = copyBlockViaBlockSerde(block).getSizeInBytes();
        assertEquals(block.getSizeInBytes(), expectedBlockSize);
        assertEquals(block.getRegionSizeInBytes(0, block.getPositionCount()), expectedBlockSize);

        // Assert logicalSize for the whole block. Note that copyBlockViaBlockSerde would flatten DictionaryBlock or RleBlock
        long logicalSizeInBytes = block.getLogicalSizeInBytes();

        long expectedLogicalBlockSize = copyBlockViaBlockSerde(block).getLogicalSizeInBytes();
        assertEquals(logicalSizeInBytes, expectedLogicalBlockSize);
        assertEquals(block.getRegionLogicalSizeInBytes(0, block.getPositionCount()), expectedLogicalBlockSize);

        // Assert approximateLogicalSize for the whole block
        long approximateLogicalSizeInBytes = block.getApproximateRegionLogicalSizeInBytes(0, block.getPositionCount());

        long expectedApproximateLogicalBlockSize = expectedLogicalBlockSize;
        if (block instanceof DictionaryBlock) {
            int dictionaryPositionCount = ((DictionaryBlock) block).getDictionary().getPositionCount();
            expectedApproximateLogicalBlockSize = ((DictionaryBlock) block).getDictionary().getApproximateRegionLogicalSizeInBytes(0, dictionaryPositionCount) * block.getPositionCount() / dictionaryPositionCount;
        }
        assertEquals(approximateLogicalSizeInBytes, expectedApproximateLogicalBlockSize);

        //------------------Test First Half Sizes---------------------------------------------------
        List<Block> splitBlock = splitBlock(block, 2);
        Block firstHalf = splitBlock.get(0);
        int firstHalfPositionCount = firstHalf.getPositionCount();

        // Assert sizeInBytes for the firstHalf block.
        long expectedFirstHalfSize = copyBlockViaBlockSerde(firstHalf).getSizeInBytes();
        assertEquals(firstHalf.getSizeInBytes(), expectedFirstHalfSize);
        assertEquals(block.getRegionSizeInBytes(0, firstHalfPositionCount), expectedFirstHalfSize);

        // Assert logicalSize for the firstHalf block
        long firstHalfLogicalSizeInBytes = firstHalf.getLogicalSizeInBytes();
        long expectedFirstHalfLogicalSize = copyBlockViaBlockSerde(firstHalf).getLogicalSizeInBytes();
        assertEquals(firstHalfLogicalSizeInBytes, expectedFirstHalfLogicalSize);
        assertEquals(firstHalf.getRegionLogicalSizeInBytes(0, firstHalfPositionCount), expectedFirstHalfLogicalSize);

        // Assert approximateLogicalSize for the firstHalf block using logicalSize
        long approximateFirstHalfLogicalSize = firstHalf.getApproximateRegionLogicalSizeInBytes(0, firstHalfPositionCount);

        long expectedApproximateFirstHalfLogicalSize = expectedFirstHalfLogicalSize;
        if (firstHalf instanceof DictionaryBlock) {
            int dictionaryPositionCount = ((DictionaryBlock) firstHalf).getDictionary().getPositionCount();
            expectedApproximateFirstHalfLogicalSize = ((DictionaryBlock) firstHalf).getDictionary().getApproximateRegionLogicalSizeInBytes(0, dictionaryPositionCount) * firstHalfPositionCount / dictionaryPositionCount;
        }
        assertEquals(approximateFirstHalfLogicalSize, expectedApproximateFirstHalfLogicalSize);

        // Assert approximateLogicalSize for the firstHalf block using the ratio of firstHalf logicalSize vs whole block logicalSize
        long expectedApproximateFirstHalfLogicalSizeFromUnsplittedBlock = logicalSizeInBytes == 0 ?
                approximateLogicalSizeInBytes :
                approximateLogicalSizeInBytes * firstHalfLogicalSizeInBytes / logicalSizeInBytes;
        assertBetweenInclusive(
                approximateFirstHalfLogicalSize,
                // Allow for some error margins due to skew in blocks
                min(expectedApproximateFirstHalfLogicalSizeFromUnsplittedBlock - 3, (long) (expectedApproximateFirstHalfLogicalSizeFromUnsplittedBlock * 0.7)),
                max(expectedApproximateFirstHalfLogicalSizeFromUnsplittedBlock + 3, (long) (expectedApproximateFirstHalfLogicalSizeFromUnsplittedBlock * 1.3)));

        //------------------Test Second Half Sizes---------------------------------------------------
        Block secondHalf = splitBlock.get(1);
        int secondHalfPositionCount = secondHalf.getPositionCount();

        // Assert sizeInBytes for the secondHalf block.
        long expectedSecondHalfSize = copyBlockViaBlockSerde(secondHalf).getSizeInBytes();
        assertEquals(secondHalf.getSizeInBytes(), expectedSecondHalfSize);
        assertEquals(block.getRegionSizeInBytes(firstHalfPositionCount, secondHalfPositionCount), expectedSecondHalfSize);

        // Assert logicalSize for the secondHalf block.
        long secondHalfLogicalSizeInBytes = secondHalf.getLogicalSizeInBytes();
        long expectedSecondHalfLogicalSize = copyBlockViaBlockSerde(secondHalf).getLogicalSizeInBytes();
        assertEquals(secondHalfLogicalSizeInBytes, expectedSecondHalfLogicalSize);
        assertEquals(secondHalf.getRegionLogicalSizeInBytes(0, secondHalfPositionCount), expectedSecondHalfLogicalSize);

        // Assert approximateLogicalSize for the secondHalf block using logicalSize
        long approximateSecondHalfLogicalSize = secondHalf.getApproximateRegionLogicalSizeInBytes(0, secondHalfPositionCount);

        long expectedApproximateSecondHalfLogicalSize = copyBlockViaBlockSerde(secondHalf).getApproximateRegionLogicalSizeInBytes(0, secondHalfPositionCount);
        if (secondHalf instanceof DictionaryBlock) {
            int dictionaryPositionCount = ((DictionaryBlock) secondHalf).getDictionary().getPositionCount();
            expectedApproximateSecondHalfLogicalSize = ((DictionaryBlock) secondHalf).getDictionary().getApproximateRegionLogicalSizeInBytes(0, dictionaryPositionCount) * secondHalfPositionCount / dictionaryPositionCount;
        }
        assertEquals(approximateSecondHalfLogicalSize, expectedApproximateSecondHalfLogicalSize);

        // Assert approximateLogicalSize for the secondHalf block using the ratio of firstHalf logicalSize vs whole block logicalSize
        long expectedApproximateSecondHalfLogicalSizeFromUnsplittedBlock = logicalSizeInBytes == 0 ?
                approximateLogicalSizeInBytes :
                approximateLogicalSizeInBytes * secondHalfLogicalSizeInBytes / logicalSizeInBytes;
        assertBetweenInclusive(
                approximateSecondHalfLogicalSize,
                // Allow for some error margins due to skew in blocks
                min(expectedApproximateSecondHalfLogicalSizeFromUnsplittedBlock - 3, (long) (expectedApproximateSecondHalfLogicalSizeFromUnsplittedBlock * 0.7)),
                max(expectedApproximateSecondHalfLogicalSizeFromUnsplittedBlock + 3, (long) (expectedApproximateSecondHalfLogicalSizeFromUnsplittedBlock * 1.3)));

        //----------------Test getPositionsSizeInBytes----------------------------------------
        boolean[] positions = new boolean[block.getPositionCount()];
        fill(positions, 0, firstHalfPositionCount, true);
        assertEquals(block.getPositionsSizeInBytes(positions), expectedFirstHalfSize);
        fill(positions, true);
        assertEquals(block.getPositionsSizeInBytes(positions), expectedBlockSize);
        fill(positions, 0, firstHalfPositionCount, false);
        assertEquals(block.getPositionsSizeInBytes(positions), expectedSecondHalfSize);
    }

    // expectedValueType is required since otherwise the expected value type is unknown when expectedValue is null.
    protected <T> void assertBlockPosition(Block block, Supplier<BlockBuilder> newBlockBuilder, int position, T expectedValue, Class<?> expectedValueType)
    {
        assertPositionValue(block, position, expectedValue);
        assertPositionValue(block.getSingleValueBlock(position), 0, expectedValue);

        assertPositionValue(block.getRegion(position, 1), 0, expectedValue);
        assertPositionValue(block.getRegion(0, position + 1), position, expectedValue);
        assertPositionValue(block.getRegion(position, block.getPositionCount() - position), 0, expectedValue);

        assertPositionValue(copyBlockViaBlockSerde(block.getRegion(position, 1)), 0, expectedValue);
        assertPositionValue(copyBlockViaBlockSerde(block.getRegion(0, position + 1)), position, expectedValue);
        assertPositionValue(copyBlockViaBlockSerde(block.getRegion(position, block.getPositionCount() - position)), 0, expectedValue);

        assertPositionValue(copyBlockViaWritePositionTo(block.getRegion(position, 1), newBlockBuilder), 0, expectedValue);
        assertPositionValue(copyBlockViaWritePositionTo(block.getRegion(0, position + 1), newBlockBuilder), position, expectedValue);
        assertPositionValue(copyBlockViaWritePositionTo(block.getRegion(position, block.getPositionCount() - position), newBlockBuilder), 0, expectedValue);

        if (expectedValueType.isArray() || expectedValueType == List.class || expectedValueType == Map.class) {
            assertPositionValue(copyBlockViaWriteStructure(block.getRegion(position, 1), newBlockBuilder), 0, expectedValue);
            assertPositionValue(copyBlockViaWriteStructure(block.getRegion(0, position + 1), newBlockBuilder), position, expectedValue);
            assertPositionValue(copyBlockViaWriteStructure(block.getRegion(position, block.getPositionCount() - position), newBlockBuilder), 0, expectedValue);
        }

        assertPositionValue(block.copyRegion(position, 1), 0, expectedValue);
        assertPositionValue(block.copyRegion(0, position + 1), position, expectedValue);
        assertPositionValue(block.copyRegion(position, block.getPositionCount() - position), 0, expectedValue);

        assertPositionValue(block.copyPositions(new int[] {position}, 0, 1), 0, expectedValue);
    }

    private <T> void assertPositionValue(Block block, int position, T expectedValue)
    {
        assertCheckedPositionValue(block, position, expectedValue);
        assertPositionValueUnchecked(block, position + block.getOffsetBase(), expectedValue);
    }

    protected <T> void assertCheckedPositionValue(Block block, int position, T expectedValue)
    {
        assertPositionValueUnchecked(block, position + block.getOffsetBase(), expectedValue);
        if (expectedValue == null) {
            assertTrue(block.isNull(position));
            return;
        }

        assertFalse(block.isNull(position));

        if (expectedValue instanceof Slice) {
            Slice expectedSliceValue = (Slice) expectedValue;

            if (isByteAccessSupported() && expectedSliceValue.length() >= SIZE_OF_BYTE) {
                assertEquals(block.getByte(position), expectedSliceValue.getByte(0));
            }

            if (isShortAccessSupported() && expectedSliceValue.length() >= SIZE_OF_SHORT) {
                assertEquals(block.getShort(position), expectedSliceValue.getShort(0));
            }

            if (isIntAccessSupported() && expectedSliceValue.length() >= SIZE_OF_INT) {
                assertEquals(block.getInt(position), expectedSliceValue.getInt(0));
            }

            if (isIntAccessSupported() && expectedSliceValue.length() >= SIZE_OF_LONG) {
                assertEquals(block.getLong(position), expectedSliceValue.getLong(0));
            }

            if (isAlignedLongAccessSupported()) {
                for (int offset = 0; offset <= expectedSliceValue.length() - SIZE_OF_LONG; offset += SIZE_OF_LONG) {
                    assertEquals(block.getLong(position, offset), expectedSliceValue.getLong(offset));
                }
            }

            if (isSliceAccessSupported()) {
                assertEquals(block.getSliceLength(position), expectedSliceValue.length());
                assertSlicePosition(block, position, expectedSliceValue);
            }
        }
        else if (expectedValue instanceof long[]) {
            Block actual = block.getBlock(position);
            long[] expected = (long[]) expectedValue;
            assertEquals(actual.getPositionCount(), expected.length);
            for (int i = 0; i < expected.length; i++) {
                assertEquals(BIGINT.getLong(actual, i), expected[i]);
            }
        }
        else if (expectedValue instanceof Slice[]) {
            Block actual = block.getBlock(position);
            Slice[] expected = (Slice[]) expectedValue;
            assertEquals(actual.getPositionCount(), expected.length);
            for (int i = 0; i < expected.length; i++) {
                assertEquals(VARCHAR.getSlice(actual, i), expected[i]);
            }
        }
        else if (expectedValue instanceof long[][]) {
            Block actual = block.getBlock(position);
            long[][] expected = (long[][]) expectedValue;
            assertEquals(actual.getPositionCount(), expected.length);
            for (int i = 0; i < expected.length; i++) {
                assertPositionValue(actual, i, expected[i]);
            }
        }
        else {
            fail("Unexpected type: " + expectedValue.getClass().getSimpleName());
        }
    }

    protected <T> void assertPositionValueUnchecked(Block block, int internalPosition, T expectedValue)
    {
        if (expectedValue == null) {
            assertTrue(block.isNullUnchecked(internalPosition));
            return;
        }

        assertFalse(block.mayHaveNull() && block.isNullUnchecked(internalPosition));

        if (expectedValue instanceof Slice) {
            Slice expectedSliceValue = (Slice) expectedValue;

            if (isByteAccessSupported() && expectedSliceValue.length() >= SIZE_OF_BYTE) {
                assertEquals(block.getByteUnchecked(internalPosition), expectedSliceValue.getByte(0));
            }

            if (isShortAccessSupported() && expectedSliceValue.length() >= SIZE_OF_SHORT) {
                assertEquals(block.getShortUnchecked(internalPosition), expectedSliceValue.getShort(0));
            }

            if (isIntAccessSupported() && expectedSliceValue.length() >= SIZE_OF_INT) {
                assertEquals(block.getIntUnchecked(internalPosition), expectedSliceValue.getInt(0));
            }

            if (isIntAccessSupported() && expectedSliceValue.length() >= SIZE_OF_LONG) {
                assertEquals(block.getLongUnchecked(internalPosition), expectedSliceValue.getLong(0));
            }

            if (isAlignedLongAccessSupported()) {
                for (int offset = 0; offset <= expectedSliceValue.length() - SIZE_OF_LONG; offset += SIZE_OF_LONG) {
                    assertEquals(block.getLongUnchecked(internalPosition, offset), expectedSliceValue.getLong(offset));
                }
            }

            if (isSliceAccessSupported()) {
                assertEquals(block.getSliceLengthUnchecked(internalPosition), expectedSliceValue.length());
                assertSlicePositionUnchecked(block, internalPosition, expectedSliceValue);
            }
        }
        else if (expectedValue instanceof long[]) {
            Block actual = block.getBlockUnchecked(internalPosition);
            long[] expected = (long[]) expectedValue;
            assertEquals(actual.getPositionCount(), expected.length);
            for (int i = 0; i < actual.getPositionCount(); i++) {
                assertEquals(BIGINT.getLongUnchecked(actual, i + actual.getOffsetBase()), expected[i]);
            }
        }
        else if (expectedValue instanceof Slice[]) {
            Block actual = block.getBlockUnchecked(internalPosition);
            Slice[] expected = (Slice[]) expectedValue;
            assertEquals(actual.getPositionCount(), expected.length);
            for (int i = 0; i < expected.length; i++) {
                assertEquals(VARCHAR.getSlice(actual, i), expected[i]);
            }
        }
        else if (expectedValue instanceof long[][]) {
            Block actual = block.getBlockUnchecked(internalPosition);
            long[][] expected = (long[][]) expectedValue;
            assertEquals(actual.getPositionCount(), expected.length);
            for (int i = 0; i < expected.length; i++) {
                assertPositionValue(actual, i, expected[i]);
            }
        }
        else {
            throw new IllegalArgumentException();
        }
    }

    protected void assertSlicePosition(Block block, int position, Slice expectedSliceValue)
    {
        int length = block.getSliceLength(position);
        assertEquals(length, expectedSliceValue.length());

        Block expectedBlock = toSingeValuedBlock(expectedSliceValue);
        for (int offset = 0; offset < length - 3; offset++) {
            assertEquals(block.getSlice(position, offset, 3), expectedSliceValue.slice(offset, 3));
            assertTrue(block.bytesEqual(position, offset, expectedSliceValue, offset, 3));
            // if your tests fail here, please change your test to not use this value
            assertFalse(block.bytesEqual(position, offset, Slices.utf8Slice("XXX"), 0, 3));

            assertEquals(block.bytesCompare(position, offset, 3, expectedSliceValue, offset, 3), 0);
            assertTrue(block.bytesCompare(position, offset, 3, expectedSliceValue, offset, 2) > 0);
            Slice greaterSlice = createGreaterValue(expectedSliceValue, offset, 3);
            assertTrue(block.bytesCompare(position, offset, 3, greaterSlice, 0, greaterSlice.length()) < 0);

            assertTrue(block.equals(position, offset, expectedBlock, 0, offset, 3));
            assertEquals(block.compareTo(position, offset, 3, expectedBlock, 0, offset, 3), 0);

            BlockBuilder blockBuilder = VARBINARY.createBlockBuilder(null, 1);
            block.writeBytesTo(position, offset, 3, blockBuilder);
            blockBuilder.closeEntry();
            Block segment = blockBuilder.build();

            assertTrue(block.equals(position, offset, segment, 0, 0, 3));
        }
    }

    protected void assertSlicePositionUnchecked(Block block, int internalPosition, Slice expectedSliceValue)
    {
        int length = block.getSliceLengthUnchecked(internalPosition);
        assertEquals(length, expectedSliceValue.length());

        Block expectedBlock = toSingeValuedBlock(expectedSliceValue);
        for (int offset = 0; offset < length - 3; offset++) {
            assertEquals(block.getSliceUnchecked(internalPosition, offset, 3), expectedSliceValue.slice(offset, 3));
            assertTrue(block.bytesEqual(internalPosition - block.getOffsetBase(), offset, expectedSliceValue, offset, 3));
            assertFalse(block.bytesEqual(internalPosition - block.getOffsetBase(), offset, Slices.utf8Slice(UUID.randomUUID().toString()), 0, 3));

            assertEquals(block.bytesCompare(internalPosition - block.getOffsetBase(), offset, 3, expectedSliceValue, offset, 3), 0);
            assertTrue(block.bytesCompare(internalPosition - block.getOffsetBase(), offset, 3, expectedSliceValue, offset, 2) > 0);
            Slice greaterSlice = createGreaterValue(expectedSliceValue, offset, 3);
            assertTrue(block.bytesCompare(internalPosition - block.getOffsetBase(), offset, 3, greaterSlice, 0, greaterSlice.length()) < 0);

            assertTrue(block.equals(internalPosition - block.getOffsetBase(), offset, expectedBlock, 0, offset, 3));
            assertEquals(block.compareTo(internalPosition - block.getOffsetBase(), offset, 3, expectedBlock, 0, offset, 3), 0);

            BlockBuilder blockBuilder = VARBINARY.createBlockBuilder(null, 1);
            block.writeBytesTo(internalPosition - block.getOffsetBase(), offset, 3, blockBuilder);
            blockBuilder.closeEntry();
            Block segment = blockBuilder.build();

            assertTrue(block.equals(internalPosition - block.getOffsetBase(), offset, segment, 0, 0, 3));
        }
    }

    protected boolean isByteAccessSupported()
    {
        return true;
    }

    protected boolean isShortAccessSupported()
    {
        return true;
    }

    protected boolean isIntAccessSupported()
    {
        return true;
    }

    protected boolean isLongAccessSupported()
    {
        return true;
    }

    protected boolean isAlignedLongAccessSupported()
    {
        return false;
    }

    protected boolean isSliceAccessSupported()
    {
        return true;
    }

    private static Block copyBlockViaBlockSerde(Block block)
    {
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        BLOCK_ENCODING_SERDE.writeBlock(sliceOutput, block);
        return BLOCK_ENCODING_SERDE.readBlock(sliceOutput.slice().getInput());
    }

    private static Block copyBlockViaWritePositionTo(Block block, Supplier<BlockBuilder> newBlockBuilder)
    {
        BlockBuilder blockBuilder = newBlockBuilder.get();
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                blockBuilder.appendNull();
            }
            else {
                block.writePositionTo(i, blockBuilder);
            }
        }
        return blockBuilder.build();
    }

    private static Block copyBlockViaWriteStructure(Block block, Supplier<BlockBuilder> newBlockBuilder)
    {
        BlockBuilder blockBuilder = newBlockBuilder.get();
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.appendStructure(block.getBlock(i));
            }
        }
        return blockBuilder.build();
    }

    private static Block toSingeValuedBlock(Slice expectedValue)
    {
        BlockBuilder blockBuilder = VARBINARY.createBlockBuilder(null, 1, expectedValue.length());
        VARBINARY.writeSlice(blockBuilder, expectedValue);
        return blockBuilder.build();
    }

    private static Slice createGreaterValue(Slice expectedValue, int offset, int length)
    {
        DynamicSliceOutput greaterOutput = new DynamicSliceOutput(length + 1);
        greaterOutput.writeBytes(expectedValue, offset, length);
        greaterOutput.writeByte('_');
        return greaterOutput.slice();
    }

    protected static Slice[] createExpectedValues(int positionCount)
    {
        Slice[] expectedValues = new Slice[positionCount];
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = createExpectedValue(position);
        }
        return expectedValues;
    }

    protected static Slice createExpectedValue(int length)
    {
        DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(16);
        for (int index = 0; index < length; index++) {
            dynamicSliceOutput.writeByte(length * (index + 1));
        }
        return dynamicSliceOutput.slice();
    }

    protected static <T> T[] alternatingNullValues(T[] objects)
    {
        T[] objectsWithNulls = Arrays.copyOf(objects, objects.length * 2 + 1);
        for (int i = 0; i < objects.length; i++) {
            objectsWithNulls[i * 2] = null;
            objectsWithNulls[i * 2 + 1] = objects[i];
        }
        objectsWithNulls[objectsWithNulls.length - 1] = null;
        return objectsWithNulls;
    }

    protected static Slice[] createExpectedUniqueValues(int positionCount)
    {
        Slice[] expectedValues = new Slice[positionCount];
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = Slices.copyOf(createExpectedValue(position));
        }
        return expectedValues;
    }

    protected static void assertEstimatedDataSizeForStats(BlockBuilder blockBuilder, Slice[] expectedSliceValues)
    {
        Block block = blockBuilder.build();
        assertEquals(block.getPositionCount(), expectedSliceValues.length);
        for (int i = 0; i < block.getPositionCount(); i++) {
            int expectedSize = expectedSliceValues[i] == null ? 0 : expectedSliceValues[i].length();
            assertEquals(blockBuilder.getEstimatedDataSizeForStats(i), expectedSize);
            assertEquals(block.getEstimatedDataSizeForStats(i), expectedSize);
        }

        BlockBuilder nullValueBlockBuilder = blockBuilder.newBlockBuilderLike(null).appendNull();
        assertEquals(nullValueBlockBuilder.getEstimatedDataSizeForStats(0), 0);
        assertEquals(nullValueBlockBuilder.build().getEstimatedDataSizeForStats(0), 0);
    }

    protected static void testCopyRegionCompactness(Block block)
    {
        assertCompact(block.copyRegion(0, block.getPositionCount()));
        if (block.getPositionCount() > 0) {
            assertCompact(block.copyRegion(0, block.getPositionCount() - 1));
            assertCompact(block.copyRegion(1, block.getPositionCount() - 1));
        }
    }

    protected static void assertCompact(Block block)
    {
        assertSame(block.copyRegion(0, block.getPositionCount()), block);
    }

    protected static void assertNotCompact(Block block)
    {
        assertNotSame(block.copyRegion(0, block.getPositionCount()), block);
    }

    protected static void testCompactBlock(Block block)
    {
        assertCompact(block);
        testCopyRegionCompactness(block);
    }

    protected static void testIncompactBlock(Block block)
    {
        assertNotCompact(block);
        testCopyRegionCompactness(block);
    }
}
