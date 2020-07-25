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
package com.facebook.presto.common.block;

import com.facebook.presto.common.type.Type;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.facebook.presto.common.block.DictionaryId.randomDictionaryId;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestBlockFlattenner
{
    private ArrayAllocator allocator;
    private BlockFlattener flattener;

    @BeforeClass
    public void setup()
    {
        this.allocator = new CountingArrayAllocator();
        this.flattener = new BlockFlattener(allocator);
    }

    @Test
    public void testLongArrayIdentityDecode()
    {
        Block block = createLongArrayBlock(1, 2, 3, 4);
        try (BlockLease blockLease = flattener.flatten(block)) {
            Block flattenedBlock = blockLease.get();
            assertSame(flattenedBlock, block);
        }
    }

    @Test
    public void testNestedDictionaryRLELongArray()
    {
        DictionaryBlock block = createTestDictionaryBlock(createTestRleBlock(createLongArrayBlock(4), 3));
        assertFlattenNumericTypeBlock(BIGINT, block, 1, LongArrayBlock.class);
    }

    @Test
    public void testNestedDictionaryDictionaryLongArray()
    {
        DictionaryBlock block = createTestDictionaryBlock(createTestDictionaryBlock(createLongArrayBlock(1, 2, 3, 4, 5)));
        assertFlattenNumericTypeBlock(BIGINT, block, 5, LongArrayBlock.class);
    }

    @Test
    public void testNestedDictionaryRleDictionaryLongArray()
    {
        DictionaryBlock block = createTestDictionaryBlock(createTestRleBlock(createTestDictionaryBlock(createLongArrayBlock(1)), 5));
        assertFlattenNumericTypeBlock(BIGINT, block, 1, LongArrayBlock.class);
    }

    @Test
    public void testIntArrayIdentityDecode()
    {
        Block block = createIntArrayBlock(1, 2, 3, 4);
        try (BlockLease blockLease = flattener.flatten(block)) {
            Block flattenedBlock = blockLease.get();
            assertSame(flattenedBlock, block);
        }
    }

    @Test
    public void testNestedDictionaryRLEIntArray()
    {
        DictionaryBlock block = createTestDictionaryBlock(createTestRleBlock(createIntArrayBlock(4), 3));
        assertFlattenNumericTypeBlock(INTEGER, block, 1, IntArrayBlock.class);
    }

    @Test
    public void testShortArrayIdentityDecode()
    {
        Block block = createShortArrayBlock(1, 2, 3, 4);
        try (BlockLease blockLease = flattener.flatten(block)) {
            Block flattenedBlock = blockLease.get();
            assertSame(flattenedBlock, block);
        }
    }

    @Test
    public void testNestedDictionaryRLEShortArray()
    {
        DictionaryBlock block = createTestDictionaryBlock(createTestRleBlock(createShortArrayBlock(4), 3));
        assertFlattenNumericTypeBlock(SMALLINT, block, 1, ShortArrayBlock.class);
    }

    @Test
    public void testByteArrayIdentityDecode()
    {
        Block block = createByteArrayBlock(1, 2, 3, 4);
        try (BlockLease blockLease = flattener.flatten(block)) {
            Block flattenedBlock = blockLease.get();
            assertSame(flattenedBlock, block);
        }
    }

    @Test
    public void testNestedDictionaryRLEByteArray()
    {
        DictionaryBlock block = createTestDictionaryBlock(createTestRleBlock(createByteArrayBlock(4), 3));
        assertFlattenNumericTypeBlock(TINYINT, block, 1, ByteArrayBlock.class);
    }

    @Test
    public void testNestedRLEs()
    {
        Block block = createTestRleBlock(createTestRleBlock(createTestRleBlock(createLongArrayBlock(5), 1), 1), 4);
        assertEquals(block.getPositionCount(), 4);
        try (BlockLease blockLease = flattener.flatten(block)) {
            Block flattenedBlock = blockLease.get();
            assertEquals(flattenedBlock.getClass(), RunLengthEncodedBlock.class);
            assertEquals(flattenedBlock.getPositionCount(), block.getPositionCount());
            assertEquals(flattenedBlock.getClass(), RunLengthEncodedBlock.class);
            assertEquals(((RunLengthEncodedBlock) flattenedBlock).getValue().getClass(), LongArrayBlock.class);

            Block innerBlock = ((RunLengthEncodedBlock) flattenedBlock).getValue();
            assertEquals(innerBlock.getPositionCount(), 1);
        }
    }

    @Test
    public void testCardinalityIncreasingNestedDictionaryBlock()
    {
        Block block = new DictionaryBlock(
                new DictionaryBlock(
                        new DictionaryBlock(
                                createLongArrayBlock(5, 6),
                                new int[] {0, 1}),
                        new int[] {0, 1, 0, 0, 1}),
                new int[] {0, 1, 0, 0, 1, 1, 0});
        assertFlatten(
                block,
                flattenedBlock -> {
                    assertEquals(flattenedBlock.getPositionCount(), block.getPositionCount());
                    assertEquals(((DictionaryBlock) flattenedBlock).getDictionary().getClass(), LongArrayBlock.class);

                    for (int i = 0; i < block.getPositionCount(); i++) {
                        assertEquals(flattenedBlock.getLong(i), block.getLong(i));
                    }
                });
    }

    @Test
    public void testCardinalityDecreasingNestedDictionaryBlock()
    {
        Block block = new DictionaryBlock(
                new DictionaryBlock(
                        new DictionaryBlock(
                                createLongArrayBlock(5, 6),
                                new int[] {0, 1, 0, 0, 1, 1, 0}),
                        new int[] {0, 1, 0}),
                new int[] {0, 1});
        assertFlatten(
                block,
                flattenedBlock -> {
                    assertEquals(flattenedBlock.getPositionCount(), block.getPositionCount());
                    assertEquals(((DictionaryBlock) flattenedBlock).getDictionary().getClass(), LongArrayBlock.class);

                    for (int i = 0; i < block.getPositionCount(); i++) {
                        assertEquals(flattenedBlock.getLong(i), block.getLong(i));
                    }
                });
    }

    @Test
    public void testNestedDictionaryWithRLEWithLeftoverData()
    {
        Random random = ThreadLocalRandom.current();
        Deque<int[]> leasedArrays = new ArrayDeque<>();
        for (int i = 0; i < 10; i++) {
            int[] randomInts = IntStream.range(0, 100).map(j -> random.nextInt()).toArray();
            int[] array = allocator.borrowIntArray(100);
            System.arraycopy(randomInts, 0, array, 0, randomInts.length);
            leasedArrays.push(array);
        }
        while (!leasedArrays.isEmpty()) {
            allocator.returnArray(leasedArrays.pop());
        }
        DictionaryBlock block = createTestDictionaryBlock(createTestRleBlock(createIntArrayBlock(4), 3));
        assertFlatten(
                block,
                flattenedBlock -> {
                    assertEquals(flattenedBlock.getClass(), DictionaryBlock.class);
                    assertEquals(((DictionaryBlock) flattenedBlock).getDictionary().getClass(), IntArrayBlock.class);
                    assertEquals(flattenedBlock.getPositionCount(), block.getPositionCount());
                    for (int i = 0; i < block.getPositionCount(); i++) {
                        assertEquals(INTEGER.getLong(flattenedBlock, i), INTEGER.getLong(block, i));
                    }
                });
    }

    private void assertFlattenNumericTypeBlock(Type type, Block block, int expectedPositionCount, Class expectedClass)
    {
        assertFlatten(
                block,
                flattenedBlock -> {
                    assertEquals(allocator.getBorrowedArrayCount(), 1);

                    assertEquals(flattenedBlock.getPositionCount(), block.getPositionCount());
                    for (int i = 0; i < block.getPositionCount(); i++) {
                        assertEquals(type.getLong(block, i), type.getLong(flattenedBlock, i));
                    }
                    assertEquals(flattenedBlock.getClass(), DictionaryBlock.class);
                    DictionaryBlock decodedDictionary = (DictionaryBlock) flattenedBlock;
                    assertEquals(decodedDictionary.getDictionary().getClass(), expectedClass);
                    assertEquals(expectedPositionCount, decodedDictionary.getDictionary().getPositionCount());
                });
    }

    private void assertFlatten(Block block, Consumer<Block> blockConsumer)
    {
        assertEquals(allocator.getBorrowedArrayCount(), 0);
        try (BlockLease blockLease = flattener.flatten(block)) {
            assertTrue(allocator.getBorrowedArrayCount() > 0);
            Block retrievedBlock = blockLease.get();
            assertNotNull(retrievedBlock);
            blockConsumer.accept(retrievedBlock);
            assertThrows(IllegalStateException.class, blockLease::get);
        }
        assertEquals(allocator.getBorrowedArrayCount(), 0);
    }

    private static Block createLongArrayBlock(long... values)
    {
        return new LongArrayBlock(values.length, Optional.empty(), values);
    }

    private static Block createIntArrayBlock(int... values)
    {
        return new IntArrayBlock(values.length, Optional.empty(), values);
    }

    private static Block createShortArrayBlock(int... values)
    {
        short[] shorts = new short[values.length];
        for (int i = 0; i < values.length; i++) {
            shorts[i] = (short) values[i];
        }
        return new ShortArrayBlock(shorts.length, Optional.empty(), shorts);
    }

    private static Block createByteArrayBlock(int... values)
    {
        byte[] bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return new ByteArrayBlock(bytes.length, Optional.empty(), bytes);
    }

    private static DictionaryBlock createTestDictionaryBlock(Block block)
    {
        int idsOffset = ThreadLocalRandom.current().nextInt(block.getPositionCount()) + 1;
        int[] dictionaryIndexes = createTestDictionaryIndexes(block.getPositionCount() + idsOffset);
        return new DictionaryBlock(idsOffset, block.getPositionCount(), block, dictionaryIndexes, false, randomDictionaryId());
    }

    private static int[] createTestDictionaryIndexes(int valueCount)
    {
        int[] dictionaryIndexes = new int[valueCount * 2];
        for (int i = 0; i < valueCount; i++) {
            dictionaryIndexes[i] = valueCount - i - 1;
            dictionaryIndexes[i + valueCount] = i;
        }
        return dictionaryIndexes;
    }

    private static RunLengthEncodedBlock createTestRleBlock(Block block, int position)
    {
        return new RunLengthEncodedBlock(block, position);
    }
}
