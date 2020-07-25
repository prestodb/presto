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
import io.airlift.slice.DynamicSliceOutput;
import it.unimi.dsi.fastutil.Hash.Strategy;
import it.unimi.dsi.fastutil.objects.Object2LongOpenCustomHashMap;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestBlockRetainedSizeBreakdown
{
    private static final int EXPECTED_ENTRIES = 100;

    @Test
    public void testArrayBlock()
    {
        BlockBuilder arrayBlockBuilder = new ArrayBlockBuilder(BIGINT, null, EXPECTED_ENTRIES);
        for (int i = 0; i < EXPECTED_ENTRIES; i++) {
            BlockBuilder arrayElementBuilder = arrayBlockBuilder.beginBlockEntry();
            writeNativeValue(BIGINT, arrayElementBuilder, castIntegerToObject(i, BIGINT));
            arrayBlockBuilder.closeEntry();
        }
        checkRetainedSize(arrayBlockBuilder.build(), false);
    }

    @Test
    public void testByteArrayBlock()
    {
        BlockBuilder blockBuilder = new ByteArrayBlockBuilder(null, EXPECTED_ENTRIES);
        for (int i = 0; i < EXPECTED_ENTRIES; i++) {
            blockBuilder.writeByte(i);
        }
        checkRetainedSize(blockBuilder.build(), false);
    }

    @Test
    public void testDictionaryBlock()
    {
        Block keyDictionaryBlock = createVariableWidthBlock(EXPECTED_ENTRIES);
        int[] keyIds = new int[EXPECTED_ENTRIES];
        for (int i = 0; i < keyIds.length; i++) {
            keyIds[i] = i;
        }
        checkRetainedSize(new DictionaryBlock(EXPECTED_ENTRIES, keyDictionaryBlock, keyIds), false);
    }

    @Test
    public void testIntArrayBlock()
    {
        BlockBuilder blockBuilder = new IntArrayBlockBuilder(null, EXPECTED_ENTRIES);
        writeEntries(EXPECTED_ENTRIES, blockBuilder, INTEGER);
        checkRetainedSize(blockBuilder.build(), false);
    }

    @Test
    public void testLongArrayBlock()
    {
        BlockBuilder blockBuilder = new LongArrayBlockBuilder(null, EXPECTED_ENTRIES);
        writeEntries(EXPECTED_ENTRIES, blockBuilder, BIGINT);
        checkRetainedSize(blockBuilder.build(), false);
    }

    @Test
    public void testRunLengthEncodedBlock()
    {
        BlockBuilder blockBuilder = new LongArrayBlockBuilder(null, 1);
        writeEntries(1, blockBuilder, BIGINT);
        checkRetainedSize(new RunLengthEncodedBlock(blockBuilder.build(), 1), false);
    }

    @Test
    public void testShortArrayBlock()
    {
        BlockBuilder blockBuilder = new ShortArrayBlockBuilder(null, EXPECTED_ENTRIES);
        for (int i = 0; i < EXPECTED_ENTRIES; i++) {
            blockBuilder.writeShort(i);
        }
        checkRetainedSize(blockBuilder.build(), false);
    }

    @Test
    public void testVariableWidthBlock()
    {
        checkRetainedSize(createVariableWidthBlock(EXPECTED_ENTRIES), false);
    }

    @Test
    public void testInt128ArrayBlock()
    {
        long[] longs = new long[EXPECTED_ENTRIES * 2];
        for (int i = 0; i < longs.length; i++) {
            longs[i] = i;
        }
        Block block = new Int128ArrayBlock(EXPECTED_ENTRIES, Optional.empty(), longs);
        checkRetainedSize(block, false);
    }

    private static final class ObjectStrategy
            implements Strategy<Object>
    {
        @Override
        public int hashCode(Object object)
        {
            return System.identityHashCode(object);
        }

        @Override
        public boolean equals(Object left, Object right)
        {
            return left == right;
        }
    }

    private static void checkRetainedSize(Block block, boolean getRegionCreateNewObjects)
    {
        AtomicLong objectSize = new AtomicLong();
        Object2LongOpenCustomHashMap<Object> trackedObjects = new Object2LongOpenCustomHashMap<>(new ObjectStrategy());

        BiConsumer<Object, Long> consumer = (object, size) -> {
            objectSize.addAndGet(size);
            trackedObjects.addTo(object, 1);
        };

        block.retainedBytesForEachPart(consumer);
        assertEquals(objectSize.get(), block.getRetainedSizeInBytes());

        Block copyBlock = block.getRegion(0, block.getPositionCount() / 2);
        copyBlock.retainedBytesForEachPart(consumer);
        assertEquals(objectSize.get(), block.getRetainedSizeInBytes() + copyBlock.getRetainedSizeInBytes());

        assertEquals(trackedObjects.getLong(block), 1);
        assertEquals(trackedObjects.getLong(copyBlock), 1);
        trackedObjects.remove(block);
        trackedObjects.remove(copyBlock);
        for (long value : trackedObjects.values()) {
            assertEquals(value, getRegionCreateNewObjects ? 1 : 2);
        }
    }

    private static void writeEntries(int expectedEntries, BlockBuilder blockBuilder, Type type)
    {
        for (int i = 0; i < expectedEntries; i++) {
            writeNativeValue(type, blockBuilder, castIntegerToObject(i, type));
        }
    }

    private static Object castIntegerToObject(int value, Type type)
    {
        if (type == INTEGER || type == TINYINT || type == BIGINT) {
            return (long) value;
        }
        if (type == VARCHAR) {
            return String.valueOf(value);
        }
        if (type == DOUBLE) {
            return (double) value;
        }
        throw new UnsupportedOperationException();
    }

    private static Block createVariableWidthBlock(int entries)
    {
        int[] offsets = new int[entries + 1];
        DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(entries);
        for (int i = 0; i < entries; i++) {
            dynamicSliceOutput.writeByte(i);
            offsets[i + 1] = dynamicSliceOutput.size();
        }
        return new VariableWidthBlock(entries, dynamicSliceOutput.slice(), offsets, Optional.empty());
    }
}
