
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
package com.facebook.presto.orc.writer;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.metadata.OrcType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.writer.TestMapFlatUtils.createFlatMapValueWriterFactory;
import static com.facebook.presto.orc.writer.TestMapFlatUtils.createIntegerBlock;
import static com.facebook.presto.orc.writer.TestMapFlatUtils.createNumericDataBlock;
import static com.facebook.presto.orc.writer.TestMapFlatUtils.createStringBlock;
import static com.facebook.presto.orc.writer.TestMapFlatUtils.createStringDataBlock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestMapFlatKeyToValueMap
{
    @Test
    public void testMapCreation()
    {
        FlatMapKeyToValueMap numericMap = new FlatMapKeyToNumericValueMap(OrcType.OrcTypeKind.INT);
        FlatMapKeyToValueMap stringMap = new FlatMapKeyToStringValueMap(OrcType.OrcTypeKind.STRING);
        expectThrows(IllegalArgumentException.class, () -> new FlatMapKeyToNumericValueMap(OrcType.OrcTypeKind.STRING));
        expectThrows(IllegalArgumentException.class, () -> new FlatMapKeyToStringValueMap(OrcType.OrcTypeKind.LONG));
        assertTrue(numericMap.isNumericKey());
        assertTrue(!stringMap.isNumericKey());
    }

    @Test
    public void testNumericInserts()
    {
        FlatMapValueWriterFactory factory = createFlatMapValueWriterFactory(INTEGER);
        Block[] blocks = createIntegerBlock();
        FlatMapKeyToValueMap numericMap = new FlatMapKeyToNumericValueMap(OrcType.OrcTypeKind.INT);
        int dwrfSequenceId = 1;

        for (Block block : blocks) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                numericMap.putIfAbsent(block, position, INTEGER, factory.getFlatMapValueColumnWriter(position + 1));
            }
        }

        for (Block block : blocks) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                assertTrue(numericMap.containsKey(block, position, INTEGER));
                assertEquals(numericMap.get(block, position, INTEGER).getDwrfSequence(), position + 1);
            }
        }

        ImmutableList.Builder<Long> values = new ImmutableList.Builder<>();
        for (long row = 0; row < 10; row++) {
            values.add(ThreadLocalRandom.current().nextLong(11, 10001));
        }
        Block[] invalidKeyBlocks = createNumericDataBlock(INTEGER, values.build());
        for (Block block : invalidKeyBlocks) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                assertTrue(!numericMap.containsKey(block, position, INTEGER));
            }
        }
    }

    @Test
    public void testStringInserts()
    {
        Type valueType = VARCHAR;
        FlatMapValueWriterFactory factory = createFlatMapValueWriterFactory(valueType);
        Block[] blocks = createStringBlock();
        FlatMapKeyToValueMap stringMap = new FlatMapKeyToStringValueMap(OrcType.OrcTypeKind.VARCHAR);
        int dwrfSequenceId = 1;

        for (Block block : blocks) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                stringMap.putIfAbsent(block, position, valueType, factory.getFlatMapValueColumnWriter(position + 1));
            }
        }

        for (Block block : blocks) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                assertTrue(stringMap.containsKey(block, position, valueType));
                assertEquals(stringMap.get(block, position, valueType).getDwrfSequence(), position + 1);
            }
        }

        ImmutableList.Builder<String> values = new ImmutableList.Builder<>();
        for (long row = 0; row < 10; row++) {
            values.add(String.valueOf(ThreadLocalRandom.current().nextLong(11, 10001)));
        }
        Block[] invalidKeyBlocks = createStringDataBlock(valueType, values.build());
        for (Block block : invalidKeyBlocks) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                assertTrue(!stringMap.containsKey(block, position, valueType));
            }
        }
    }

    @Test
    public void testNumericIteration()
    {
        FlatMapValueWriterFactory factory = createFlatMapValueWriterFactory(INTEGER);
        Block[] blocks = createIntegerBlock();
        FlatMapKeyToValueMap numericMap = new FlatMapKeyToNumericValueMap(OrcType.OrcTypeKind.INT);

        for (Block block : blocks) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                numericMap.putIfAbsent(block, position, INTEGER, factory.getFlatMapValueColumnWriter(position + 1));
            }
        }

        ImmutableSet.Builder<Long> presentKeys = new ImmutableSet.Builder<>();
        for (Long2ObjectMap.Entry<FlatMapValueColumnWriter> entry : numericMap.getLongKeyEntrySet()) {
            presentKeys.add(entry.getLongKey());
        }
        ImmutableSet<Long> presentKeysSet = presentKeys.build();

        AtomicInteger entryCount = new AtomicInteger();
        for (Block block : blocks) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                entryCount.getAndIncrement();
                assertTrue(presentKeysSet.contains(INTEGER.getLong(block, position)));
            }
        }
        assertEquals(entryCount.get(), presentKeysSet.size());

        BiConsumer<Long, FlatMapValueColumnWriter> biConsumer = (key, value) -> {
            assertTrue(presentKeysSet.contains(key));
            entryCount.getAndDecrement();
        };
        numericMap.forEach(biConsumer);
        assertEquals(entryCount.get(), 0);
    }

    @Test
    public void testStringIteration()
    {
        Type valueType = VARCHAR;
        FlatMapValueWriterFactory factory = createFlatMapValueWriterFactory(valueType);
        Block[] blocks = createStringBlock();
        FlatMapKeyToValueMap stringMap = new FlatMapKeyToStringValueMap(OrcType.OrcTypeKind.VARCHAR);

        for (Block block : blocks) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                stringMap.putIfAbsent(block, position, valueType, factory.getFlatMapValueColumnWriter(position + 1));
            }
        }

        ImmutableSet.Builder<String> presentKeys = new ImmutableSet.Builder<>();
        for (Map.Entry<String, FlatMapValueColumnWriter> entry : stringMap.getStringKeyEntrySet()) {
            presentKeys.add(entry.getKey());
        }
        ImmutableSet<String> presentKeysSet = presentKeys.build();

        AtomicInteger entryCount = new AtomicInteger();
        for (Block block : blocks) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                entryCount.getAndIncrement();
                assertTrue(presentKeysSet.contains(valueType.getSlice(block, position).toStringUtf8()));
            }
        }
        assertEquals(entryCount.get(), presentKeysSet.size());

        BiConsumer<String, FlatMapValueColumnWriter> biConsumer = (key, value) -> {
            assertTrue(presentKeysSet.contains(key));
            entryCount.getAndDecrement();
        };
        stringMap.forEach(biConsumer);
        assertEquals(entryCount.get(), 0);
    }
}
