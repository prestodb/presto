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
package com.facebook.presto.common.array;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.IntArrayBlockBuilder;
import org.openjdk.jol.info.ClassLayout;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestBlockBigArray
{
    @Test
    public void testRetainedSizeWithOverlappingBlocks()
    {
        int entries = 123;
        BlockBuilder blockBuilder = new IntArrayBlockBuilder(null, entries);
        for (int i = 0; i < entries; i++) {
            blockBuilder.writeInt(i);
        }
        Block block = blockBuilder.build();

        // Verify we do not over count
        int arraySize = 456;
        int blocks = 7890;
        BlockBigArray blockBigArray = new BlockBigArray();
        blockBigArray.ensureCapacity(arraySize);
        for (int i = 0; i < blocks; i++) {
            blockBigArray.set(i % arraySize, block.getRegion(0, entries));
        }

        ReferenceCountMap referenceCountMap = new ReferenceCountMap();
        referenceCountMap.incrementAndGet(block);
        long expectedSize = ClassLayout.parseClass(BlockBigArray.class).instanceSize()
                + referenceCountMap.sizeOf()
                + (new ObjectBigArray()).sizeOf()
                + block.getRetainedSizeInBytes() + (arraySize - 1) * ClassLayout.parseClass(block.getClass()).instanceSize();
        assertEquals(blockBigArray.sizeOf(), expectedSize);
    }
}
