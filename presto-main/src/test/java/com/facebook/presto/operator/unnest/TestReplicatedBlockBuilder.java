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
package com.facebook.presto.operator.unnest;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.DictionaryBlock;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.Collections;

import static com.facebook.presto.block.ColumnarTestUtils.assertBlock;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.createReplicatedOutputSlice;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.createSimpleBlock;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.toSlices;
import static com.facebook.presto.operator.unnest.UnnestOperatorBlockUtil.calculateNewArraySize;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestReplicatedBlockBuilder
{
    @Test
    public void testReplicateOutput()
    {
        String[] values = {"a", "b", "c", null, null, "e"};
        int[] count = {1, 0, 2, 2, 0, 3};
        testReplication(toSlices(values), count);
    }

    @Test
    public void testReplicateEmptyOutput()
    {
        String[] values = {"a", null, "b"};
        int[] count = {0, 0, 0};
        testReplication(toSlices(values), count);
    }

    private static void testReplication(Slice[] values, int[] counts)
    {
        assertEquals(values.length, counts.length);

        ReplicatedBlockBuilder replicateBlockBuilder = new ReplicatedBlockBuilder();
        Block valuesBlock = createSimpleBlock(values);
        replicateBlockBuilder.resetInputBlock(valuesBlock);
        replicateBlockBuilder.startNewOutput(100);

        for (int i = 0; i < counts.length; i++) {
            replicateBlockBuilder.appendRepeated(i, counts[i]);
        }

        Block outputBlock = replicateBlockBuilder.buildOutputAndFlush();
        assertBlock(outputBlock, createReplicatedOutputSlice(values, counts));
        assertTrue(outputBlock instanceof DictionaryBlock);
    }

    @Test
    public void testCapacityIncrease()
    {
        assertSmallCapacityIncrease(4, 1, 4);
        assertBigCapacityIncrease(50, 49, 100);
    }

    /**
     * verify capacity increase when required new capacity is <= the value returned from {@link UnnestOperatorBlockUtil#calculateNewArraySize}
     */
    private static void assertSmallCapacityIncrease(int initialSize, int firstAppendCount, int secondAppendCount)
    {
        assertTrue(firstAppendCount <= initialSize);
        assertTrue(firstAppendCount + secondAppendCount > initialSize);
        assertTrue(firstAppendCount + secondAppendCount <= calculateNewArraySize(initialSize));
        assertCapacityIncrease(initialSize, firstAppendCount, secondAppendCount, new ReplicatedBlockBuilder());
    }

    /**
     * verify capacity increase when required new capacity is > the value returned from {@link UnnestOperatorBlockUtil#calculateNewArraySize}
     */
    private static void assertBigCapacityIncrease(int initialSize, int firstAppendCount, int secondAppendCount)
    {
        assertTrue(firstAppendCount <= initialSize);
        assertTrue(firstAppendCount + secondAppendCount > initialSize);
        assertTrue(firstAppendCount + secondAppendCount > calculateNewArraySize(initialSize));
        assertCapacityIncrease(initialSize, firstAppendCount, secondAppendCount, new ReplicatedBlockBuilder());
    }

    private static void assertCapacityIncrease(int initialSize, int firstAppendCount, int secondAppendCount, ReplicatedBlockBuilder replicatedBlockBuilder)
    {
        Slice[] values = {null, utf8Slice("a")};
        Block inputBlock = createSimpleBlock(values);
        int repeatingIndex = 1;

        replicatedBlockBuilder.resetInputBlock(inputBlock);
        replicatedBlockBuilder.startNewOutput(initialSize);

        replicatedBlockBuilder.appendRepeated(repeatingIndex, firstAppendCount);
        replicatedBlockBuilder.appendRepeated(repeatingIndex, secondAppendCount);

        Block output = replicatedBlockBuilder.buildOutputAndFlush();
        int totalCount = firstAppendCount + secondAppendCount;
        assertBlock(output, Collections.nCopies(totalCount, values[repeatingIndex]).toArray(new Slice[totalCount]));
    }
}
