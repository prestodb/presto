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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestTypedKeyValueHeap
{
    private static final int INPUT_SIZE = 1_000_000; // larger than COMPACT_THRESHOLD_* to guarantee coverage of compact
    private static final int OUTPUT_SIZE = 1_000;

    private static final BlockComparator MAX_ELEMENTS_COMPARATOR = BIGINT::compareTo;
    private static final BlockComparator MIN_ELEMENTS_COMPARATOR = (leftBlock, leftIndex, rightBlock, rightIndex) -> -BIGINT.compareTo(leftBlock, leftIndex, rightBlock, rightIndex);

    @Test
    public void testAscending()
    {
        test(IntStream.range(0, INPUT_SIZE),
                IntStream.range(0, INPUT_SIZE).mapToObj(key -> Integer.toString(key * 2)),
                MAX_ELEMENTS_COMPARATOR,
                IntStream.range(INPUT_SIZE - OUTPUT_SIZE, INPUT_SIZE).mapToObj(key -> Integer.toString(key * 2)).iterator());
        test(IntStream.range(0, INPUT_SIZE),
                IntStream.range(0, INPUT_SIZE).mapToObj(key -> Integer.toString(key * 2)),
                MIN_ELEMENTS_COMPARATOR,
                IntStream.range(0, OUTPUT_SIZE).map(x -> OUTPUT_SIZE - 1 - x).mapToObj(key -> Integer.toString(key * 2)).iterator());
    }

    @Test
    public void testDescending()
    {
        test(IntStream.range(0, INPUT_SIZE).map(x -> INPUT_SIZE - 1 - x),
                IntStream.range(0, INPUT_SIZE).map(x -> INPUT_SIZE - 1 - x).mapToObj(key -> Integer.toString(key * 2)),
                MAX_ELEMENTS_COMPARATOR,
                IntStream.range(INPUT_SIZE - OUTPUT_SIZE, INPUT_SIZE).mapToObj(key -> Integer.toString(key * 2)).iterator());
        test(IntStream.range(0, INPUT_SIZE).map(x -> INPUT_SIZE - 1 - x),
                IntStream.range(0, INPUT_SIZE).map(x -> INPUT_SIZE - 1 - x).mapToObj(key -> Integer.toString(key * 2)),
                MIN_ELEMENTS_COMPARATOR,
                IntStream.range(0, OUTPUT_SIZE).map(x -> OUTPUT_SIZE - 1 - x).mapToObj(key -> Integer.toString(key * 2)).iterator());
    }

    @Test
    public void testShuffled()
    {
        List<Integer> list = IntStream.range(0, INPUT_SIZE).collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        Collections.shuffle(list);
        test(list.stream().mapToInt(Integer::intValue),
                list.stream().mapToInt(Integer::intValue).mapToObj(key -> Integer.toString(key * 2)),
                MAX_ELEMENTS_COMPARATOR,
                IntStream.range(INPUT_SIZE - OUTPUT_SIZE, INPUT_SIZE).mapToObj(key -> Integer.toString(key * 2)).iterator());
        test(list.stream().mapToInt(Integer::intValue),
                list.stream().mapToInt(Integer::intValue).mapToObj(key -> Integer.toString(key * 2)),
                MIN_ELEMENTS_COMPARATOR,
                IntStream.range(0, OUTPUT_SIZE).map(x -> OUTPUT_SIZE - 1 - x).mapToObj(key -> Integer.toString(key * 2)).iterator());
    }

    private static void test(IntStream keyInputStream, Stream<String> valueInputStream, BlockComparator comparator, Iterator<String> outputIterator)
    {
        BlockBuilder keysBlockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), INPUT_SIZE);
        BlockBuilder valuesBlockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), INPUT_SIZE);
        keyInputStream.forEach(x -> BIGINT.writeLong(keysBlockBuilder, x));
        valueInputStream.forEach(x -> VARCHAR.writeString(valuesBlockBuilder, x));

        TypedKeyValueHeap heap = new TypedKeyValueHeap(comparator, BIGINT, VARCHAR, OUTPUT_SIZE);
        heap.addAll(keysBlockBuilder, valuesBlockBuilder);

        BlockBuilder resultBlockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), OUTPUT_SIZE);
        heap.popAll(resultBlockBuilder);

        Block resultBlock = resultBlockBuilder.build();
        assertEquals(resultBlock.getPositionCount(), OUTPUT_SIZE);
        for (int i = 0; i < OUTPUT_SIZE; i++) {
            assertEquals(VARCHAR.getSlice(resultBlock, i).toStringUtf8(), outputIterator.next());
        }
    }
}
