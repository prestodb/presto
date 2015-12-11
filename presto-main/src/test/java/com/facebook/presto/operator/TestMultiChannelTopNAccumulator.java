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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestMultiChannelTopNAccumulator
{
    private static final int INPUT_SIZE = 1_000_000; // larger than COMPACT_THRESHOLD_* to guarantee coverage of compact
    private static final int OUTPUT_SIZE = 1_001; // use an odd number to expose more potential issues

    @Test
    public void testAscending()
    {
        // max OUTPUT_SIZE
        test(Arrays.asList(LongStream.range(0, INPUT_SIZE).mapToObj(x -> x)),
                Arrays.asList(BIGINT),
                Arrays.asList(0),
                Arrays.asList(BIGINT),
                Arrays.asList(SortOrder.DESC_NULLS_FIRST),
                Arrays.asList(LongStream.range(INPUT_SIZE - OUTPUT_SIZE, INPUT_SIZE)
                        .map(x -> INPUT_SIZE - 1 - (x - (INPUT_SIZE - OUTPUT_SIZE))).iterator()));
        // min OUTPUT_SIZE
        test(Arrays.asList(LongStream.range(0, INPUT_SIZE).mapToObj(x -> x)),
                Arrays.asList(BIGINT),
                Arrays.asList(0),
                Arrays.asList(BIGINT),
                Arrays.asList(SortOrder.ASC_NULLS_FIRST),
                Arrays.asList(LongStream.range(0, OUTPUT_SIZE).iterator()));
    }

    @Test
    public void testAscending2Columns()
    {
        // max OUTPUT_SIZE
        test(Arrays.asList(
                LongStream.range(0, INPUT_SIZE).mapToObj(x -> x),
                LongStream.range(0, INPUT_SIZE).mapToObj(x -> INPUT_SIZE - 1 - x)),
                Arrays.asList(BIGINT, BIGINT),
                Arrays.asList(0),
                Arrays.asList(BIGINT),
                Arrays.asList(SortOrder.DESC_NULLS_FIRST),
                Arrays.asList(LongStream.range(INPUT_SIZE - OUTPUT_SIZE, INPUT_SIZE)
                                .map(x -> INPUT_SIZE - 1 - (x - (INPUT_SIZE - OUTPUT_SIZE))).iterator(),
                        LongStream.range(0, OUTPUT_SIZE).iterator()));
        // min OUTPUT_SIZE
        test(Arrays.asList(
                LongStream.range(0, INPUT_SIZE).mapToObj(x -> x),
                LongStream.range(0, INPUT_SIZE).mapToObj(x -> INPUT_SIZE - 1 - x)),
                Arrays.asList(BIGINT, BIGINT),
                Arrays.asList(0),
                Arrays.asList(BIGINT),
                Arrays.asList(SortOrder.ASC_NULLS_FIRST),
                Arrays.asList(LongStream.range(0, OUTPUT_SIZE).iterator(),
                        LongStream.range(INPUT_SIZE - OUTPUT_SIZE, INPUT_SIZE)
                                .map(x -> INPUT_SIZE - 1 - (x - (INPUT_SIZE - OUTPUT_SIZE))).iterator())
        );
    }

    @Test
    public void testDescending()
    {
        // max N
        test(Arrays.asList(
                LongStream.range(0, INPUT_SIZE).mapToObj(x -> x)
                ),
                Arrays.asList(BIGINT),
                Arrays.asList(0),
                Arrays.asList(BIGINT),
                Arrays.asList(SortOrder.DESC_NULLS_FIRST),
                Arrays.asList(
                        LongStream.range(0, OUTPUT_SIZE)
                                .map(x -> INPUT_SIZE - 1 - x).iterator()
                )
        );
    }

    @Test
    public void testDescending2Columns()
    {
        // max N
        test(Arrays.asList(
                LongStream.range(0, INPUT_SIZE).mapToObj(x -> x),
                LongStream.range(0, INPUT_SIZE).mapToObj(x -> String.format("%d", x))
                ),
                Arrays.asList(BIGINT, VARCHAR),
                Arrays.asList(0),
                Arrays.asList(BIGINT),
                Arrays.asList(SortOrder.DESC_NULLS_FIRST),
                Arrays.asList(
                        LongStream.range(0, OUTPUT_SIZE)
                                .map(x -> INPUT_SIZE - 1 - x).iterator(),
                        LongStream.range(0, OUTPUT_SIZE)
                                .mapToObj(x -> String.format("%d", INPUT_SIZE - 1 - x)).iterator()
                )
        );
        // min N
        test(Arrays.asList(
                LongStream.range(0, INPUT_SIZE).mapToObj(x -> INPUT_SIZE - 1 - x),
                LongStream.range(0, INPUT_SIZE).mapToObj(x -> String.format("%d", INPUT_SIZE - 1 - x))
                ),
                Arrays.asList(BIGINT, VARCHAR),
                Arrays.asList(0),
                Arrays.asList(BIGINT),
                Arrays.asList(SortOrder.ASC_NULLS_FIRST),
                Arrays.asList(LongStream.range(0, OUTPUT_SIZE).iterator(),
                        LongStream.range(0, OUTPUT_SIZE).mapToObj(x -> String.format("%d", x)).iterator())
        );
        /// min N, but using both columns as sort keys, note that we use a different order for the sort keys than
        /// the original columns
        test(Arrays.asList(
                LongStream.range(0, INPUT_SIZE).mapToObj(x -> INPUT_SIZE - 1 - x),
                LongStream.range(0, INPUT_SIZE).mapToObj(x -> String.format("%010d", x))
                ),
                Arrays.asList(BIGINT, VARCHAR),
                Arrays.asList(1, 0),
                Arrays.asList(VARCHAR, BIGINT),
                Arrays.asList(SortOrder.ASC_NULLS_FIRST, SortOrder.ASC_NULLS_FIRST),
                Arrays.asList(LongStream.range(0, OUTPUT_SIZE).mapToObj(x -> INPUT_SIZE - 1 - x).iterator(),
                        LongStream.range(0, OUTPUT_SIZE).mapToObj(x -> String.format("%010d", x)).iterator())
        );
    }

    @Test
    public void testShuffled()
    {
        List<Long> list = LongStream.range(0, INPUT_SIZE).collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        Collections.shuffle(list);
        Iterator expected = LongStream.range(INPUT_SIZE - OUTPUT_SIZE, INPUT_SIZE)
                .map(x -> INPUT_SIZE - 1 - (x - (INPUT_SIZE - OUTPUT_SIZE))).iterator();
        Iterator expected2 = LongStream.range(INPUT_SIZE - OUTPUT_SIZE, INPUT_SIZE)
            .map(x -> INPUT_SIZE - 1 - (x - (INPUT_SIZE - OUTPUT_SIZE))).iterator();
        test(Arrays.asList(list.stream(), list.stream()),
                Arrays.asList(BIGINT, BIGINT),
                Arrays.asList(0),
                Arrays.asList(BIGINT),
                Arrays.asList(SortOrder.DESC_NULLS_FIRST),
                Arrays.asList(expected, expected2));
        test(Arrays.asList(list.stream()),
                Arrays.asList(BIGINT),
                Arrays.asList(0),
                Arrays.asList(BIGINT),
                Arrays.asList(SortOrder.ASC_NULLS_FIRST),
                Arrays.asList(LongStream.range(0, OUTPUT_SIZE).iterator()));
    }

    private void test(List<Stream<? extends Object>> inputStream,
            List<? extends Type> sourceTypes,
            List<Integer> sortChannels,
            List<? extends Type> sortTypes,
            List<SortOrder> sortOrders,
            List<Iterator> outputIterator)
    {
        BlockBuilder[] blockBuilders = new BlockBuilder[inputStream.size()];
        for (int j = 0; j < inputStream.size(); j++) {
            final Type sourceType = sourceTypes.get(j);
            blockBuilders[j] = sourceTypes.get(j).createBlockBuilder(new BlockBuilderStatus(), INPUT_SIZE);
            final BlockBuilder block = blockBuilders[j];
            for (Object x : inputStream.get(j).collect(Collectors.toList())) {
                // it's a bit clumsy to support arbitrary dumping into blocks now, since there is not a
                // universal writeObject(), so only support BIGINT and VARCHAR now, as is needed by the above test cases
                if (sourceType instanceof BigintType) {
                    ((BigintType) sourceType).writeLong(block, (Long) x);
                }
                else {
                    ((VarcharType) sourceType).writeString(block, (String) x);
                }
            }
        }
        MultiChannelTopNAccumulator heap = new MultiChannelTopNAccumulator(
                sourceTypes,
                OUTPUT_SIZE,
                sortChannels,
                sortTypes,
                sortOrders
        );

        for (int i = 0; i < blockBuilders[0].getPositionCount(); i++) {
            heap.add(blockBuilders, i);
        }

        Block[] resultBlocks = heap.flushContent().toBlocks();
        for (int j = 0; j < inputStream.size(); j++) {
            assertEquals(resultBlocks[j].getPositionCount(), OUTPUT_SIZE);
            for (int i = 0; i < OUTPUT_SIZE; i++) {
                Object actual = sourceTypes.get(j).getObjectValue(null, resultBlocks[j], i);
                Object expected = outputIterator.get(j).next();
                assertEquals(actual, expected);
            }
        }
    }
}
