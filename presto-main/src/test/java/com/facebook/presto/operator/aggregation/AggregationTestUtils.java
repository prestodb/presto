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

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.rle.RunLengthEncodedBlock;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slices;

import java.util.Arrays;
import java.util.Collections;

import static com.facebook.presto.tuple.Tuples.NULL_BOOLEAN_TUPLE;
import static org.testng.Assert.assertEquals;

public final class AggregationTestUtils
{
    private AggregationTestUtils()
    {
    }

    public static void assertAggregation(AggregationFunction function, Object expectedValue, int positions, Block... blocks)
    {
        if (positions == 0) {
            assertAggregation(function, expectedValue);
        }
        else {
            assertAggregation(function, expectedValue, new Page(positions, blocks));
        }
    }

    public static void assertAggregation(AggregationFunction function, Object expectedValue, Page... pages)
    {
        assertEquals(aggregation(function, pages), expectedValue);
        assertEquals(partialAggregation(function, pages), expectedValue);
        if (pages.length > 0) {
            assertEquals(groupedAggregation(function, pages), expectedValue);
            assertEquals(groupedPartialAggregation(function, pages), expectedValue);
        }
    }

    public static Object aggregation(AggregationFunction function, Page... pages)
    {
        // execute with args in positions: arg0, arg1, arg2
        Object aggregation = aggregation(function, createArgs(function), pages);

        // execute with args in reverse order: arg2, arg1, arg0
        if (function.getParameterTypes().size() > 1) {
            Object aggregationWithOffset = aggregation(function, reverseArgs(function), reverseColumns(pages));
            assertEquals(aggregationWithOffset, aggregation, "Inconsistent results with reversed channels");
        }

        // execute with args at an offset (and possibly reversed): null, null, null, arg2, arg1, arg0
        Object aggregationWithOffset = aggregation(function, offsetArgs(function, 3), offsetColumns(pages, 3));
        assertEquals(aggregationWithOffset, aggregation, "Inconsistent results with channel offset");

        return aggregation;
    }

    private static Object aggregation(AggregationFunction function, int[] args, Page... pages)
    {
        Accumulator aggregation = function.createAggregation(args);
        for (Page page : pages) {
            if (page.getPositionCount() > 0) {
                aggregation.addInput(page);
            }
        }

        Block block = aggregation.evaluateFinal();
        return BlockAssertions.toValues(block).get(0).get(0);
    }

    public static Object partialAggregation(AggregationFunction function, Page... pages)
    {
        // execute with args in positions: arg0, arg1, arg2
        Object aggregation = partialAggregation(function, createArgs(function), pages);

        // execute with args in reverse order: arg2, arg1, arg0
        if (function.getParameterTypes().size() > 1) {
            Object aggregationWithOffset = partialAggregation(function, reverseArgs(function), reverseColumns(pages));
            assertEquals(aggregationWithOffset, aggregation, "Inconsistent results with reversed channels");
        }

        // execute with args at an offset (and possibly reversed): null, null, null, arg2, arg1, arg0
        Object aggregationWithOffset = partialAggregation(function, offsetArgs(function, 3), offsetColumns(pages, 3));
        assertEquals(aggregationWithOffset, aggregation, "Inconsistent results with channel offset");

        return aggregation;
    }

    public static Object partialAggregation(AggregationFunction function, int[] args, Page... pages)
    {
        Accumulator partialAggregation = function.createAggregation(args);
        for (Page page : pages) {
            if (page.getPositionCount() > 0) {
                partialAggregation.addInput(page);
            }
        }

        Block partialBlock = partialAggregation.evaluateIntermediate();

        Accumulator finalAggregation = function.createIntermediateAggregation();
        finalAggregation.addIntermediate(partialBlock);

        Block finalBlock = finalAggregation.evaluateFinal();
        return BlockAssertions.toValues(finalBlock).get(0).get(0);
    }

    public static Object groupedAggregation(AggregationFunction function, Page... pages)
    {
        // execute with args in positions: arg0, arg1, arg2
        Object aggregation = groupedAggregation(function, createArgs(function), pages);

        // execute with args in reverse order: arg2, arg1, arg0
        if (function.getParameterTypes().size() > 1) {
            Object aggregationWithOffset = groupedAggregation(function, reverseArgs(function), reverseColumns(pages));
            assertEquals(aggregationWithOffset, aggregation, "Inconsistent results with reversed channels");
        }

        // execute with args at an offset (and possibly reversed): null, null, null, arg2, arg1, arg0
        Object aggregationWithOffset = groupedAggregation(function, offsetArgs(function, 3), offsetColumns(pages, 3));
        assertEquals(aggregationWithOffset, aggregation, "Inconsistent results with channel offset");

        return aggregation;
    }

    public static Object groupedAggregation(AggregationFunction function, int[] args, Page... pages)
    {
        GroupedAccumulator groupedAggregation = function.createGroupedAggregation(args);
        for (Page page : pages) {
            groupedAggregation.addInput(createGroupByIdBlock(0, page.getPositionCount()), page);
        }
        Object groupValue = getGroupValue(groupedAggregation, 0);

        for (Page page : pages) {
            groupedAggregation.addInput(createGroupByIdBlock(4000, page.getPositionCount()), page);
        }
        Object largeGroupValue = getGroupValue(groupedAggregation, 4000);
        assertEquals(largeGroupValue, groupValue, "Inconsistent results with large group id");

        return groupValue;
    }

    public static Object groupedPartialAggregation(AggregationFunction function, Page... pages)
    {
        // execute with args in positions: arg0, arg1, arg2
        Object aggregation = groupedPartialAggregation(function, createArgs(function), pages);

        // execute with args in reverse order: arg2, arg1, arg0
        if (function.getParameterTypes().size() > 1) {
            Object aggregationWithOffset = groupedPartialAggregation(function, reverseArgs(function), reverseColumns(pages));
            assertEquals(aggregationWithOffset, aggregation, "Inconsistent results with reversed channels");
        }

        // execute with args at an offset (and possibly reversed): null, null, null, arg2, arg1, arg0
        Object aggregationWithOffset = groupedPartialAggregation(function, offsetArgs(function, 3), offsetColumns(pages, 3));
        assertEquals(aggregationWithOffset, aggregation, "Inconsistent results with channel offset");

        return aggregation;
    }

    public static Object groupedPartialAggregation(AggregationFunction function, int[] args, Page... pages)
    {
        GroupedAccumulator partialAggregation = function.createGroupedAggregation(args);
        for (Page page : pages) {
            partialAggregation.addInput(createGroupByIdBlock(0, page.getPositionCount()), page);
        }

        BlockBuilder partialOut = new BlockBuilder(partialAggregation.getIntermediateTupleInfo());
        partialAggregation.evaluateIntermediate(0, partialOut);
        UncompressedBlock partialBlock = partialOut.build();

        GroupedAccumulator finalAggregation = function.createGroupedIntermediateAggregation();
        finalAggregation.addIntermediate(createGroupByIdBlock(0, partialBlock.getPositionCount()), partialBlock);

        return getGroupValue(finalAggregation, 0);
    }

    public static GroupByIdBlock createGroupByIdBlock(int groupId, int positions)
    {
        if (positions == 0) {
            return new GroupByIdBlock(groupId, new UncompressedBlock(0, TupleInfo.SINGLE_LONG, Slices.EMPTY_SLICE));
        }

        BlockBuilder blockBuilder = new BlockBuilder(TupleInfo.SINGLE_LONG);
        for (int i = 0; i < positions; i++) {
            blockBuilder.append(groupId);
        }
        return new GroupByIdBlock(groupId, blockBuilder.build());
    }

    private static int[] createArgs(AggregationFunction function)
    {
        int[] args = new int[function.getParameterTypes().size()];
        for (int i = 0; i < args.length; i++) {
            args[i] = i;
        }
        return args;
    }

    private static int[] reverseArgs(AggregationFunction function)
    {
        int[] args = createArgs(function);
        Collections.reverse(Ints.asList(args));
        return args;
    }

    private static int[] offsetArgs(AggregationFunction function, int offset)
    {
        int[] args = createArgs(function);
        for (int i = 0; i < args.length; i++) {
            args[i] += offset;
        }
        return args;
    }

    private static Page[] reverseColumns(Page[] pages)
    {
        Page[] newPages = new Page[pages.length];
        for (int i = 0; i < pages.length; i++) {
            Page page = pages[i];
            if (page.getPositionCount() == 0) {
                newPages[i] = page;
            }
            else {
                Block[] newBlocks = Arrays.copyOf(page.getBlocks(), page.getChannelCount());
                Collections.reverse(Arrays.asList(newBlocks));
                newPages[i] = new Page(page.getPositionCount(), newBlocks);
            }
        }
        return newPages;
    }

    private static Page[] offsetColumns(Page[] pages, int offset)
    {
        Page[] newPages = new Page[pages.length];
        for (int i = 0; i < pages.length; i++) {
            Page page = pages[i];
            if (page.getPositionCount() == 0) {
                newPages[i] = page;
            }
            else {
                Block[] newBlocks = new Block[page.getChannelCount() + offset];
                for (int channel = 0; channel < offset; channel++) {
                    newBlocks[channel] = new RunLengthEncodedBlock(NULL_BOOLEAN_TUPLE, page.getPositionCount());
                }
                for (int channel = 0; channel < page.getBlocks().length; channel++) {
                    newBlocks[channel + offset] = page.getBlocks()[channel];
                }
                newPages[i] = new Page(page.getPositionCount(), newBlocks);
            }
        }
        return newPages;
    }

    private static Object getGroupValue(GroupedAccumulator groupedAggregation, int groupId)
    {
        BlockBuilder out = new BlockBuilder(groupedAggregation.getFinalTupleInfo());
        groupedAggregation.evaluateFinal(groupId, out);
        return BlockAssertions.toValues(out.build()).get(0).get(0);
    }
}
