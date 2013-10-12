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
import com.facebook.presto.block.RandomAccessBlock;
import com.facebook.presto.block.rle.RunLengthEncodedBlock;
import com.facebook.presto.block.uncompressed.FixedWidthBlock;
import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.FixedWidthTypeInfo;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slices;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.facebook.presto.block.BlockBuilders.createBlockBuilder;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_BOOLEAN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public final class AggregationTestUtils
{
    private AggregationTestUtils()
    {
    }

    public static void assertAggregation(AggregationFunction function, double confidence, Object expectedValue, int positions, Block... blocks)
    {
        if (positions == 0) {
            assertAggregation(function, confidence, expectedValue);
        }
        else {
            assertAggregation(function, confidence, expectedValue, new Page(positions, blocks));
        }
    }

    public static void assertApproximateAggregation(AggregationFunction function, int sampleWeightChannel, double confidence, Double expectedValue, Page... pages)
    {
        assertTrue(approximateAggregationWithinErrorBound(function, sampleWeightChannel, confidence, expectedValue, pages));
        assertTrue(partialApproximateAggregationWithinErrorBound(function, sampleWeightChannel, confidence, expectedValue, pages));
        assertTrue(groupedApproximateAggregationWithinErrorBound(function, sampleWeightChannel, confidence, expectedValue, pages));
    }

    public static boolean approximateAggregationWithinErrorBound(AggregationFunction function, int sampleWeightChannel, double confidence, Double expectedValue, Page... pages)
    {
        Accumulator accumulator = function.createAggregation(Optional.<Integer>absent(), Optional.of(sampleWeightChannel), confidence, 0);
        for (Page page : pages) {
            accumulator.addInput(page);
        }
        Block result = accumulator.evaluateFinal();

        if (expectedValue == null) {
            return BlockAssertions.toValues(result).get(0) == null;
        }

        return withinErrorBound(BlockAssertions.toValues(result).get(0).toString(), expectedValue);
    }

    public static boolean partialApproximateAggregationWithinErrorBound(AggregationFunction function, int sampleWeightChannel, double confidence, Double expectedValue, Page... pages)
    {
        Accumulator partialAccumulator = function.createAggregation(Optional.<Integer>absent(), Optional.of(sampleWeightChannel), confidence, 0);
        for (Page page : pages) {
            if (page.getPositionCount() > 0) {
                partialAccumulator.addInput(page);
            }
        }

        Block partialBlock = partialAccumulator.evaluateIntermediate();

        Accumulator finalAggregation = function.createIntermediateAggregation(confidence);
        finalAggregation.addIntermediate(partialBlock);

        Block finalBlock = finalAggregation.evaluateFinal();

        if (expectedValue == null) {
            return BlockAssertions.toValues(finalBlock).get(0) == null;
        }

        return withinErrorBound(BlockAssertions.toValues(finalBlock).get(0).toString(), expectedValue);
    }

    public static boolean groupedApproximateAggregationWithinErrorBound(AggregationFunction function, int sampleWeightChannel, double confidence, Double expectedValue, Page... pages)
    {
        GroupedAccumulator groupedAggregation = function.createGroupedAggregation(Optional.<Integer>absent(), Optional.of(sampleWeightChannel), confidence, 0);
        for (Page page : pages) {
            groupedAggregation.addInput(createGroupByIdBlock(0, page.getPositionCount()), page);
        }
        Object groupValue = getGroupValue(groupedAggregation, 0);

        if (expectedValue == null) {
            return groupValue == null;
        }

        return withinErrorBound(groupValue.toString(), expectedValue);
    }

    private static boolean withinErrorBound(String approximateValue, double expected)
    {
        List<String> parts = Splitter.on(' ').splitToList(approximateValue);
        double actual = Double.parseDouble(parts.get(0));
        double error = Double.parseDouble(parts.get(2));

        return Math.abs(expected - actual) <= error && !Double.isInfinite(error);
    }

    public static void assertAggregation(AggregationFunction function, double confidence, Object expectedValue, Page... pages)
    {
        assertEquals(aggregation(function, confidence, pages), expectedValue);
        assertEquals(partialAggregation(function, confidence, pages), expectedValue);
        if (pages.length > 0) {
            assertEquals(groupedAggregation(function, confidence, pages), expectedValue);
            assertEquals(groupedPartialAggregation(function, confidence, pages), expectedValue);
            assertEquals(distinctAggregation(function, confidence, pages), expectedValue);
        }
    }

    public static Object distinctAggregation(AggregationFunction function, double confidence, Page... pages)
    {
        Optional<Integer> maskChannel = Optional.of(pages[0].getChannelCount());
        // Execute normally
        Object aggregation = aggregation(function, createArgs(function), maskChannel, confidence, maskPages(true, pages));
        Page[] dupedPages = new Page[pages.length * 2];
        // Create two copies of each page with one of them masked off
        System.arraycopy(maskPages(true, pages), 0, dupedPages, 0, pages.length);
        System.arraycopy(maskPages(false, pages), 0, dupedPages, pages.length, pages.length);
        // Execute with masked pages and assure equal to normal execution
        Object aggregationWithDupes = aggregation(function, createArgs(function), maskChannel, confidence, dupedPages);

        assertEquals(aggregationWithDupes, aggregation, "Inconsistent results with mask");

        return aggregation;
    }

    // Adds the mask as the last channel
    private static Page[] maskPages(boolean maskValue, Page... pages)
    {
        Page[] maskedPages = new Page[pages.length];
        for (int i = 0; i < pages.length; i++) {
            Page page = pages[i];
            int positionCount = page.getPositionCount();
            int blockSize = SINGLE_BOOLEAN.getFixedSize() * positionCount;
            BlockBuilder blockBuilder = createBlockBuilder(SINGLE_BOOLEAN, Slices.allocate(blockSize));
            for (int j = 0; j < page.getPositionCount(); j++) {
                blockBuilder.append(maskValue);
            }
            Block[] sourceBlocks = page.getBlocks();
            Block[] outputBlocks = new Block[sourceBlocks.length + 1]; // +1 for the single boolean output channel

            System.arraycopy(sourceBlocks, 0, outputBlocks, 0, sourceBlocks.length);
            outputBlocks[sourceBlocks.length] = blockBuilder.build();

            maskedPages[i] = new Page(outputBlocks);
        }

        return maskedPages;
    }

    public static Object aggregation(AggregationFunction function, double confidence, Page... pages)
    {
        // execute with args in positions: arg0, arg1, arg2
        Object aggregation = aggregation(function, createArgs(function), Optional.<Integer>absent(), confidence, pages);

        // execute with args in reverse order: arg2, arg1, arg0
        if (function.getParameterTypes().size() > 1) {
            Object aggregationWithOffset = aggregation(function, reverseArgs(function), Optional.<Integer>absent(), confidence, reverseColumns(pages));
            assertEquals(aggregationWithOffset, aggregation, "Inconsistent results with reversed channels");
        }

        // execute with args at an offset (and possibly reversed): null, null, null, arg2, arg1, arg0
        Object aggregationWithOffset = aggregation(function, offsetArgs(function, 3), Optional.<Integer>absent(), confidence, offsetColumns(pages, 3));
        assertEquals(aggregationWithOffset, aggregation, "Inconsistent results with channel offset");

        return aggregation;
    }

    private static Object aggregation(AggregationFunction function, int[] args, Optional<Integer> maskChannel, double confidence, Page... pages)
    {
        Accumulator aggregation = function.createAggregation(maskChannel, Optional.<Integer>absent(), confidence, args);
        for (Page page : pages) {
            if (page.getPositionCount() > 0) {
                aggregation.addInput(page);
            }
        }

        Block block = aggregation.evaluateFinal();
        return BlockAssertions.getOnlyValue(block);
    }

    public static Object partialAggregation(AggregationFunction function, double confidence, Page... pages)
    {
        // execute with args in positions: arg0, arg1, arg2
        Object aggregation = partialAggregation(function, confidence, createArgs(function), pages);

        // execute with args in reverse order: arg2, arg1, arg0
        if (function.getParameterTypes().size() > 1) {
            Object aggregationWithOffset = partialAggregation(function, confidence, reverseArgs(function), reverseColumns(pages));
            assertEquals(aggregationWithOffset, aggregation, "Inconsistent results with reversed channels");
        }

        // execute with args at an offset (and possibly reversed): null, null, null, arg2, arg1, arg0
        Object aggregationWithOffset = partialAggregation(function, confidence, offsetArgs(function, 3), offsetColumns(pages, 3));
        assertEquals(aggregationWithOffset, aggregation, "Inconsistent results with channel offset");

        return aggregation;
    }

    public static Object partialAggregation(AggregationFunction function, double confidence, int[] args, Page... pages)
    {
        Accumulator partialAggregation = function.createAggregation(Optional.<Integer>absent(), Optional.<Integer>absent(), confidence, args);
        for (Page page : pages) {
            if (page.getPositionCount() > 0) {
                partialAggregation.addInput(page);
            }
        }

        Block partialBlock = partialAggregation.evaluateIntermediate();

        Accumulator finalAggregation = function.createIntermediateAggregation(confidence);
        // Test handling of empty intermediate blocks
        Block emptyBlock = function.createAggregation(Optional.<Integer>absent(), Optional.<Integer>absent(), confidence, args).evaluateIntermediate();
        finalAggregation.addIntermediate(emptyBlock);
        finalAggregation.addIntermediate(partialBlock);

        Block finalBlock = finalAggregation.evaluateFinal();
        return BlockAssertions.getOnlyValue(finalBlock);
    }

    public static Object groupedAggregation(AggregationFunction function, double confidence, Page... pages)
    {
        // execute with args in positions: arg0, arg1, arg2
        Object aggregation = groupedAggregation(function, confidence, createArgs(function), pages);

        // execute with args in reverse order: arg2, arg1, arg0
        if (function.getParameterTypes().size() > 1) {
            Object aggregationWithOffset = groupedAggregation(function, confidence, reverseArgs(function), reverseColumns(pages));
            assertEquals(aggregationWithOffset, aggregation, "Inconsistent results with reversed channels");
        }

        // execute with args at an offset (and possibly reversed): null, null, null, arg2, arg1, arg0
        Object aggregationWithOffset = groupedAggregation(function, confidence, offsetArgs(function, 3), offsetColumns(pages, 3));
        assertEquals(aggregationWithOffset, aggregation, "Inconsistent results with channel offset");

        return aggregation;
    }

    public static Object groupedAggregation(AggregationFunction function, double confidence, int[] args, Page... pages)
    {
        GroupedAccumulator groupedAggregation = function.createGroupedAggregation(Optional.<Integer>absent(), Optional.<Integer>absent(), confidence, args);
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

    public static Object groupedPartialAggregation(AggregationFunction function, double confidence, Page... pages)
    {
        // execute with args in positions: arg0, arg1, arg2
        Object aggregation = groupedPartialAggregation(function, confidence, createArgs(function), pages);

        // execute with args in reverse order: arg2, arg1, arg0
        if (function.getParameterTypes().size() > 1) {
            Object aggregationWithOffset = groupedPartialAggregation(function, confidence, reverseArgs(function), reverseColumns(pages));
            assertEquals(aggregationWithOffset, aggregation, "Inconsistent results with reversed channels");
        }

        // execute with args at an offset (and possibly reversed): null, null, null, arg2, arg1, arg0
        Object aggregationWithOffset = groupedPartialAggregation(function, confidence, offsetArgs(function, 3), offsetColumns(pages, 3));
        assertEquals(aggregationWithOffset, aggregation, "Inconsistent results with channel offset");

        return aggregation;
    }

    public static Object groupedPartialAggregation(AggregationFunction function, double confidence, int[] args, Page... pages)
    {
        GroupedAccumulator partialAggregation = function.createGroupedAggregation(Optional.<Integer>absent(), Optional.<Integer>absent(), confidence, args);
        for (Page page : pages) {
            partialAggregation.addInput(createGroupByIdBlock(0, page.getPositionCount()), page);
        }

        BlockBuilder partialOut = createBlockBuilder(partialAggregation.getIntermediateTupleInfo());
        partialAggregation.evaluateIntermediate(0, partialOut);
        Block partialBlock = partialOut.build();

        GroupedAccumulator finalAggregation = function.createGroupedIntermediateAggregation(confidence);
        // Add an empty block to test the handling of empty intermediates
        GroupedAccumulator emptyAggregation = function.createGroupedAggregation(Optional.<Integer>absent(), Optional.<Integer>absent(), confidence, args);
        BlockBuilder emptyOut = createBlockBuilder(emptyAggregation.getIntermediateTupleInfo());
        emptyAggregation.evaluateIntermediate(0, emptyOut);
        Block emptyBlock = emptyOut.build();
        finalAggregation.addIntermediate(createGroupByIdBlock(0, emptyBlock.getPositionCount()), emptyBlock);

        finalAggregation.addIntermediate(createGroupByIdBlock(0, partialBlock.getPositionCount()), partialBlock);

        return getGroupValue(finalAggregation, 0);
    }

    public static GroupByIdBlock createGroupByIdBlock(int groupId, int positions)
    {
        if (positions == 0) {
            return new GroupByIdBlock(groupId, new FixedWidthBlock(new FixedWidthTypeInfo(Type.FIXED_INT_64), 0, Slices.EMPTY_SLICE));
        }

        BlockBuilder blockBuilder = createBlockBuilder(TupleInfo.SINGLE_LONG);
        for (int i = 0; i < positions; i++) {
            blockBuilder.append(groupId);
        }
        return new GroupByIdBlock(groupId, (FixedWidthBlock) blockBuilder.build().toRandomAccessBlock());
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
                    newBlocks[channel] = createNullRLEBlock(page.getPositionCount());
                }
                for (int channel = 0; channel < page.getBlocks().length; channel++) {
                    newBlocks[channel + offset] = page.getBlocks()[channel];
                }
                newPages[i] = new Page(page.getPositionCount(), newBlocks);
            }
        }
        return newPages;
    }

    private static RunLengthEncodedBlock createNullRLEBlock(int positionCount)
    {
        RandomAccessBlock value = createBlockBuilder(SINGLE_BOOLEAN)
                .appendNull()
                .build()
                .toRandomAccessBlock();

        return new RunLengthEncodedBlock(value, positionCount);
    }

    private static Object getGroupValue(GroupedAccumulator groupedAggregation, int groupId)
    {
        BlockBuilder out = createBlockBuilder(groupedAggregation.getFinalTupleInfo());
        groupedAggregation.evaluateFinal(groupId, out);
        return BlockAssertions.getOnlyValue(out.build());
    }
}
