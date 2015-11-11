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

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.util.Numbers.isFloatingNumber;
import static com.facebook.presto.util.Numbers.isNan;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public final class AggregationTestUtils
{
    private AggregationTestUtils()
    {
    }

    public static void assertAggregation(InternalAggregationFunction function, double confidence, Object expectedValue, Block... blocks)
    {
        int positions = blocks[0].getPositionCount();
        for (int i = 1; i < blocks.length; i++) {
            assertEquals(positions, blocks[i].getPositionCount(), "input blocks provided are not equal in position count");
        }
        if (positions == 0) {
            assertAggregation(function, confidence, expectedValue, new Page[] {});
        }
        else if (positions == 1) {
            assertAggregation(function, confidence, expectedValue, new Page(positions, blocks));
        }
        else {
            int split = positions / 2; // [0, split - 1] goes to first list of blocks; [split, positions - 1] goes to second list of blocks.
            Block[] blockArray1 = new Block[blocks.length];
            Block[] blockArray2 = new Block[blocks.length];
            for (int i = 0; i < blocks.length; i++) {
                blockArray1[i] = blocks[i].getRegion(0, split);
                blockArray2[i] = blocks[i].getRegion(split, positions - split);
            }
            assertAggregation(function, confidence, expectedValue, new Page(blockArray1), new Page(blockArray2));
        }
    }

    public static void assertApproximateAggregation(InternalAggregationFunction function, int sampleWeightChannel, double confidence, Double expectedValue, Page... pages)
    {
        assertTrue(approximateAggregationWithinErrorBound(function, sampleWeightChannel, confidence, expectedValue, pages));
        assertTrue(partialApproximateAggregationWithinErrorBound(function, sampleWeightChannel, confidence, expectedValue, pages));
        assertTrue(groupedApproximateAggregationWithinErrorBound(function, sampleWeightChannel, confidence, expectedValue, pages));
    }

    public static boolean approximateAggregationWithinErrorBound(InternalAggregationFunction function, int sampleWeightChannel, double confidence, Double expectedValue, Page... pages)
    {
        Accumulator accumulator = function.bind(ImmutableList.of(0), Optional.empty(), Optional.of(sampleWeightChannel), confidence).createAccumulator();
        for (Page page : pages) {
            accumulator.addInput(page);
        }
        Block result = getFinalBlock(accumulator);

        if (expectedValue == null) {
            return BlockAssertions.toValues(function.getFinalType(), result).get(0) == null;
        }

        return withinErrorBound(BlockAssertions.toValues(function.getFinalType(), result).get(0).toString(), expectedValue);
    }

    public static Block getIntermediateBlock(Accumulator accumulator)
    {
        BlockBuilder blockBuilder = accumulator.getIntermediateType().createBlockBuilder(new BlockBuilderStatus(), 1000);
        accumulator.evaluateIntermediate(blockBuilder);
        return blockBuilder.build();
    }

    public static Block getIntermediateBlock(GroupedAccumulator accumulator)
    {
        BlockBuilder blockBuilder = accumulator.getIntermediateType().createBlockBuilder(new BlockBuilderStatus(), 1000);
        accumulator.evaluateIntermediate(0, blockBuilder);
        return blockBuilder.build();
    }

    public static Block getFinalBlock(Accumulator accumulator)
    {
        BlockBuilder blockBuilder = accumulator.getFinalType().createBlockBuilder(new BlockBuilderStatus(), 1000);
        accumulator.evaluateFinal(blockBuilder);
        return blockBuilder.build();
    }

    public static Block getFinalBlock(GroupedAccumulator accumulator)
    {
        BlockBuilder blockBuilder = accumulator.getFinalType().createBlockBuilder(new BlockBuilderStatus(), 1000);
        accumulator.evaluateFinal(0, blockBuilder);
        return blockBuilder.build();
    }

    public static boolean partialApproximateAggregationWithinErrorBound(InternalAggregationFunction function, int sampleWeightChannel, double confidence, Double expectedValue, Page... pages)
    {
        AccumulatorFactory factory = function.bind(ImmutableList.of(0), Optional.empty(), Optional.of(sampleWeightChannel), confidence);
        Accumulator partialAccumulator = factory.createAccumulator();
        for (Page page : pages) {
            if (page.getPositionCount() > 0) {
                partialAccumulator.addInput(page);
            }
        }

        Block partialBlock = getIntermediateBlock(partialAccumulator);

        Accumulator finalAggregation = factory.createIntermediateAccumulator();
        finalAggregation.addIntermediate(partialBlock);

        Block finalBlock = getFinalBlock(finalAggregation);

        if (expectedValue == null) {
            return BlockAssertions.toValues(function.getFinalType(), finalBlock).get(0) == null;
        }

        return withinErrorBound(BlockAssertions.toValues(function.getFinalType(), finalBlock).get(0).toString(), expectedValue);
    }

    public static boolean groupedApproximateAggregationWithinErrorBound(InternalAggregationFunction function, int sampleWeightChannel, double confidence, Double expectedValue, Page... pages)
    {
        GroupedAccumulator groupedAggregation = function.bind(ImmutableList.of(0), Optional.empty(), Optional.of(sampleWeightChannel), confidence).createGroupedAccumulator();
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

    private static void assertAggregation(InternalAggregationFunction function, double confidence, Object expectedValue, Page... pages)
    {
        BiConsumer<Object, Object> equalAssertion = (actual, expected) -> {
            assertEquals(actual, expected);
        };
        if (isFloatingNumber(expectedValue) && !isNan(expectedValue)) {
            equalAssertion = (actual, expected) -> {
                assertEquals((double) actual, (double) expected, 1e-10);
            };
        }

        // This assertAggregation does not try to split up the page to test the correctness of combine function.
        // Do not use this directly. Always use the other assertAggregation.
        equalAssertion.accept(aggregation(function, confidence, pages), expectedValue);
        equalAssertion.accept(partialAggregation(function, confidence, pages), expectedValue);
        if (pages.length > 0) {
            equalAssertion.accept(groupedAggregation(function, confidence, pages), expectedValue);
            equalAssertion.accept(groupedPartialAggregation(function, confidence, pages), expectedValue);
            equalAssertion.accept(distinctAggregation(function, confidence, pages), expectedValue);
        }
    }

    public static Object distinctAggregation(InternalAggregationFunction function, double confidence, Page... pages)
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
            BlockBuilder blockBuilder = BOOLEAN.createBlockBuilder(new BlockBuilderStatus(), page.getPositionCount());
            for (int j = 0; j < page.getPositionCount(); j++) {
                BOOLEAN.writeBoolean(blockBuilder, maskValue);
            }
            Block[] sourceBlocks = page.getBlocks();
            Block[] outputBlocks = new Block[sourceBlocks.length + 1]; // +1 for the single boolean output channel

            System.arraycopy(sourceBlocks, 0, outputBlocks, 0, sourceBlocks.length);
            outputBlocks[sourceBlocks.length] = blockBuilder.build();

            maskedPages[i] = new Page(outputBlocks);
        }

        return maskedPages;
    }

    public static Object aggregation(InternalAggregationFunction function, double confidence, Page... pages)
    {
        // execute with args in positions: arg0, arg1, arg2
        Object aggregation = aggregation(function, createArgs(function), Optional.empty(), confidence, pages);

        // execute with args in reverse order: arg2, arg1, arg0
        if (function.getParameterTypes().size() > 1) {
            Object aggregationWithOffset = aggregation(function, reverseArgs(function), Optional.empty(), confidence, reverseColumns(pages));
            assertEquals(aggregationWithOffset, aggregation, "Inconsistent results with reversed channels");
        }

        // execute with args at an offset (and possibly reversed): null, null, null, arg2, arg1, arg0
        Object aggregationWithOffset = aggregation(function, offsetArgs(function, 3), Optional.empty(), confidence, offsetColumns(pages, 3));
        assertEquals(aggregationWithOffset, aggregation, "Inconsistent results with channel offset");

        return aggregation;
    }

    private static Object aggregation(InternalAggregationFunction function, int[] args, Optional<Integer> maskChannel, double confidence, Page... pages)
    {
        Accumulator aggregation = function.bind(Ints.asList(args), maskChannel, Optional.empty(), confidence).createAccumulator();
        for (Page page : pages) {
            if (page.getPositionCount() > 0) {
                aggregation.addInput(page);
            }
        }

        Block block = getFinalBlock(aggregation);
        return BlockAssertions.getOnlyValue(aggregation.getFinalType(), block);
    }

    public static Object partialAggregation(InternalAggregationFunction function, double confidence, Page... pages)
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

    public static Object partialAggregation(InternalAggregationFunction function, double confidence, int[] args, Page... pages)
    {
        AccumulatorFactory factory = function.bind(Ints.asList(args), Optional.empty(), Optional.empty(), confidence);
        Accumulator finalAggregation = factory.createIntermediateAccumulator();

        // Test handling of empty intermediate blocks
        Accumulator emptyAggregation = factory.createAccumulator();
        Block emptyBlock = getIntermediateBlock(emptyAggregation);

        finalAggregation.addIntermediate(emptyBlock);

        for (Page page : pages) {
            Accumulator partialAggregation = factory.createAccumulator();
            if (page.getPositionCount() > 0) {
                partialAggregation.addInput(page);
            }
            Block partialBlock = getIntermediateBlock(partialAggregation);
            finalAggregation.addIntermediate(partialBlock);
        }

        finalAggregation.addIntermediate(emptyBlock);

        Block finalBlock = getFinalBlock(finalAggregation);
        return BlockAssertions.getOnlyValue(finalAggregation.getFinalType(), finalBlock);
    }

    public static Object groupedAggregation(InternalAggregationFunction function, double confidence, Page... pages)
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

    public static Object groupedAggregation(InternalAggregationFunction function, double confidence, int[] args, Page... pages)
    {
        GroupedAccumulator groupedAggregation = function.bind(Ints.asList(args), Optional.empty(), Optional.empty(), confidence).createGroupedAccumulator();
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

    public static Object groupedPartialAggregation(InternalAggregationFunction function, double confidence, Page... pages)
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

    public static Object groupedPartialAggregation(InternalAggregationFunction function, double confidence, int[] args, Page... pages)
    {
        AccumulatorFactory factory = function.bind(Ints.asList(args), Optional.empty(), Optional.empty(), confidence);
        GroupedAccumulator finalAggregation = factory.createGroupedIntermediateAccumulator();

        // Add an empty block to test the handling of empty intermediates
        GroupedAccumulator emptyAggregation = factory.createGroupedAccumulator();
        Block emptyBlock = getIntermediateBlock(emptyAggregation);

        finalAggregation.addIntermediate(createGroupByIdBlock(0, emptyBlock.getPositionCount()), emptyBlock);

        for (Page page : pages) {
            GroupedAccumulator partialAggregation = factory.createGroupedAccumulator();
            partialAggregation.addInput(createGroupByIdBlock(0, page.getPositionCount()), page);
            Block partialBlock = getIntermediateBlock(partialAggregation);
            finalAggregation.addIntermediate(createGroupByIdBlock(0, partialBlock.getPositionCount()), partialBlock);
        }

        finalAggregation.addIntermediate(createGroupByIdBlock(0, emptyBlock.getPositionCount()), emptyBlock);

        return getGroupValue(finalAggregation, 0);
    }

    public static GroupByIdBlock createGroupByIdBlock(int groupId, int positions)
    {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), positions);
        for (int i = 0; i < positions; i++) {
            BIGINT.writeLong(blockBuilder, groupId);
        }
        return new GroupByIdBlock(groupId, blockBuilder.build());
    }

    private static int[] createArgs(InternalAggregationFunction function)
    {
        int[] args = new int[function.getParameterTypes().size()];
        for (int i = 0; i < args.length; i++) {
            args[i] = i;
        }
        return args;
    }

    private static int[] reverseArgs(InternalAggregationFunction function)
    {
        int[] args = createArgs(function);
        Collections.reverse(Ints.asList(args));
        return args;
    }

    private static int[] offsetArgs(InternalAggregationFunction function, int offset)
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
            Block[] newBlocks = new Block[page.getChannelCount() + offset];
            for (int channel = 0; channel < offset; channel++) {
                newBlocks[channel] = createNullRLEBlock(page.getPositionCount());
            }
            for (int channel = 0; channel < page.getBlocks().length; channel++) {
                newBlocks[channel + offset] = page.getBlocks()[channel];
            }
            newPages[i] = new Page(page.getPositionCount(), newBlocks);
        }
        return newPages;
    }

    private static RunLengthEncodedBlock createNullRLEBlock(int positionCount)
    {
        Block value = BOOLEAN.createBlockBuilder(new BlockBuilderStatus(), 1)
                .appendNull()
                .build();

        return new RunLengthEncodedBlock(value, positionCount);
    }

    private static Object getGroupValue(GroupedAccumulator groupedAggregation, int groupId)
    {
        BlockBuilder out = groupedAggregation.getFinalType().createBlockBuilder(new BlockBuilderStatus(), 1);
        groupedAggregation.evaluateFinal(groupId, out);
        return BlockAssertions.getOnlyValue(groupedAggregation.getFinalType(), out.build());
    }

    public static double[] constructDoublePrimitiveArray(int start, int length)
    {
        return IntStream.range(start, start + length).asDoubleStream().toArray();
    }
}
