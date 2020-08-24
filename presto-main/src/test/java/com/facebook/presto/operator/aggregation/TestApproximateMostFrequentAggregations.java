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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.operator.aggregation.groupByAggregations.AggregationTestInput;
import com.facebook.presto.operator.aggregation.groupByAggregations.AggregationTestInputBuilder;
import com.facebook.presto.operator.aggregation.groupByAggregations.AggregationTestOutput;
import com.facebook.presto.operator.aggregation.groupByAggregations.GroupByAggregationTestUtils;
import com.facebook.presto.operator.aggregation.mostfrequent.state.TestTopElementsHistogram;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;
import org.testng.internal.collections.Ints;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.aggregation;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.groupedAggregation;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestApproximateMostFrequentAggregations
        extends AbstractTestFunctions
{
    @Test
    public void testSimple()
    {
        InternalAggregationFunction approxMostFrequent = getAggregation(VARCHAR, DOUBLE);
        assertAggregation(
                approxMostFrequent,
                ImmutableMap.of("a", 3L, "b", 2L),
                createStringsBlock("a", "b", "c", "a", "a", "b"),
                createRLEBlock(30.0, 6));
    }

    @Test
    public void testWithError()
    {
        InternalAggregationFunction approxMostFrequent = getAggregation(VARCHAR, DOUBLE, DOUBLE);
        assertAggregation(
                approxMostFrequent,
                ImmutableMap.of("a", 3L, "b", 2L),
                createStringsBlock("a", "b", "c", "a", "a", "b"),
                createRLEBlock(30.0, 6),
                createRLEBlock(0.01, 6));
    }

    @Test
    public void testWithErrorConfidence()
    {
        InternalAggregationFunction approxMostFrequent = getAggregation(VARCHAR, DOUBLE, DOUBLE, DOUBLE);
        assertAggregation(
                approxMostFrequent,
                ImmutableMap.of("a", 3L, "b", 2L),
                createStringsBlock("a", "b", "c", "a", "a", "b"),
                createRLEBlock(30.0, 6),
                createRLEBlock(0.01, 6),
                createRLEBlock(0.99, 6));
    }

    @Test
    public void testSimpleIncrement()
    {
        InternalAggregationFunction approxMostFrequent = getAggregation(VARCHAR, BIGINT, DOUBLE);
        assertAggregation(
                approxMostFrequent,
                ImmutableMap.of("a", 3L, "b", 2L),
                createStringsBlock("a", "b", "c"),
                createLongsBlock(3L, 2L, 1L),
                createRLEBlock(30.0, 3));
    }

    public static Page[] getPages()
    {
        List<String> lst = TestTopElementsHistogram.getDeterministicDataSet();
        Collections.shuffle(lst);
        List<String> itemList = new ArrayList<>();
        List<Long> countList = new ArrayList<>();
        for (String entry : lst) {
            String[] e = entry.split(":");
            itemList.add(e[0]);
            countList.add(Long.valueOf(e[1]));
        }

        int subListStart = (int) Math.floor(lst.size() / 2);

        Page[] pages = new Page[2];
        pages[0] = new Page(createStringsBlock(itemList.subList(0, subListStart)),
                createLongsBlock(countList.subList(0, subListStart)),
                createRLEBlock(0.02, subListStart));
        pages[1] = new Page(createStringsBlock(itemList.subList(subListStart, lst.size())),
                        createLongsBlock(countList.subList(subListStart, lst.size())),
                        createRLEBlock(0.02, lst.size() - subListStart));
        return pages;
    }

    @Test
    public void testLargeDataset()
    {
        Page[] pages = getPages();
        InternalAggregationFunction approxMostFrequent = getAggregation(VARCHAR, BIGINT, DOUBLE);
        Map<String, Long> result = (Map<String, Long>) aggregation(approxMostFrequent, pages);
        assertTrue(result.containsKey("top0"));
        assertTrue(result.containsKey("top1"));
        assertTrue(result.containsKey("top2"));
        assertTrue(result.containsKey("top3"));
    }

    @Test
    public void testEmpty()
    {
        InternalAggregationFunction approxMostFrequent = getAggregation(VARCHAR, DOUBLE);
        assertAggregation(
                approxMostFrequent,
                ImmutableMap.of(),
                createStringsBlock((String) null),
                createRLEBlock(40.0, 1));
    }

    @Test
    public void testNullOnly()
    {
        InternalAggregationFunction approxMostFrequent = getAggregation(VARCHAR, DOUBLE);
        assertAggregation(
                approxMostFrequent,
                ImmutableMap.of(),
                createStringsBlock((String) null, (String) null, (String) null),
                createRLEBlock(40.0, 3));
    }

    @Test
    public void testNullPartial()
    {
        InternalAggregationFunction approxMostFrequent = getAggregation(VARCHAR, DOUBLE);
        assertAggregation(
                approxMostFrequent,
                ImmutableMap.of("a", 1L, "b", 1L),
                createStringsBlock((String) null, "a", (String) null, "b", (String) null),
                createRLEBlock(10.0, 5));
    }

    @Test
    public void testSaturation()
    {
        int numItems = 1000000;
        String[] guids = new String[numItems];
        for (int i = 0; i < numItems; i++) {
            guids[i] = UUID.randomUUID().toString();
        }
        InternalAggregationFunction approxMostFrequent = getAggregation(VARCHAR, DOUBLE, DOUBLE, DOUBLE);
        assertAggregation(
                approxMostFrequent,
                ImmutableMap.of(),
                createStringsBlock(guids),
                createRLEBlock(11.0, numItems),
                createRLEBlock(0.01, numItems),
                createRLEBlock(0.99, numItems));
    }

    @Test
    public void testEmptyStateOutputsNull()
    {
        InternalAggregationFunction approxMostFrequent = getAggregation(VARCHAR, DOUBLE);
        GroupedAccumulator groupedAccumulator = approxMostFrequent.bind(Ints.asList(new int[] {}), Optional.empty())
                .createGroupedAccumulator();
        BlockBuilder blockBuilder = groupedAccumulator.getFinalType().createBlockBuilder(null, 1000);

        groupedAccumulator.evaluateFinal(0, blockBuilder);
        assertFalse(blockBuilder.isNull(0));
    }

    @Test
    public void testWithMultiplePages()
    {
        InternalAggregationFunction approxMostFrequent = getAggregation(VARCHAR, DOUBLE);

        AggregationTestInputBuilder testInputBuilder = new AggregationTestInputBuilder(
                new Block[] {
                        createStringsBlock("hello", "world", "hello", "world", "hello", "world", "goodbye"),
                        createRLEBlock(10.0, 7),
                },
                approxMostFrequent);
        AggregationTestOutput testOutput = new AggregationTestOutput(ImmutableMap.of("hello", 3L, "world", 3L, "goodbye", 1L));
        AggregationTestInput testInput = testInputBuilder.build();

        testInput.runPagesOnAccumulatorWithAssertion(0L, testInput.createGroupedAccumulator(), testOutput);
    }

    @Test
    public void testLargeWithMultiplePages()
    {
        InternalAggregationFunction approxMostFrequent = getAggregation(VARCHAR, BIGINT, DOUBLE);
        Page[] pages = getPages();
        Map<String, Long> result = (Map<String, Long>) groupedAggregation(approxMostFrequent, pages);
        assertTrue(result.containsKey("top0"));
        assertTrue(result.containsKey("top1"));
        assertTrue(result.containsKey("top2"));
        assertTrue(result.containsKey("top3"));
    }

    @Test
    public void testMultipleGroupsWithMultiplePages()
    {
        InternalAggregationFunction approxMostFrequent = getAggregation(VARCHAR, DOUBLE);

        Block block1 = createStringsBlock("a", "b", "c", "d", "e");
        Block block2 = createStringsBlock("f", "g", "h", "i", "j");
        Block minPercentShare = createRLEBlock(9.0, 5);
        AggregationTestOutput aggregationTestOutput1 = new AggregationTestOutput(ImmutableMap.of("a", 1L, "b", 1L, "c", 1L, "d", 1L, "e", 1L));
        AggregationTestInputBuilder testInputBuilder1 = new AggregationTestInputBuilder(
                new Block[] {block1, minPercentShare},
                approxMostFrequent);
        AggregationTestInput test1 = testInputBuilder1.build();
        GroupedAccumulator groupedAccumulator = test1.createGroupedAccumulator();

        test1.runPagesOnAccumulatorWithAssertion(0L, groupedAccumulator, aggregationTestOutput1);

        AggregationTestOutput aggregationTestOutput2 = new AggregationTestOutput(ImmutableMap.of("f", 1L, "g", 1L, "h", 1L, "i", 1L, "j", 1L));
        AggregationTestInputBuilder testBuilder2 = new AggregationTestInputBuilder(
                new Block[] {block2, minPercentShare},
                approxMostFrequent);
        AggregationTestInput test2 = testBuilder2.build();
        test2.runPagesOnAccumulatorWithAssertion(255L, groupedAccumulator, aggregationTestOutput2);
    }

    @Test
    public void testManyValues()
    {
        // Test many values so multiple BlockBuilders will be used to store group state.
        InternalAggregationFunction approxMostFrequent = getAggregation(VARCHAR, DOUBLE);

        int numGroups = 500;
        int arraySize = 30;
        Random random = new Random();
        GroupedAccumulator groupedAccumulator = createGroupedAccumulator(approxMostFrequent);
        Block minPercentShare = createRLEBlock(2.0, arraySize);

        for (int j = 0; j < numGroups; j++) {
            Map<String, Long> expectedValues = new HashMap<>();
            List<String> valueList = new ArrayList<>();

            for (int i = 0; i < arraySize; i++) {
                String str = String.valueOf(random.nextInt());
                valueList.add(str);
                expectedValues.merge(str, 1L, Long::sum);
            }

            Block block = createStringsBlock(valueList);
            AggregationTestInputBuilder testInputBuilder = new AggregationTestInputBuilder(
                    new Block[] {block, minPercentShare},
                    approxMostFrequent);
            AggregationTestInput test1 = testInputBuilder.build();

            test1.runPagesOnAccumulatorWithAssertion(j, groupedAccumulator, new AggregationTestOutput(expectedValues));
        }
    }

    private GroupedAccumulator createGroupedAccumulator(InternalAggregationFunction function)
    {
        int[] args = GroupByAggregationTestUtils.createArgs(function);

        return function.bind(Ints.asList(args), Optional.empty())
                .createGroupedAccumulator();
    }

    private InternalAggregationFunction getAggregation(Type... type)
    {
        FunctionManager functionManager = functionAssertions.getMetadata().getFunctionManager();
        return functionAssertions.getMetadata().getFunctionManager().getAggregateFunctionImplementation(
                functionManager.lookupFunction("approx_most_frequent", fromTypes(type)));
    }
}
