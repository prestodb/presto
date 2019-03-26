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

import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.aggregation.groupByAggregations.AggregationTestInput;
import com.facebook.presto.operator.aggregation.groupByAggregations.AggregationTestInputBuilder;
import com.facebook.presto.operator.aggregation.groupByAggregations.AggregationTestOutput;
import com.facebook.presto.operator.aggregation.groupByAggregations.GroupByAggregationTestUtils;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;
import org.testng.internal.collections.Ints;

import java.util.*;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.block.BlockAssertions.*;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.testng.Assert.assertTrue;
import static com.facebook.presto.block.BlockAssertions.createRLEBlock;


public class TestApproximateHeavyHittersAggregation
{
    private static final FunctionManager functionManager = MetadataManager.createTestMetadataManager().getFunctionManager();

    @Test
    public void testSimple()
    {
        InternalAggregationFunction approxHeavyHitters = getAggregation(VARCHAR, DOUBLE);
        assertAggregation(
                approxHeavyHitters,
                ImmutableMap.of("a", 3L),
                createStringsBlock("a","b","c","a","a","b"),
                createRLEBlock(40.0, 6)
        );
    }

    @Test
    public void testEmpty()
    {
        InternalAggregationFunction approxHeavyHitters = getAggregation(VARCHAR, DOUBLE);
        assertAggregation(
                approxHeavyHitters,
                null,
                createStringsBlock((String) null),
                createRLEBlock(40.0, 1)
        );
    }

    @Test
    public void testNullOnly()
    {
        InternalAggregationFunction approxHeavyHitters = getAggregation(VARCHAR, DOUBLE);
        assertAggregation(
                approxHeavyHitters,
                ImmutableMap.of(null, 1L, null, 1L, null, 1L),
                createStringsBlock(null, null, null),
                createRLEBlock(40.0, 1)
        );
    }

    @Test
    public void testNullPartial()
    {
        InternalAggregationFunction approxHeavyHitters = getAggregation(VARCHAR, DOUBLE);
        assertAggregation(
                approxHeavyHitters,
                ImmutableMap.of(null, 1L, null, 1L, null, 1L),
                createStringsBlock(null, "a", null, "b", null),
                createRLEBlock(10.0, 1)
        );
    }


    @Test
    public void testEmptyStateOutputsNull()
    {
        InternalAggregationFunction approxHeavyHitters = getAggregation(VARCHAR, DOUBLE);
        GroupedAccumulator groupedAccumulator = approxHeavyHitters.bind(Ints.asList(new int[] {}), Optional.empty())
                .createGroupedAccumulator();
        BlockBuilder blockBuilder = groupedAccumulator.getFinalType().createBlockBuilder(null, 1000);

        groupedAccumulator.evaluateFinal(0, blockBuilder);
        assertTrue(blockBuilder.isNull(0));
    }

    @Test
    public void testWithMultiplePages()
    {
        InternalAggregationFunction approxHeavyHitters = getAggregation(VARCHAR, DOUBLE);

        AggregationTestInputBuilder testInputBuilder = new AggregationTestInputBuilder(
                new Block[] {
                        createStringsBlock("hello", "world", "hello", "world", "hello", "world", "goodbye"),
                        createRLEBlock(10.0, 7),
                },
                approxHeavyHitters
        );
        AggregationTestOutput testOutput = new AggregationTestOutput(ImmutableMap.of("hello", 3, "world", 3, "goodbye", 1));
        AggregationTestInput testInput = testInputBuilder.build();

        testInput.runPagesOnAccumulatorWithAssertion(0L, testInput.createGroupedAccumulator(), testOutput);
    }

    @Test
    public void testMultipleGroupsWithMultiplePages()
    {
        InternalAggregationFunction approxHeavyHitters = getAggregation(VARCHAR, DOUBLE);

        Block block1 = createStringsBlock("a", "b", "c", "d", "e");
        Block block2 = createStringsBlock("f", "g", "h", "i", "j");
        Block minPercentShare = createRLEBlock(9.0, 5);
        AggregationTestOutput aggregationTestOutput1 = new AggregationTestOutput(ImmutableMap.of("a", 1, "b", 1, "c", 1, "d", 1, "e", 1));
        AggregationTestInputBuilder testInputBuilder1 = new AggregationTestInputBuilder(
                new Block[] {block1, minPercentShare},
                approxHeavyHitters);
        AggregationTestInput test1 = testInputBuilder1.build();
        GroupedAccumulator groupedAccumulator = test1.createGroupedAccumulator();

        test1.runPagesOnAccumulatorWithAssertion(0L, groupedAccumulator, aggregationTestOutput1);

        AggregationTestOutput aggregationTestOutput2 = new AggregationTestOutput(ImmutableMap.of("f", 1, "g",  1, "h", 1,  "i", 1,  "j", 1));
        AggregationTestInputBuilder testBuilder2 = new AggregationTestInputBuilder(
                new Block[] {block2, minPercentShare},
                approxHeavyHitters);
        AggregationTestInput test2 = testBuilder2.build();
        test2.runPagesOnAccumulatorWithAssertion(255L, groupedAccumulator, aggregationTestOutput2);
    }

    @Test
    public void testManyValues()
    {
        // Test many values so multiple BlockBuilders will be used to store group state.
        InternalAggregationFunction approxHeavyHitters = getAggregation(VARCHAR, DOUBLE);

        int numGroups = 50000;
        int arraySize = 30;
        Random random = new Random();
        GroupedAccumulator groupedAccumulator = createGroupedAccumulator(approxHeavyHitters);
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
                    approxHeavyHitters);
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

    private InternalAggregationFunction getAggregation(Type... arguments)
    {
        return functionManager.getAggregateFunctionImplementation(functionManager.resolveFunction(TEST_SESSION, QualifiedName.of("approx_heavy_hitters"), fromTypes(arguments)));
    }
}
