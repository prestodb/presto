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
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;
import org.testng.internal.collections.Ints;

import java.util.*;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.block.BlockAssertions.*;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.testng.Assert.assertTrue;

public class TestApproximateHeavyHittersAggregation
{
    private static final FunctionManager functionManager = MetadataManager.createTestMetadataManager().getFunctionManager();

    @Test
    public void testEmpty()
    {
        InternalAggregationFunction approxHeavyHitters = getAggregation(VARCHAR, DOUBLE);
        assertAggregation(
                approxHeavyHitters,
                ImmutableMap.of("a", 3L, "b", 2L, "c", 1L),
                createStringsBlock("a","b","c","a","a","b")
        );
    }

    @Test
    public void testNullOnly()
    {
        InternalAggregationFunction bigIntAgg = getAggregation(BIGINT);
        assertAggregation(
                bigIntAgg,
                Arrays.asList(null, null, null),
                createLongsBlock(new Long[] {null, null, null}));
    }

    @Test
    public void testNullPartial()
    {
        InternalAggregationFunction bigIntAgg = getAggregation(BIGINT);
        assertAggregation(
                bigIntAgg,
                Arrays.asList(null, 2L, null, 3L, null),
                createLongsBlock(new Long[] {null, 2L, null, 3L, null}));
    }

    @Test
    public void testBoolean()
    {
        InternalAggregationFunction booleanAgg = getAggregation(BOOLEAN);
        assertAggregation(
                booleanAgg,
                Arrays.asList(true, false),
                createBooleansBlock(new Boolean[] {true, false}));
    }

    @Test
    public void testBigInt()
    {
        InternalAggregationFunction bigIntAgg = getAggregation(BIGINT);
        assertAggregation(
                bigIntAgg,
                Arrays.asList(2L, 1L, 2L),
                createLongsBlock(new Long[] {2L, 1L, 2L}));
    }

    @Test
    public void testVarchar()
    {
        InternalAggregationFunction varcharAgg = getAggregation(VARCHAR);
        assertAggregation(
                varcharAgg,
                Arrays.asList("hello", "world"),
                createStringsBlock(new String[] {"hello", "world"}));
    }

    @Test
    public void testDate()
    {
        InternalAggregationFunction varcharAgg = getAggregation(DATE);
        assertAggregation(
                varcharAgg,
                Arrays.asList(new SqlDate(1), new SqlDate(2), new SqlDate(4)),
                createTypedLongsBlock(DATE, ImmutableList.of(1L, 2L, 4L)));
    }

    @Test
    public void testArray()
    {
        InternalAggregationFunction varcharAgg = getAggregation(new ArrayType(BIGINT));
        assertAggregation(
                varcharAgg,
                Arrays.asList(Arrays.asList(1L), Arrays.asList(1L, 2L), Arrays.asList(1L, 2L, 3L)),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(1L), ImmutableList.of(1L, 2L), ImmutableList.of(1L, 2L, 3L))));
    }

    @Test
    public void testEmptyStateOutputsNull()
    {
        InternalAggregationFunction bigIntAgg = getAggregation(BIGINT);
        GroupedAccumulator groupedAccumulator = bigIntAgg.bind(Ints.asList(new int[] {}), Optional.empty())
                .createGroupedAccumulator();
        BlockBuilder blockBuilder = groupedAccumulator.getFinalType().createBlockBuilder(null, 1000);

        groupedAccumulator.evaluateFinal(0, blockBuilder);
        assertTrue(blockBuilder.isNull(0));
    }

    @Test
    public void testWithMultiplePages()
    {
        InternalAggregationFunction varcharAgg = getAggregation(VARCHAR);

        AggregationTestInputBuilder testInputBuilder = new AggregationTestInputBuilder(
                new Block[] {
                        createStringsBlock("hello", "world", "hello2", "world2", "hello3", "world3", "goodbye")},
                varcharAgg);
        AggregationTestOutput testOutput = new AggregationTestOutput(ImmutableList.of("hello", "world", "hello2", "world2", "hello3", "world3", "goodbye"));
        AggregationTestInput testInput = testInputBuilder.build();

        testInput.runPagesOnAccumulatorWithAssertion(0L, testInput.createGroupedAccumulator(), testOutput);
    }

    @Test
    public void testMultipleGroupsWithMultiplePages()
    {
        InternalAggregationFunction varcharAgg = getAggregation(VARCHAR);

        Block block1 = createStringsBlock("a", "b", "c", "d", "e");
        Block block2 = createStringsBlock("f", "g", "h", "i", "j");
        AggregationTestOutput aggregationTestOutput1 = new AggregationTestOutput(ImmutableList.of("a", "b", "c", "d", "e"));
        AggregationTestInputBuilder testInputBuilder1 = new AggregationTestInputBuilder(
                new Block[] {block1},
                varcharAgg);
        AggregationTestInput test1 = testInputBuilder1.build();
        GroupedAccumulator groupedAccumulator = test1.createGroupedAccumulator();

        test1.runPagesOnAccumulatorWithAssertion(0L, groupedAccumulator, aggregationTestOutput1);

        AggregationTestOutput aggregationTestOutput2 = new AggregationTestOutput(ImmutableList.of("f", "g", "h", "i", "j"));
        AggregationTestInputBuilder testBuilder2 = new AggregationTestInputBuilder(
                new Block[] {block2},
                varcharAgg);
        AggregationTestInput test2 = testBuilder2.build();
        test2.runPagesOnAccumulatorWithAssertion(255L, groupedAccumulator, aggregationTestOutput2);
    }

    @Test
    public void testManyValues()
    {
        // Test many values so multiple BlockBuilders will be used to store group state.
        InternalAggregationFunction varcharAgg = getAggregation(VARCHAR);

        int numGroups = 50000;
        int arraySize = 30;
        Random random = new Random();
        GroupedAccumulator groupedAccumulator = createGroupedAccumulator(varcharAgg);

        for (int j = 0; j < numGroups; j++) {
            List<String> expectedValues = new ArrayList<>();
            List<String> valueList = new ArrayList<>();

            for (int i = 0; i < arraySize; i++) {
                String str = String.valueOf(random.nextInt());
                valueList.add(str);
                expectedValues.add(str);
            }

            Block block = createStringsBlock(valueList);
            AggregationTestInputBuilder testInputBuilder = new AggregationTestInputBuilder(
                    new Block[] {block},
                    varcharAgg);
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
