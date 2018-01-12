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

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.aggregation.groupByAggregations.AggregationTestInput;
import com.facebook.presto.operator.aggregation.groupByAggregations.AggregationTestInputBuilder;
import com.facebook.presto.operator.aggregation.groupByAggregations.AggregationTestOutput;
import com.facebook.presto.spi.block.Block;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

public class TestAreaUnderRocCurveAggregation
{
    private static final MetadataManager METADATA = MetadataManager.createTestMetadataManager();

    @Test
    public void testSingleClass()
    {
        testRunner(null, createBooleansBlock(true, true, true), createDoublesBlock(0.3, 0.1, 0.2));
        testRunner(null, createBooleansBlock(false, false, false), createDoublesBlock(0.3, 0.1, 0.2));
    }

    @Test
    public void testTrivial()
    {
        // max
        testRunner(1.0, createBooleansBlock(true, true, false), createDoublesBlock(0.3, 0.2, 0.1));

        // min
        testRunner(0.0, createBooleansBlock(false, false, true), createDoublesBlock(0.3, 0.2, 0.1));

        // mid
        testRunner(0.5, createBooleansBlock(true, false, true), createDoublesBlock(0.3, 0.2, 0.1));
    }

    @Test
    public void testAuc()
    {
        // In Python scikit-learn machine learning library:
        // >>> from sklearn.metrics import roc_auc_score
        // >>> roc_auc_score([0, 1, 0, 1, 1], [0.5, 0.3, 0.2, 0.8, 0.7])
        // 0.83333333333333326
        testRunner(0.8333333333, createBooleansBlock(false, true, false, true, true), createDoublesBlock(0.5, 0.3, 0.2, 0.8, 0.7));
    }

    private static void testRunner(Double expectedValue, Block labels, Block scores)
    {
        InternalAggregationFunction function = getInternalAggregationFunction();

        AggregationTestInputBuilder testInputBuilder = new AggregationTestInputBuilder(
                new Block[] {labels, scores},
                function);
        AggregationTestInput test = testInputBuilder.build();

        AggregationTestOutput expectedOutput = new AggregationTestOutput(expectedValue);

        test.runPagesOnAccumulatorWithAssertion(0L, test.createGroupedAccumulator(), expectedOutput);
    }

    @Test
    public void testAucOnMultipleGroups()
    {
        InternalAggregationFunction function = getInternalAggregationFunction();

        Block labels1 = createBooleansBlock(false, true);
        Block scores1 = createDoublesBlock(0.5, 0.3); // sort by score: false, true
        AggregationTestInputBuilder testInputBuilder1 = new AggregationTestInputBuilder(
                new Block[] {labels1, scores1},
                function);
        AggregationTestInput test1 = testInputBuilder1.build();
        AggregationTestOutput expectedOutput1 = new AggregationTestOutput(0.0);

        GroupedAccumulator groupedAccumulator = test1.createGroupedAccumulator();

        test1.runPagesOnAccumulatorWithAssertion(0L, groupedAccumulator, expectedOutput1);

        Block labels2 = createBooleansBlock(false, true, true);
        Block scores2 = createDoublesBlock(0.2, 0.8, 0.7); // sort by score: true, true, false
        AggregationTestInputBuilder testInputBuilder2 = new AggregationTestInputBuilder(
                new Block[] {labels2, scores2},
                function);
        AggregationTestInput test2 = testInputBuilder2.build();
        AggregationTestOutput expectedOutput2 = new AggregationTestOutput(1.0);

        test2.runPagesOnAccumulatorWithAssertion(255L, groupedAccumulator, expectedOutput2);
    }

    @Test
    public void testAucSingleState()
    {
        assertAggregation(getInternalAggregationFunction(),
                0.8333333333,
                createBooleansBlock(false, true, false, true, true),
                createDoublesBlock(0.5, 0.3, 0.2, 0.8, 0.7));
    }

    private static InternalAggregationFunction getInternalAggregationFunction()
    {
        return METADATA.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("area_under_roc_curve",
                        AGGREGATE,
                        DOUBLE.getTypeSignature(),
                        BOOLEAN.getTypeSignature(),
                        DOUBLE.getTypeSignature()));
    }
}
