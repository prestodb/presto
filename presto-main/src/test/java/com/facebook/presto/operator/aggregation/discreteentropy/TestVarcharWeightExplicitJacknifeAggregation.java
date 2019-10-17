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
package com.facebook.presto.operator.aggregation.discreteentropy;

import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.aggregation.AbstractTestAggregationFunction;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.discreteentropy.DiscreteEntropyStateStrategy.JACKNIFE_METHOD_NAME;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;

public class TestVarcharWeightExplicitJacknifeAggregation
        extends AbstractTestAggregationFunction
{
    private static final String FUNCTION_NAME = "discrete_entropy";

    private InternalAggregationFunction entropyFunction;

    @BeforeClass
    public void setUp()
    {
        FunctionManager functionManager = MetadataManager.createTestMetadataManager().getFunctionManager();
        entropyFunction = functionManager.getAggregateFunctionImplementation(
                functionManager.lookupFunction(TestVarcharWeightExplicitJacknifeAggregation.FUNCTION_NAME, fromTypes(VARCHAR, DOUBLE, VARCHAR)));
    }

    @Test
    public void testEntropyOfASingle()
    {
        assertAggregation(entropyFunction,
                0.0,
                createStringsBlock("false"),
                createDoublesBlock(10.0),
                createStringsBlock(JACKNIFE_METHOD_NAME));
    }

    @Test
    public void testEntropyOfTwoDistinct()
    {
        assertAggregation(entropyFunction,
                2.0,
                createStringsBlock("false", "true"),
                createDoublesBlock(10.0, 10.0),
                createStringsBlock(JACKNIFE_METHOD_NAME, JACKNIFE_METHOD_NAME));

        assertAggregation(entropyFunction,
                2.0,
                createStringsBlock("false", "true", "true"),
                createDoublesBlock(10.0, 10.0, null),
                createStringsBlock(JACKNIFE_METHOD_NAME, JACKNIFE_METHOD_NAME, null));
    }

    @Test
    public void testEntropyOfOnlyNulls()
    {
        assertAggregation(entropyFunction,
                0.0,
                createStringsBlock(null, null, null),
                createDoublesBlock(20.0, 10.0, null),
                createStringsBlock(JACKNIFE_METHOD_NAME, JACKNIFE_METHOD_NAME, null));
    }

    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder samples = VARCHAR.createBlockBuilder(null, length);
        BlockBuilder weights = DOUBLE.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            double current = Math.abs(i) % 2;
            VARCHAR.writeString(samples, Integer.toString((int) current));
            DOUBLE.writeDouble(weights, current);
        }
        return new Block[] {
                samples.build(),
                weights.build(),
                createRLEBlock(JACKNIFE_METHOD_NAME, length),
        };
    }

    @Override
    public Number getExpectedValue(int start, int length)
    {
        int[] counts = {0, 0};
        double[] weights = {0.0, 0.0};
        for (int i = start; i < start + length; i++) {
            int current = Math.abs(i) % 2;
            ++counts[current];
            weights[current] += current;
        }
        double entropy = length * EntropyCalculations.calculateEntropy(weights, counts);
        for (int j = start; j < start + length; j++) {
            int[] holdoutCounts = {0, 0};
            double[] holdoutWeights = {0.0, 0.0};
            for (int i = start; i < start + length; i++) {
                if (j != i) {
                    int current = Math.abs(i) % 2;
                    ++holdoutCounts[current];
                    holdoutWeights[current] += current;
                }
            }
            entropy -= (length - 1) * EntropyCalculations.calculateEntropy(holdoutWeights, holdoutCounts) / length;
        }
        return entropy;
    }

    @Override
    protected String getFunctionName()
    {
        return FUNCTION_NAME;
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(StandardTypes.VARCHAR, StandardTypes.DOUBLE, StandardTypes.VARCHAR);
    }
}
