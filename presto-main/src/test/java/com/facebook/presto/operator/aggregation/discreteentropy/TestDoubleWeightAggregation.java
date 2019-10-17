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
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;

public class TestDoubleWeightAggregation
        extends AbstractTestAggregationFunction
{
    private static final String FUNCTION_NAME = "discrete_entropy";

    private InternalAggregationFunction entropyFunction;

    @BeforeClass
    public void setUp()
    {
        FunctionManager functionManager = MetadataManager.createTestMetadataManager().getFunctionManager();
        entropyFunction = functionManager.getAggregateFunctionImplementation(
                functionManager.lookupFunction(TestDoubleWeightAggregation.FUNCTION_NAME, fromTypes(DOUBLE, DOUBLE)));
    }

    @Test
    public void testEntropyOfASingle()
    {
        assertAggregation(entropyFunction,
                0.0,
                createDoublesBlock(Double.valueOf(1.0)),
                createDoublesBlock(Double.valueOf(1.0)));
    }

    @Test
    public void testEntropyOfTwoDistinct()
    {
        assertAggregation(entropyFunction,
                1.0,
                createDoublesBlock(1.0, 2.0),
                createDoublesBlock(1.0, 1.0));

        assertAggregation(entropyFunction,
                1.0,
                createDoublesBlock(1.0, null, 2.0, null),
                createDoublesBlock(1.0, 1.0, 1.0, null));
    }

    @Test
    public void testEntropyOfSkewedTwoDistinct()
    {
        assertAggregation(entropyFunction,
                0.9182958340544894,
                createDoublesBlock(1.0, 2.0),
                createDoublesBlock(1.0, 2.0));

        assertAggregation(entropyFunction,
                0.9182958340544894,
                createDoublesBlock(1.0, 2.0, 2.0),
                createDoublesBlock(2.0, 2.0, 2.0));

        assertAggregation(entropyFunction,
                0.9182958340544894,
                createDoublesBlock(null, 1.0, 2.0, 2.0),
                createDoublesBlock(null, 2.0, 2.0, 2.0));
    }

    @Test
    public void testEntropyOfOnlyNulls()
    {
        assertAggregation(entropyFunction,
                0.0,
                createDoublesBlock(null, null),
                createDoublesBlock(null, null));
    }

    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder samples = DOUBLE.createBlockBuilder(null, length);
        BlockBuilder weights = DOUBLE.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            double current = Math.abs(i) % 2;
            DOUBLE.writeDouble(samples, current);
            DOUBLE.writeDouble(weights, current);
        }
        return new Block[] {
                samples.build(),
                weights.build()};
    }

    @Override
    public Number getExpectedValue(int start, int length)
    {
        double[] weights = {0.0, 0.0};
        for (int i = start; i < start + length; i++) {
            weights[Math.abs(i) % 2] += (double) (Math.abs(i) % 2);
        }
        return EntropyCalculations.calculateEntropy(weights);
    }

    @Override
    protected String getFunctionName()
    {
        return FUNCTION_NAME;
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(
                StandardTypes.DOUBLE,
                StandardTypes.DOUBLE);
    }
}
