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

import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.discreteentropy.DiscreteEntropyStateStrategy.JACKNIFE_METHOD_NAME;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;

public class TestBooleanExplicitJacknifeAggregation
        extends AbstractTestAggregationFunction
{
    private static final String FUNCTION_NAME = "discrete_entropy";

    private InternalAggregationFunction entropyFunction;

    @BeforeClass
    public void setUp()
    {
        FunctionManager functionManager = MetadataManager.createTestMetadataManager().getFunctionManager();
        entropyFunction = functionManager.getAggregateFunctionImplementation(
                functionManager.lookupFunction(TestBooleanExplicitJacknifeAggregation.FUNCTION_NAME, fromTypes(BOOLEAN, VARCHAR)));
    }

    @Test
    public void testEntropyOfASingle()
    {
        assertAggregation(entropyFunction,
                0.0,
                createBooleansBlock(Boolean.FALSE),
                createStringsBlock(JACKNIFE_METHOD_NAME));
    }

    @Test
    public void testEntropyOfTwoDistinct()
    {
        assertAggregation(entropyFunction,
                2.0,
                createBooleansBlock(Boolean.FALSE, Boolean.TRUE),
                createStringsBlock(JACKNIFE_METHOD_NAME, JACKNIFE_METHOD_NAME));

        assertAggregation(entropyFunction,
                2.0,
                createBooleansBlock(null, Boolean.FALSE, null, Boolean.TRUE),
                createStringsBlock(null, JACKNIFE_METHOD_NAME, JACKNIFE_METHOD_NAME, JACKNIFE_METHOD_NAME));
    }

    @Test
    public void testEntropyOfOnlyNulls()
    {
        assertAggregation(entropyFunction,
                0.0,
                createBooleansBlock(null, null, null),
                createStringsBlock(JACKNIFE_METHOD_NAME, JACKNIFE_METHOD_NAME, null));
    }

    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder samples = BOOLEAN.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            BOOLEAN.writeBoolean(samples, Math.abs(i) % 2 == 0);
        }
        return new Block[] {
                samples.build(),
                createRLEBlock(JACKNIFE_METHOD_NAME, length),
        };
    }

    @Override
    public Number getExpectedValue(int start, int length)
    {
        int[] counts = {0, 0};
        for (int i = start; i < start + length; i++) {
            ++counts[Math.abs(i) % 2];
        }
        double entropy = length * EntropyCalculations.calculateEntropy(counts);
        for (int j = start; j < start + length; j++) {
            int[] holdouts = {0, 0};
            for (int i = start; i < start + length; i++) {
                if (j != i) {
                    ++holdouts[Math.abs(i) % 2];
                }
            }
            entropy -= (length - 1) * EntropyCalculations.calculateEntropy(holdouts) / length;
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
        return ImmutableList.of(StandardTypes.BOOLEAN, StandardTypes.VARCHAR);
    }
}
