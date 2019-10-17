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

import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.discreteentropy.DiscreteEntropyStateStrategy.MLE_METHOD_NAME;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;

public class TestVarcharExplicitMleAggregation
        extends AbstractTestAggregationFunction
{
    private static final String FUNCTION_NAME = "discrete_entropy";

    private InternalAggregationFunction entropyFunction;

    @BeforeClass
    public void setUp()
    {
        FunctionManager functionManager = MetadataManager.createTestMetadataManager().getFunctionManager();
        entropyFunction = functionManager.getAggregateFunctionImplementation(
                functionManager.lookupFunction(TestVarcharExplicitMleAggregation.FUNCTION_NAME, fromTypes(VARCHAR, VARCHAR)));
    }

    @Test
    public void testEntropyOfASingle()
    {
        assertAggregation(entropyFunction,
                0.0,
                createStringsBlock(new String("false")),
                createStringsBlock(MLE_METHOD_NAME));
    }

    @Test
    public void testEntropyOfTwoDistinct()
    {
        assertAggregation(entropyFunction,
                1.0,
                createStringsBlock("false", "true"),
                createStringsBlock(MLE_METHOD_NAME, MLE_METHOD_NAME));

        assertAggregation(entropyFunction,
                1.0,
                createStringsBlock("false", "true", "true"),
                createStringsBlock(MLE_METHOD_NAME, MLE_METHOD_NAME, null));
    }

    @Test
    public void testEntropyOfSkewedTwoDistinct()
    {
        assertAggregation(entropyFunction,
                0.9182958340544894,
                createStringsBlock("false", "false", "true"),
                createStringsBlock(MLE_METHOD_NAME, MLE_METHOD_NAME, MLE_METHOD_NAME));

        assertAggregation(entropyFunction,
                0.9182958340544894,
                createStringsBlock("false", "false", "true", "true"),
                createStringsBlock(MLE_METHOD_NAME, MLE_METHOD_NAME, MLE_METHOD_NAME, null));
    }

    @Test
    public void testEntropyOfOnlyNulls()
    {
        assertAggregation(entropyFunction,
                0.0,
                createStringsBlock(null, null, null),
                createStringsBlock(MLE_METHOD_NAME, MLE_METHOD_NAME, null));
    }

    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder samples = VARCHAR.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            VARCHAR.writeString(samples, Integer.toString(Math.abs(i) % 2));
        }
        return new Block[] {
                samples.build(),
                createRLEBlock(MLE_METHOD_NAME, length),
        };
    }

    @Override
    public Number getExpectedValue(int start, int length)
    {
        int[] counts = {0, 0};
        for (int i = start; i < start + length; i++) {
            ++counts[Math.abs(i) % 2];
        }
        return EntropyCalculations.calculateEntropy(counts);
    }

    @Override
    protected String getFunctionName()
    {
        return FUNCTION_NAME;
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(StandardTypes.VARCHAR, StandardTypes.VARCHAR);
    }
}
