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

import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;

public class TestIntAggregation
        extends AbstractTestAggregationFunction
{
    private static final String FUNCTION_NAME = "discrete_entropy";

    private InternalAggregationFunction entropyFunction;

    @BeforeClass
    public void setUp()
    {
        FunctionManager functionManager = MetadataManager.createTestMetadataManager().getFunctionManager();
        entropyFunction = functionManager.getAggregateFunctionImplementation(
                functionManager.lookupFunction(TestIntAggregation.FUNCTION_NAME, fromTypes(DOUBLE)));
    }

    @Test
    public void testEntropyOfASingle()
    {
        assertAggregation(entropyFunction,
                0.0,
                createLongsBlock(Long.valueOf(1)));
    }

    @Test
    public void testEntropyOfTwoDistinct()
    {
        assertAggregation(entropyFunction,
                1.0,
                createLongsBlock(1, 2));

        assertAggregation(entropyFunction,
                1.0,
                createLongsBlock(null, 1L, null, 2L));
    }

    @Test
    public void testEntropyOfSkewedTwoDistinct()
    {
        assertAggregation(entropyFunction,
                0.9182958340544894,
                createLongsBlock(1, 1, 2));

        assertAggregation(entropyFunction,
                0.9182958340544894,
                createLongsBlock(null, 1L, null, 1L, 2L));
    }

    @Test
    public void testEntropyOfOnlyNulls()
    {
        assertAggregation(entropyFunction,
                0.0,
                createLongsBlock(null, null));
    }

    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder samples = BIGINT.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            BIGINT.writeLong(samples, Math.abs(i) % 2);
        }
        return new Block[] {samples.build()};
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
        return ImmutableList.of(StandardTypes.BIGINT);
    }
}
