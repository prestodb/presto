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
package com.facebook.presto.operator.aggregation.differentialentropy;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.aggregation;

public class TestWeightedReservoirAggregation
        extends AbstractTestReservoirAggregation
{
    @Test(
            expectedExceptions = PrestoException.class,
            expectedExceptionsMessageRegExp = "In differential_entropy UDF, max samples must be positive: -200")
    public void testInvalidMaxSamples()
    {
        aggregation(
                getFunction(),
                createLongsBlock(-200),
                createDoublesBlock(0.1),
                createDoublesBlock(0.2));
    }

    @Test(
            expectedExceptions = PrestoException.class,
            expectedExceptionsMessageRegExp = "In differential_entropy UDF, weight must be non-negative: -0.2")
    public void testNegativeWeight()
    {
        aggregation(
                getFunction(),
                createLongsBlock(200),
                createDoublesBlock(0.1),
                createDoublesBlock(-0.2));
    }

    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        int positionCount = 2 * length;
        BlockBuilder samples = DOUBLE.createBlockBuilder(null, positionCount);
        BlockBuilder weights = DOUBLE.createBlockBuilder(null, positionCount);
        for (int weight = 1; weight < 3; weight++) {
            for (int i = start; i < start + length; i++) {
                DOUBLE.writeDouble(samples, i);
                DOUBLE.writeDouble(weights, weight);
            }
        }

        return new Block[] {
                createRLEBlock(MAX_SAMPLES, positionCount),
                samples.build(),
                weights.build()
        };
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(StandardTypes.INTEGER, StandardTypes.DOUBLE, StandardTypes.DOUBLE);
    }
}
