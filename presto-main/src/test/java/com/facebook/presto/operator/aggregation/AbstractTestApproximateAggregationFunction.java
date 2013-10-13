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

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.operator.OperatorAssertion;
import com.facebook.presto.operator.Page;
import com.facebook.presto.type.Type;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.facebook.presto.block.BlockBuilders.createBlockBuilder;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertApproximateAggregation;
import static com.facebook.presto.type.Types.BIGINT;
import static com.facebook.presto.type.Types.DOUBLE;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestApproximateAggregationFunction
        extends AbstractTestAggregationFunction
{
    private static final int WEIGHT = 10;
    protected abstract Type getType();

    // values may contain nulls
    protected abstract Double getExpectedValue(List<Number> values);

    @Override
    public double getConfidence()
    {
        return 0.5;
    }

    @Override
    public Block getSequenceBlock(int start, int length)
    {
        BlockBuilder blockBuilder = createBlockBuilder(getType());
        for (int i = start; i < start + length; i++) {
            if (getType() == BIGINT) {
                blockBuilder.append((long) i);
            }
            else {
                blockBuilder.append((double) i);
            }
        }
        return blockBuilder.build();
    }

    @Override
    public Double getExpectedValue(int start, int length)
    {
        List<Number> values = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            if (getType() == BIGINT) {
                for (int j = 0; j < WEIGHT; j++) {
                    values.add((long) start + i);
                }
            }
            else {
                for (int j = 0; j < WEIGHT; j++) {
                    values.add((double) start + i);
                }
            }
        }

        return getExpectedValue(values);
    }

    @Override
    public Double getExpectedValueIncludingNulls(int start, int length, int lengthIncludingNulls)
    {
        List<Number> values = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            if (getType() == BIGINT) {
                for (int j = 0; j < WEIGHT; j++) {
                    values.add((long) start + i);
                }
            }
            else {
                for (int j = 0; j < WEIGHT; j++) {
                    values.add((double) start + i);
                }
            }
        }
        for (int i = length; i < lengthIncludingNulls; i++) {
            for (int j = 0; j < WEIGHT; j++) {
                values.add(null);
            }
        }

        return getExpectedValue(values);
    }

    @Test
    public void testCorrectnessOnGaussianData()
            throws Exception
    {
        int originalDataSize = 10000;
        Random distribution = new Random(0);
        List<Number> list = new ArrayList<>();
        for (int i = 0; i < originalDataSize; i++) {
            list.add(distribution.nextGaussian() * 100);
        }

        testCorrectnessOfErrorFunction(list);
    }

    @Test
    public void testCorrectnessOnUniformData()
            throws Exception
    {
        int originalDataSize = 10000;
        Random distribution = new Random(0);
        List<Number> list = new ArrayList<>();
        for (int i = 0; i < originalDataSize; i++) {
            list.add(distribution.nextDouble() * 1000);
        }

        testCorrectnessOfErrorFunction(list);
    }

    private void testCorrectnessOfErrorFunction(List<Number> inputList)
            throws Exception
    {
        int inRange = 0;
        int numberOfRuns = 1000;
        double sampleRatio = 1 / (double) WEIGHT;
        double actual = getExpectedValue(inputList);
        Random rand = new Random(1);

        for (int i = 0; i < numberOfRuns; i++) {
            //Compute Sampled Value using sampledList (numberOfRuns times)
            ImmutableList.Builder<Number> sampledList = ImmutableList.builder();
            for (Number x : inputList) {
                if (rand.nextDouble() < sampleRatio) {
                    sampledList.add(x);
                }
            }

            BlockBuilder builder = createBlockBuilder(getType());
            for (Number sample : sampledList.build()) {
                if (getType() == BIGINT) {
                    builder.append(sample.longValue());
                }
                else if (getType() == DOUBLE) {
                    builder.append(sample.doubleValue());
                }
                else {
                    throw new AssertionError("Can only handle longs and doubles");
                }
            }
            Page page = new Page(builder.build());
            page = OperatorAssertion.appendSampleWeight(ImmutableList.of(page), WEIGHT).get(0);
            Accumulator accumulator = getFunction().createAggregation(Optional.<Integer>absent(), Optional.of(page.getChannelCount() - 1), getConfidence(), 0);

            accumulator.addInput(page);
            Block result = accumulator.evaluateFinal();

            String approxValue = BlockAssertions.toValues(result).get(0).toString();
            double approx = Double.parseDouble(approxValue.split(" ")[0]);
            double error = Double.parseDouble(approxValue.split(" ")[2]);

            //Check if actual answer lies within [approxAnswer - error, approxAnswer + error]
            if (Math.abs(approx - actual) <= error) {
                inRange++;
            }
        }

        BinomialDistribution binomial = new BinomialDistribution(numberOfRuns, getConfidence());
        int lowerBound = binomial.inverseCumulativeProbability(0.01);
        int upperBound = binomial.inverseCumulativeProbability(0.99);
        assertTrue(lowerBound < inRange && inRange < upperBound, String.format("%d out of %d passed. Expected [%d, %d]", inRange, numberOfRuns, lowerBound, upperBound));
    }

    @Override
    protected void testAggregation(Object expectedValue, Block block)
    {
        Page page = OperatorAssertion.appendSampleWeight(ImmutableList.of(new Page(block)), WEIGHT).get(0);
        assertApproximateAggregation(getFunction(), page.getChannelCount() - 1, getConfidence(), (Double) expectedValue, page);
    }
}
