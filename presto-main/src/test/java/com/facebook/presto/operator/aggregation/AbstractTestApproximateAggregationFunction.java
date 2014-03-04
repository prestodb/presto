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
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertApproximateAggregation;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestApproximateAggregationFunction
        extends AbstractTestAggregationFunction
{
    protected abstract TupleInfo getTupleInfo();

    // values may contain nulls
    protected abstract Double getExpectedValue(List<Number> values);

    @Override
    public double getConfidence()
    {
        return 0.99;
    }

    @Override
    public Block getSequenceBlock(int start, int length)
    {
        BlockBuilder blockBuilder = new BlockBuilder(getTupleInfo());
        for (int i = start; i < start + length; i++) {
            if (getTupleInfo() == SINGLE_LONG) {
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
            // Insert twice since we use a sample weight of two
            if (getTupleInfo() == SINGLE_LONG) {
                values.add((long) start + i);
                values.add((long) start + i);
            }
            else {
                values.add((double) start + i);
                values.add((double) start + i);
            }
        }

        return getExpectedValue(values);
    }

    @Override
    public Double getExpectedValueIncludingNulls(int start, int length, int lengthIncludingNulls)
    {
        List<Number> values = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            // Insert twice since we use a sample weight of two
            if (getTupleInfo() == SINGLE_LONG) {
                values.add((long) start + i);
                values.add((long) start + i);
            }
            else {
                values.add((double) start + i);
                values.add((double) start + i);
            }
        }
        for (int i = length; i < lengthIncludingNulls; i++) {
            // Insert twice since we use a sample weight of two
            values.add(null);
            values.add(null);
        }

        return getExpectedValue(values);
    }

    @Test
    public void testCorrectnessOnGaussianData()
            throws Exception
    {
        int originalDataSize = 100;
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
        int originalDataSize = 100;
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
        double sampleRatio = 0.1;

        for (int i = 0; i < numberOfRuns; i++) {
            //Compute Sampled Value using sampledList (numberOfRuns times)
            Iterable<Number> sampledList = Iterables.limit(shuffle(inputList, i), (int) (inputList.size() * sampleRatio));
            // Duplicate every element in the list, since we're going to use a sample weight of two
            double actual = getExpectedValue(ImmutableList.<Number>builder().addAll(sampledList).addAll(sampledList).build());

            BlockBuilder builder = new BlockBuilder(getTupleInfo());
            for (Number sample : sampledList) {
                if (getTupleInfo() == SINGLE_LONG) {
                    builder.append(sample.longValue());
                }
                else if (getTupleInfo() == SINGLE_DOUBLE) {
                    builder.append(sample.doubleValue());
                }
                else {
                    throw new AssertionError("Can only handle longs and doubles");
                }
            }
            Page page = new Page(builder.build());
            page = OperatorAssertion.appendSampleWeight(ImmutableList.of(page), 2).get(0);
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

        assertTrue(inRange >= getConfidence() * numberOfRuns);
    }

    @Override
    protected void testAggregation(Object expectedValue, Block block)
    {
        Page page = OperatorAssertion.appendSampleWeight(ImmutableList.of(new Page(block)), 2).get(0);
        assertApproximateAggregation(getFunction(), page.getChannelCount() - 1, getConfidence(), (Double) expectedValue, page);
    }

    private static List<Number> shuffle(Iterable<Number> iterable, long seed)
    {
        List<Number> list = Lists.newArrayList(iterable);
        Collections.shuffle(list, new Random(seed));
        return list;
    }
}
