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
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.testng.Assert.assertTrue;

public abstract class AbstractTestApproximateAggregationFunction
        extends AbstractTestAggregationFunction
{
    /**
     * This method tests the correctness of the closed-form error estimates for
     * average function on given data. We first calculate the true average: actualAvg
     * and then create 1,000 samples from original data (samplingRatio = 0.1) and compute
     * the approxAvg (with error bars) on each. The test passes if at least 99% of all
     * runs contain the true answer within the range [ApproxAvg - error, ApproxAvg + error]
     * @param inputList
     * @throws Exception
     */
    public void testCorrectnessOfErrorFunction(List<Number> inputList, TupleInfo type)
            throws Exception
    {
        //Compute Actual Value using list
        double actualSum = 0;
        for (Number value : inputList) {
            actualSum += value.doubleValue();
        }

        double actualAvg = actualSum / inputList.size();

        int inRange = 0;
        int numberOfRuns = 1000;
        double sampleRatio = 0.1;

        for (int i = 0; i < numberOfRuns; i++) {
            //Compute Sampled Value using sampledList (numberOfRuns times)
            Iterable<Number> sampledList = Iterables.limit(shuffle(inputList), (int) (inputList.size() * sampleRatio));

            BlockBuilder builder = new BlockBuilder(type);
            for (Number sample: sampledList) {
                if (sample instanceof Double) {
                    builder.append(sample.doubleValue());
                }
                else if (sample instanceof Long) {
                    builder.append(sample.longValue());
                }
                else {
                    throw new AssertionError("Can only handle longs and doubles");
                }
            }
            Accumulator accumulator = getFunction().createAggregation(Optional.<Integer>absent(), 0);

            accumulator.addInput(new Page(builder.build()));
            Block result = accumulator.evaluateFinal();

            String approxValue = BlockAssertions.toValues(result).get(0).toString();
            double approxAvg = Double.parseDouble(approxValue.split(" ")[0]);
            double error = Double.parseDouble(approxValue.split(" ")[2]);

            //Check if actual answer lies within [approxAnswer - error, approxAnswer + error]
            if ((approxAvg - error <= actualAvg) && (approxAvg + error >= actualAvg)) {
                inRange++;
            }
        }

        double confidence = 0.99; //i.e., a confidence of 99%
        assertTrue(inRange >= confidence * numberOfRuns);
    }

    protected static List<Number> shuffle(Iterable<Number> iterable)
    {
        List<Number> list = Lists.newArrayList(iterable);
        Collections.shuffle(list, new Random(1));
        return list;
    }
}
