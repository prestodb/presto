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
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.AggregationOperator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.tree.Input;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.*;

import static com.facebook.presto.operator.AggregationFunctionDefinition.aggregation;
import static com.facebook.presto.operator.AggregationOperator.createAggregator;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestApproximateAggregationFunction
        extends AbstractTestAggregationFunction
{

    public <T> Block getSequenceBlock(Iterable<T> list)
    {
        BlockBuilder blockBuilder;

        if (Iterables.get(list, 0) instanceof Long) {
            blockBuilder = new BlockBuilder(SINGLE_LONG);
        } else if (Iterables.get(list, 0) instanceof Double) {
            blockBuilder = new BlockBuilder(SINGLE_DOUBLE);
        } else {
            blockBuilder = new BlockBuilder(SINGLE_VARBINARY);
        }

        for (Object i : list) {
            if (i instanceof Long) {
                blockBuilder.append((Long) i);
            } else if (i instanceof Double) {
                blockBuilder.append((Double) i);
            }
        }

        return blockBuilder.build();
    }

    /**
     * This method tests the correctness of the closed-form error estimates for
     * average function on given data. We first calculate the true average: actualAvg
     * and then create 1,000 samples from original data (samplingRatio = 0.1) and compute
     * the approxAvg (with error bars) on each. The test passes if at least 99% of all
     * runs contain the true answer within the range [ApproxAvg - error, ApproxAvg + error]
     * @param inputList
     * @throws Exception
     */
    public void testCorrectnessOfErrorFunction(List<Number> inputList)
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

            BlockCursor cursor = createCompositeTupleBlock(getSequenceBlock(sampledList), 0).cursor();
            AggregationOperator.Aggregator function = createAggregator(aggregation(getFunction(),
                    new Input(0, 0)), AggregationNode.Step.SINGLE);

            for (int j = 0; j < (int) (inputList.size() * sampleRatio); j++) {
                assertTrue(cursor.advanceNextPosition());
                function.addValue(cursor);
            }

            String approxValue = BlockAssertions.toValues(function.getResult()).get(0).get(0).toString();
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
