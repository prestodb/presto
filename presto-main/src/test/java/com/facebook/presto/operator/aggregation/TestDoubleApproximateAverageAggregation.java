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
import com.facebook.presto.block.BlockBuilder;

import static com.facebook.presto.operator.aggregation.ApproximateAverageAggregation.DOUBLE_APPROXIMATE_AVERAGE_AGGREGATION;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;

public class TestDoubleApproximateAverageAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    public Block getSequenceBlock(int start, int length)
    {
        BlockBuilder blockBuilder = new BlockBuilder(SINGLE_DOUBLE);
        for (int i = start; i < start + length; i++) {
            blockBuilder.append((double) i);
        }
        return blockBuilder.build();
    }

    @Override
    public AggregationFunction getFunction()
    {
        return DOUBLE_APPROXIMATE_AVERAGE_AGGREGATION;
    }

    @Override
    public String getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }

        double sum = 0;
        for (int i = start; i < start + length; i++) {
            sum += i;
        }

        double mean = sum / length;
        double m2 = 0.0;
        for (int i = start; i < start + length; i++) {
            m2 += (i - mean) * (i - mean);
        }

        double variance = m2 / length;

        StringBuilder sb = new StringBuilder();
        sb.append(mean);
        sb.append(" +/- ");
        sb.append((2.575 * Math.sqrt(variance / length)));

        return sb.toString();
    }

}
