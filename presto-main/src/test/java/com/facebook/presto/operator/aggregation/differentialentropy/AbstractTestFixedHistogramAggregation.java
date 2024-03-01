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
import com.facebook.presto.operator.aggregation.AbstractTestAggregationFunction;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;

abstract class AbstractTestFixedHistogramAggregation
        extends AbstractTestAggregationFunction
{
    private static final int NUM_BINS = 5;
    protected final String method;

    protected AbstractTestFixedHistogramAggregation(String method)
    {
        this.method = method;
    }

    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        int positionCount = 2 * length;
        BlockBuilder samples = DOUBLE.createBlockBuilder(null, positionCount);
        BlockBuilder weights = DOUBLE.createBlockBuilder(null, positionCount);
        for (int weight = 1; weight < 3; weight++) {
            for (int i = start; i < start + length; i++) {
                int bin = Math.max(Math.min(i, NUM_BINS - 1), 0);
                DOUBLE.writeDouble(samples, bin);
                DOUBLE.writeDouble(weights, weight);
            }
        }

        return new Block[] {
                createRLEBlock(NUM_BINS, positionCount),
                samples.build(),
                weights.build(),
                createRLEBlock(this.method, positionCount),
                createRLEBlock(0.0, positionCount),
                createRLEBlock((double) NUM_BINS, positionCount)
        };
    }

    @Override
    protected String getFunctionName()
    {
        return "differential_entropy";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(
                StandardTypes.INTEGER,
                StandardTypes.DOUBLE,
                StandardTypes.DOUBLE,
                StandardTypes.VARCHAR,
                StandardTypes.DOUBLE,
                StandardTypes.DOUBLE);
    }

    protected static void generateSamplesAndWeights(int start, int length, List<Double> samples, List<Double> weights)
    {
        for (int weight = 1; weight < 3; weight++) {
            for (int i = start; i < start + length; i++) {
                int bin = Math.max(Math.min(i, NUM_BINS - 1), 0);
                samples.add(Double.valueOf(bin));
                weights.add(Double.valueOf(weight));
            }
        }
    }

    protected static double calculateEntropy(List<Double> samples, List<Double> weights)
    {
        double totalWeight = weights.stream().mapToDouble(weight -> weight).sum();
        if (totalWeight == 0.0) {
            return Double.NaN;
        }

        Map<Double, Double> bucketWeights = new HashMap<>();
        for (int i = 0; i < samples.size(); i++) {
            double sample = samples.get(i);
            double weight = weights.get(i);
            bucketWeights.put(sample, bucketWeights.getOrDefault(sample, 0.0) + weight);
        }

        double entropy = bucketWeights.values().stream()
                .mapToDouble(weight -> weight == 0.0 ? 0.0 : weight / totalWeight * Math.log(totalWeight / weight))
                .sum();
        return entropy / Math.log(2);
    }
}
