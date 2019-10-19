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
package com.facebook.presto.operator.aggregation.differentialmutualinformationclassification;

import com.facebook.presto.operator.aggregation.AbstractTestAggregationFunction;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

abstract class AbstractTestFixedHistogramAggregation
        extends AbstractTestAggregationFunction
{
    protected static final int MAX_SAMPLES = 500;
    protected static final int LENGTH_FACTOR = 8;
    protected static final int TRUE_FACTOR = 4;
    private static final int NUM_BINS = 5;
    protected final String method;

    protected AbstractTestFixedHistogramAggregation(String method)
    {
        this.method = method;
    }

    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        int positionCount = LENGTH_FACTOR * length;
        BlockBuilder outcomes = BIGINT.createBlockBuilder(null, positionCount);
        BlockBuilder samples = DOUBLE.createBlockBuilder(null, positionCount);
        BlockBuilder weights = DOUBLE.createBlockBuilder(null, positionCount);
        for (int i = start; i < start + LENGTH_FACTOR * length; i++) {
            BIGINT.writeLong(outcomes, i % TRUE_FACTOR == 0 ? 0 : 1);
            DOUBLE.writeDouble(samples, Math.abs(i) % NUM_BINS);
            DOUBLE.writeDouble(weights, Math.abs(i) % 3 + 1);
        }

        return new Block[] {
                createRLEBlock(NUM_BINS, positionCount),
                outcomes.build(),
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
        return "normalized_differential_mutual_information_classification";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(
                StandardTypes.BIGINT,
                StandardTypes.BIGINT,
                StandardTypes.DOUBLE,
                StandardTypes.DOUBLE,
                StandardTypes.VARCHAR,
                StandardTypes.DOUBLE,
                StandardTypes.DOUBLE);
    }

    protected static void generateOutcomesSamplesAndWeights(int start, int length, List<Integer> outcomes, List<Double> samples, List<Double> weights)
    {
        for (int i = start; i < start + LENGTH_FACTOR * length; i++) {
            outcomes.add(i % TRUE_FACTOR == 0 ? 0 : 1);
            samples.add((double) (Math.abs(i) % NUM_BINS));
            weights.add((double) (Math.abs(i) % 3 + 1));
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
