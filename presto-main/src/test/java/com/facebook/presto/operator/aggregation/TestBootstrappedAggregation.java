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

import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.testng.annotations.Test;

import java.util.Random;

import static com.facebook.presto.operator.aggregation.AggregationTestUtils.approximateAggregationWithinErrorBound;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertApproximateAggregation;
import static com.facebook.presto.operator.aggregation.LongSumAggregation.LONG_SUM;
import static com.facebook.presto.serde.TestingBlockEncodingManager.createTestingBlockEncodingManager;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertTrue;

public class TestBootstrappedAggregation
{
    @Test
    public void testSum()
            throws Exception
    {
        int sum = 1_000;
        PageBuilder builder = new PageBuilder(ImmutableList.of(BIGINT, BIGINT));
        Random rand = new Random(0);
        for (int i = 0; i < sum; i++) {
            if (rand.nextDouble() < 0.5) {
                builder.getBlockBuilder(0).append(1);
                builder.getBlockBuilder(1).append(2);
            }
        }

        AggregationFunction function = new DeterministicBootstrappedAggregation(createTestingBlockEncodingManager(), LONG_SUM);

        assertApproximateAggregation(function, 1, 0.99, (double) sum, builder.build());
    }

    @Test
    public void testErrorBound()
            throws Exception
    {
        int trials = 20;
        BinomialDistribution binomial = new BinomialDistribution(trials, 0.5);

        int successes = 0;
        Random rand = new Random(0);
        for (int i = 0; i < trials; i++) {
            int sum = 1_000;
            PageBuilder builder = new PageBuilder(ImmutableList.of(BIGINT, BIGINT));
            for (int j = 0; j < sum; j++) {
                if (rand.nextDouble() < 0.5) {
                    builder.getBlockBuilder(0).append(1);
                    builder.getBlockBuilder(1).append(2);
                }
            }

            AggregationFunction function = new DeterministicBootstrappedAggregation(createTestingBlockEncodingManager(), LONG_SUM);

            successes += approximateAggregationWithinErrorBound(function, 1, 0.5, (double) sum, builder.build()) ? 1 : 0;
        }

        // Since we used a confidence of 0.5, successes should have a binomial distribution B(n=20, p=0.5)
        assertTrue(binomial.inverseCumulativeProbability(0.01) < successes && successes < binomial.inverseCumulativeProbability(0.99));
    }

    private static class DeterministicBootstrappedAggregation
            extends BootstrappedAggregation
    {
        public DeterministicBootstrappedAggregation(BlockEncodingSerde blockEncodingManager, AggregationFunction function)
        {
            super(blockEncodingManager, function);
        }

        @Override
        public Accumulator createAggregation(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int... argumentChannels)
        {
            return createDeterministicAggregation(maskChannel, sampleWeightChannel.get(), confidence, 0, argumentChannels);
        }

        @Override
        public Accumulator createIntermediateAggregation(double confidence)
        {
            return createDeterministicIntermediateAggregation(confidence, 0);
        }

        @Override
        public GroupedAccumulator createGroupedAggregation(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int... argumentChannels)
        {
            return createDeterministicGroupedAggregation(maskChannel, sampleWeightChannel.get(), confidence, 0, argumentChannels);
        }

        @Override
        public GroupedAccumulator createGroupedIntermediateAggregation(double confidence)
        {
            return createDeterministicGroupedIntermediateAggregation(confidence, 0);
        }
    }
}
