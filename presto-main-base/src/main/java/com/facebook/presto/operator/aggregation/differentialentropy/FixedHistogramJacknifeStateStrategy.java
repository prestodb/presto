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

import com.facebook.presto.operator.aggregation.fixedhistogram.FixedDoubleBreakdownHistogram;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.util.Map;

import static com.facebook.presto.operator.aggregation.differentialentropy.EntropyCalculations.calculateEntropyFromHistogramAggregates;
import static com.facebook.presto.operator.aggregation.differentialentropy.FixedHistogramStateStrategyUtils.getXLogX;
import static com.google.common.collect.Streams.stream;
import static java.lang.Math.toIntExact;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summingDouble;

/**
 * Calculates sample entropy using jacknife estimates on a fixed histogram.
 * See http://cs.brown.edu/~pvaliant/unseen_nips.pdf.
 */
public class FixedHistogramJacknifeStateStrategy
        implements DifferentialEntropyStateStrategy
{
    private final FixedDoubleBreakdownHistogram histogram;

    public FixedHistogramJacknifeStateStrategy(long bucketCount, double min, double max)
    {
        FixedHistogramStateStrategyUtils.validateParameters(
                bucketCount,
                min,
                max);

        histogram = new FixedDoubleBreakdownHistogram(toIntExact(bucketCount), min, max);
    }

    private FixedHistogramJacknifeStateStrategy(FixedDoubleBreakdownHistogram histogram)
    {
        this.histogram = histogram;
    }

    private FixedHistogramJacknifeStateStrategy(FixedHistogramJacknifeStateStrategy other)
    {
        histogram = other.histogram.clone();
    }

    @Override
    public void validateParameters(long bucketCount, double sample, double weight, double min, double max)
    {
        FixedHistogramStateStrategyUtils.validateParameters(
                histogram.getBucketCount(),
                histogram.getMin(),
                histogram.getMax(),
                bucketCount,
                sample,
                weight,
                min,
                max);
    }

    @Override
    public void mergeWith(DifferentialEntropyStateStrategy other)
    {
        histogram.mergeWith(((FixedHistogramJacknifeStateStrategy) other).histogram);
    }

    @Override
    public void add(double value, double weight)
    {
        histogram.add(value, weight);
    }

    @Override
    public double getTotalPopulationWeight()
    {
        return stream(histogram.iterator())
                .mapToDouble(FixedDoubleBreakdownHistogram.Bucket::getWeight)
                .sum();
    }

    @Override
    public double calculateEntropy()
    {
        Map<Double, Double> bucketWeights = stream(histogram.iterator()).collect(
                groupingBy(
                        FixedDoubleBreakdownHistogram.Bucket::getLeft,
                        summingDouble(e -> e.getCount() * e.getWeight())));
        double sumWeight = bucketWeights.values().stream().mapToDouble(Double::doubleValue).sum();
        if (sumWeight == 0.0) {
            return Double.NaN;
        }
        long n = stream(histogram.iterator())
                .mapToLong(FixedDoubleBreakdownHistogram.Bucket::getCount)
                .sum();
        double sumWeightLogWeight =
                bucketWeights.values().stream().mapToDouble(w -> w == 0.0 ? 0.0 : w * Math.log(w)).sum();

        double entropy = n * calculateEntropyFromHistogramAggregates(histogram.getWidth(), sumWeight, sumWeightLogWeight);
        for (FixedDoubleBreakdownHistogram.Bucket bucketWeight : histogram) {
            double weight = bucketWeights.get(bucketWeight.getLeft());
            if (weight > 0.0) {
                entropy -= getHoldOutEntropy(
                        n,
                        bucketWeight.getRight() - bucketWeight.getLeft(),
                        sumWeight,
                        sumWeightLogWeight,
                        weight,
                        bucketWeight.getWeight(),
                        bucketWeight.getCount());
            }
        }
        return entropy;
    }

    private static double getHoldOutEntropy(
            long n,
            double width,
            double sumW,
            double sumWeightLogWeight,
            double bucketWeight,
            double entryWeight,
            long entryMultiplicity)
    {
        double holdoutBucketWeight = Math.max(bucketWeight - entryWeight, 0);
        double holdoutSumWeight =
                sumW - bucketWeight + holdoutBucketWeight;
        double holdoutSumWeightLogWeight =
                sumWeightLogWeight - getXLogX(bucketWeight) + getXLogX(holdoutBucketWeight);
        double holdoutEntropy = entryMultiplicity * (n - 1) *
                calculateEntropyFromHistogramAggregates(width, holdoutSumWeight, holdoutSumWeightLogWeight) /
                n;
        return holdoutEntropy;
    }

    @Override
    public long getEstimatedSize()
    {
        return histogram.estimatedInMemorySize();
    }

    @Override
    public int getRequiredBytesForSpecificSerialization()
    {
        return histogram.getRequiredBytesForSerialization();
    }

    public static FixedHistogramJacknifeStateStrategy deserialize(SliceInput input)
    {
        FixedDoubleBreakdownHistogram histogram = FixedDoubleBreakdownHistogram.deserialize(input);
        return new FixedHistogramJacknifeStateStrategy(histogram);
    }

    @Override
    public void serialize(SliceOutput out)
    {
        histogram.serialize(out);
    }

    @Override
    public DifferentialEntropyStateStrategy clone()
    {
        return new FixedHistogramJacknifeStateStrategy(this);
    }

    @Override
    public DifferentialEntropyStateStrategy cloneEmpty()
    {
        return new FixedHistogramJacknifeStateStrategy(histogram.getBucketCount(), histogram.getMin(), histogram.getMax());
    }
}
