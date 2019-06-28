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
import com.google.common.collect.Streams;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.operator.aggregation.differentialentropy.FixedHistogramStateStrategyUtils.getXLogX;
import static com.facebook.presto.operator.aggregation.differentialentropy.FixedHistogramStateStrategyUtils.throwIfIllegalParameters;

/*
Calculates sample entropy using jacknife estimates on a fixed histogram.
See http://cs.brown.edu/~pvaliant/unseen_nips.pdf.
 */
public class FixedHistogramJacknifeStateStrategy
        implements StateStrategy
{
    protected final FixedDoubleBreakdownHistogram histogram;

    public FixedHistogramJacknifeStateStrategy(long bucketCount, double min, double max)
    {
        histogram = new FixedDoubleBreakdownHistogram((int) bucketCount, min, max);
    }

    protected FixedHistogramJacknifeStateStrategy(FixedHistogramJacknifeStateStrategy other) {
        histogram = other.getBreakdownHistogram().clone();
    }

    public FixedHistogramJacknifeStateStrategy(SliceInput input)
    {
        histogram = new FixedDoubleBreakdownHistogram(input);
    }

    @Override
    public void validateParameters(
            long bucketCount,
            double sample,
            double weight,
            double min,
            double max)
    {
        throwIfIllegalParameters(
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
    public void validateParameters(
            long bucketCount,
            double sample,
            double weight)
    {
        throwIfIllegalParameters(
                histogram.getBucketCount(),
                histogram.getMin(),
                histogram.getMax(),
                bucketCount,
                sample,
                weight);
    }

    @Override
    public void mergeWith(StateStrategy other)
    {
        getBreakdownHistogram()
                .mergeWith(((FixedHistogramJacknifeStateStrategy) other).getBreakdownHistogram());
    }

    @Override
    public void add(double value, double weight)
    {
        getBreakdownHistogram().add(value, weight);
    }

    @Override
    public double calculateEntropy()
    {
        Map<Double, Double> bucketWeights = Streams.stream(getBreakdownHistogram().iterator()).collect(
                Collectors.groupingBy(
                        FixedDoubleBreakdownHistogram.BucketWeight::getLeft,
                        Collectors.summingDouble(e -> e.getCount() * e.getWeight())));
        double sumW = bucketWeights.values().stream().mapToDouble(Double::doubleValue).sum();
        if (sumW == 0.0) {
            return 0.0;
        }
        long n = Streams.stream(getBreakdownHistogram().iterator())
                .mapToLong(FixedDoubleBreakdownHistogram.BucketWeight::getCount)
                .sum();
        double sumWLogW =
                bucketWeights.values().stream().mapToDouble(w -> w == 0.0 ? 0.0 : w * Math.log(w)).sum();

        double entropy = n * calculateEntropy(histogram.getWidth(), sumW, sumWLogW);
        entropy -= Streams.stream(getBreakdownHistogram().iterator()).mapToDouble(
                e -> {
                    double bucketWeight = bucketWeights.get(e.getLeft());
                    if (bucketWeight == 0.0) {
                        return 0.0;
                    }
                    return getHoldOutEntropy(
                        n,
                        e.getRight() - e.getLeft(),
                        sumW,
                        sumWLogW,
                        bucketWeight,
                        e.getWeight(),
                        e.getCount());
                })
                .sum();
        return entropy;
    }

    private static double getHoldOutEntropy(
            long n,
            double width,
            double sumW,
            double sumWLogW,
            double bucketWeight,
            double entryWeight,
            long entryMultiplicity)
    {
        double holdoutBucketWeight = Math.max(bucketWeight - entryWeight, 0);
        double holdoutSumW =
                sumW - bucketWeight + holdoutBucketWeight;
        double holdoutSumWLogW =
                sumWLogW - getXLogX(bucketWeight) + getXLogX(holdoutBucketWeight);
        double holdoutEntropy = entryMultiplicity * (n - 1) *
                calculateEntropy(width, holdoutSumW, holdoutSumWLogW) /
                n;
        return holdoutEntropy;
    }

    private static double calculateEntropy(double width, double sumW, double sumWLogW)
    {
        if (sumW == 0.0) {
            return 0.0;
        }
        double entropy = Math.max(
                (Math.log(width * sumW) - sumWLogW / sumW) / Math.log(2.0),
                0.0);
        return entropy;
    }

    @Override
    public long estimatedInMemorySize()
    {
        return getBreakdownHistogram().estimatedInMemorySize();
    }

    @Override
    public int getRequiredBytesForSerialization()
    {
        return getBreakdownHistogram().getRequiredBytesForSerialization();
    }

    @Override
    public void serialize(SliceOutput out)
    {
        getBreakdownHistogram().serialize(out);
    }

    public FixedDoubleBreakdownHistogram getBreakdownHistogram()
    {
        return histogram;
    }

    @Override
    public StateStrategy clone()
    {
        return new FixedHistogramJacknifeStateStrategy(this);
    }
}
