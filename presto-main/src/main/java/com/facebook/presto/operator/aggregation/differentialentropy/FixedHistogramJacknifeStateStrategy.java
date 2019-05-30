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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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

    protected FixedHistogramJacknifeStateStrategy(FixedHistogramJacknifeStateStrategy other)
    {
        histogram = other.getBreakdownHistogram().clone();
    }

    public FixedHistogramJacknifeStateStrategy(SliceInput input)
    {
        histogram = new FixedDoubleBreakdownHistogram(input);
    }

    @Override
    public void validateParams(
            long bucketCount,
            double sample,
            Double weight,
            Double min,
            Double max)
    {
        FixedHistogramStateStrategyUtils.validateParams(
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
        Map<Double, Double> bucketWeights = new HashMap<Double, Double>();
        Streams.stream(getBreakdownHistogram().iterator()).forEach(
                e -> {
                    bucketWeights.put(
                            e.left,
                            Streams.stream(e.breakdown.iterator()).mapToDouble(w -> w.weight * w.count).sum());
                });
        System.out.println("bucketWeights " + Arrays.toString(bucketWeights.entrySet().toArray()));
        double sumW =
                bucketWeights.values().stream().mapToDouble(w -> w).sum();
        if (sumW == 0.0) {
            return 0.0;
        }
        long n = Streams.stream(getBreakdownHistogram())
                .mapToLong(e -> Streams.stream(e.breakdown.iterator()).mapToLong(w -> w.count).sum())
                .sum();
        double sumWLogW =
                bucketWeights.values().stream().mapToDouble(w -> w == 0.0 ? 0.0 : w * Math.log(w)).sum();

        double entropy = n * calculateEntropy(histogram.getWidth(), sumW, sumWLogW);
        entropy -= Streams.stream(getBreakdownHistogram().iterator()).mapToDouble(
                e -> {
                    double bucketWeight = bucketWeights.get(e.left);
                    if (bucketWeight == 0.0) {
                        return 0.0;
                    }
                    return Streams.stream(e.breakdown.iterator()).mapToDouble(
                            bucketE -> FixedHistogramJacknifeStateStrategy.getHoldOutEntropy(
                                    n,
                                    histogram.getWidth(),
                                    sumW,
                                    sumWLogW,
                                    bucketWeight,
                                    bucketE.weight,
                                    bucketE.count))
                            .sum();
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
                sumWLogW - FixedHistogramStateStrategyUtils.getXLogX(bucketWeight) +
                        FixedHistogramStateStrategyUtils.getXLogX(holdoutBucketWeight);
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
