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

import com.facebook.presto.operator.aggregation.reservoirsample.WeightedDoubleReservoirSample;
import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static com.facebook.presto.operator.aggregation.differentialentropy.FixedHistogramStateStrategyUtils.getXLogX;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public class WeightedReservoirSampleStateStrategy
        implements StateStrategy
{
    protected final WeightedDoubleReservoirSample reservoir;

    public WeightedReservoirSampleStateStrategy(long size)
    {
        reservoir = new WeightedDoubleReservoirSample((int) size);
    }

    protected WeightedReservoirSampleStateStrategy(WeightedReservoirSampleStateStrategy other)
    {
        reservoir = other.reservoir.clone();
    }

    public WeightedReservoirSampleStateStrategy(SliceInput input)
    {
        reservoir = new WeightedDoubleReservoirSample(input);
    }

    @Override
    public void validateParameters(
            long bucketCount,
            double sample,
            double weight,
            double min,
            double max)
    {
        throw new IllegalArgumentException("Unsupported for this type");
    }

    @Override
    public void validateParameters(
            long size,
            double sample,
            double weight)
    {
        if (weight < 0.0) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "Weight must be non-negative");
        }

        if (size != reservoir.getMaxSamples()) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "Inconsistent size");
        }
    }

    @Override
    public void mergeWith(StateStrategy other)
    {
        reservoir.mergeWith(((WeightedReservoirSampleStateStrategy) other).reservoir);
    }

    @Override
    public void add(double value, double weight)
    {
        reservoir.add(value, weight);
    }

    @Override
    public double calculateEntropy()
    {
        /*
        Map<Double, Double> bucketWeights = other.reservoir.stream().collect(
                Collectors.groupingBy(
                        FixedDoubleBreakdownHistogram.BucketWeight::getLeft,
                        Collectors.summingDouble(FixedDoubleBreakdownHistogram.BucketWeight::getWeight)));
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
        System.out.println("orig ent " + calculateEntropy(histogram.getWidth(), sumW, sumWLogW));
        entropy -= Streams.stream(getBreakdownHistogram().iterator()).mapToDouble(
                e -> {
                    double bucketWeight = bucketWeights.get(e.getLeft());
                    if (bucketWeight == 0.0) {
                        return 0.0;
                    }
                    return getHoldOutEntropy(
                        e.getCount(),
                        e.getRight() - e.getLeft(),
                        sumW,
                        sumWLogW,
                        bucketWeight,
                        e.getWeight(),
                        e.getCount());
                })
                .sum();
        return entropy;
         */
        return 0.0;
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
        return reservoir.estimatedInMemorySize();
    }

    @Override
    public int getRequiredBytesForSerialization()
    {
        return reservoir.getRequiredBytesForSerialization();
    }

    @Override
    public void serialize(SliceOutput out)
    {
        reservoir.serialize(out);
    }

    @Override
    public StateStrategy clone()
    {
        return new WeightedReservoirSampleStateStrategy(this);
    }
}
