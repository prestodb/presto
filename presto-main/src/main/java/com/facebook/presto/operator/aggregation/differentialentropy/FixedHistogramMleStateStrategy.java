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

import com.facebook.presto.operator.aggregation.fixedhistogram.FixedDoubleHistogram;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static com.facebook.presto.operator.aggregation.differentialentropy.FixedHistogramStateStrategyUtils.getXLogX;
import static com.google.common.collect.Streams.stream;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Calculates sample entropy using MLE (maximum likelihood estimates) on a NumericHistogram.
 */
public class FixedHistogramMleStateStrategy
        implements DifferentialEntropyStateStrategy
{
    private final FixedDoubleHistogram histogram;

    public FixedHistogramMleStateStrategy(long bucketCount, double min, double max)
    {
        FixedHistogramStateStrategyUtils.validateParameters(
                bucketCount,
                min,
                max);

        histogram = new FixedDoubleHistogram(toIntExact(bucketCount), min, max);
    }

    private FixedHistogramMleStateStrategy(FixedHistogramMleStateStrategy other)
    {
        histogram = other.histogram.clone();
    }

    private FixedHistogramMleStateStrategy(FixedDoubleHistogram histogram)
    {
        this.histogram = requireNonNull(histogram, "histogram is null");
    }

    @Override
    public void validateParameters(
            long bucketCount,
            double sample,
            double weight,
            double min,
            double max)
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
    public void add(double sample, double weight)
    {
        histogram.add(sample, weight);
    }

    @Override
    public double getTotalPopulationWeight()
    {
        return stream(histogram.iterator())
                .mapToDouble(FixedDoubleHistogram.Bucket::getWeight)
                .sum();
    }

    @Override
    public double calculateEntropy()
    {
        double sum = 0;
        for (FixedDoubleHistogram.Bucket bucket : histogram) {
            sum += bucket.getWeight();
        }
        if (sum == 0.0) {
            return Double.NaN;
        }

        double rawEntropy = 0;
        for (FixedDoubleHistogram.Bucket bucket : histogram) {
            rawEntropy -= getXLogX(bucket.getWeight() / sum);
        }
        return (rawEntropy + Math.log(histogram.getWidth())) / Math.log(2);
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

    @Override
    public void mergeWith(DifferentialEntropyStateStrategy other)
    {
        histogram.mergeWith(((FixedHistogramMleStateStrategy) other).histogram);
    }

    public static FixedHistogramMleStateStrategy deserialize(SliceInput input)
    {
        FixedDoubleHistogram histogram = FixedDoubleHistogram.deserialize(input);

        return new FixedHistogramMleStateStrategy(histogram);
    }

    @Override
    public void serialize(SliceOutput out)
    {
        histogram.serialize(out);
    }

    @Override
    public DifferentialEntropyStateStrategy clone()
    {
        return new FixedHistogramMleStateStrategy(this);
    }

    @Override
    public DifferentialEntropyStateStrategy cloneEmpty()
    {
        return new FixedHistogramMleStateStrategy(histogram.getBucketCount(), histogram.getMin(), histogram.getMax());
    }
}
