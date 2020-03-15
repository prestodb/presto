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

import static com.facebook.presto.operator.aggregation.differentialentropy.EntropyCalculations.calculateFromSamplesUsingVasicek;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;

public class WeightedReservoirSampleStateStrategy
        implements DifferentialEntropyStateStrategy
{
    private final WeightedDoubleReservoirSample reservoir;

    public WeightedReservoirSampleStateStrategy(long maxSamples)
    {
        if (maxSamples <= 0) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    format("In differential_entropy UDF, max samples must be positive: %s", maxSamples));
        }
        if (maxSamples >= WeightedDoubleReservoirSample.MAX_SAMPLES_LIMIT) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    format("In differential_entropy UDF, max samples  must be capped: max_samples=%s, cap=%s", maxSamples, WeightedDoubleReservoirSample.MAX_SAMPLES_LIMIT));
        }

        reservoir = new WeightedDoubleReservoirSample((int) maxSamples);
    }

    private WeightedReservoirSampleStateStrategy(WeightedReservoirSampleStateStrategy other)
    {
        reservoir = other.reservoir.clone();
    }

    private WeightedReservoirSampleStateStrategy(WeightedDoubleReservoirSample reservoir)
    {
        this.reservoir = reservoir;
    }

    @Override
    public void validateParameters(
            long maxSamples,
            double sample,
            double weight)
    {
        if (weight < 0.0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("In differential_entropy UDF, weight must be non-negative: %s", weight));
        }

        if (maxSamples != reservoir.getMaxSamples()) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    format("In differential_entropy UDF, inconsistent maxSamples: %s, %s", maxSamples, reservoir.getMaxSamples()));
        }
    }

    @Override
    public void mergeWith(DifferentialEntropyStateStrategy other)
    {
        verify(other instanceof WeightedReservoirSampleStateStrategy,
                format("class should be an instance of WeightedReservoirSampleStateStrategy: %s", other.getClass().getSimpleName()));
        reservoir.mergeWith(((WeightedReservoirSampleStateStrategy) other).reservoir);
    }

    @Override
    public void add(double value, double weight)
    {
        reservoir.add(value, weight);
    }

    @Override
    public double getTotalPopulationWeight()
    {
        return reservoir.getTotalPopulationWeight();
    }

    @Override
    public double calculateEntropy()
    {
        return calculateFromSamplesUsingVasicek(reservoir.getSamples());
    }

    @Override
    public long getEstimatedSize()
    {
        return reservoir.estimatedInMemorySize();
    }

    @Override
    public int getRequiredBytesForSpecificSerialization()
    {
        return reservoir.getRequiredBytesForSerialization();
    }

    public static WeightedReservoirSampleStateStrategy deserialize(SliceInput input)
    {
        return new WeightedReservoirSampleStateStrategy(WeightedDoubleReservoirSample.deserialize(input));
    }

    @Override
    public void serialize(SliceOutput out)
    {
        reservoir.serialize(out);
    }

    @Override
    public DifferentialEntropyStateStrategy clone()
    {
        return new WeightedReservoirSampleStateStrategy(this);
    }

    @Override
    public DifferentialEntropyStateStrategy cloneEmpty()
    {
        return new WeightedReservoirSampleStateStrategy(reservoir.getMaxSamples());
    }
}
