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

import com.facebook.presto.operator.aggregation.reservoirsample.UnweightedDoubleReservoirSample;
import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static com.facebook.presto.operator.aggregation.differentialentropy.EntropyCalculations.calculateFromSamplesUsingVasicek;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

public class UnweightedReservoirSampleStateStrategy
        implements DifferentialEntropyStateStrategy
{
    private final UnweightedDoubleReservoirSample reservoir;

    public UnweightedReservoirSampleStateStrategy(long maxSamples)
    {
        if (maxSamples <= 0) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    format("In differential_entropy UDF, max samples must be positive: %s", maxSamples));
        }
        if (maxSamples >= UnweightedDoubleReservoirSample.MAX_SAMPLES_LIMIT) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    format("In differential_entropy UDF, max samples  must be capped: max_samples=%s, cap=%s", maxSamples, UnweightedDoubleReservoirSample.MAX_SAMPLES_LIMIT));
        }

        reservoir = new UnweightedDoubleReservoirSample(toIntExact(maxSamples));
    }

    private UnweightedReservoirSampleStateStrategy(UnweightedReservoirSampleStateStrategy other)
    {
        reservoir = other.reservoir.clone();
    }

    private UnweightedReservoirSampleStateStrategy(UnweightedDoubleReservoirSample reservoir)
    {
        this.reservoir = reservoir;
    }

    @Override
    public void validateParameters(long maxSamples, double sample, double weight)
    {
        if (weight != 1.0) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    format("In differential_entropy UDF, weight must be 1.0: %s", weight));
        }

        if (maxSamples != reservoir.getMaxSamples()) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    format("In differential_entropy UDF, inconsistent maxSamples: %s, %s", maxSamples, reservoir.getMaxSamples()));
        }
    }

    @Override
    public void validateParameters(long maxSamples, double sample)
    {
        if (maxSamples != reservoir.getMaxSamples()) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    format("In differential_entropy UDF, inconsistent maxSamples: %s, %s", maxSamples, reservoir.getMaxSamples()));
        }
    }

    @Override
    public void mergeWith(DifferentialEntropyStateStrategy other)
    {
        reservoir.mergeWith(((UnweightedReservoirSampleStateStrategy) other).reservoir);
    }

    @Override
    public void add(double value)
    {
        reservoir.add(value);
    }

    @Override
    public double getTotalPopulationWeight()
    {
        return reservoir.getTotalPopulationCount();
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

    public static UnweightedReservoirSampleStateStrategy deserialize(SliceInput input)
    {
        return new UnweightedReservoirSampleStateStrategy(UnweightedDoubleReservoirSample.deserialize(input));
    }

    @Override
    public void serialize(SliceOutput out)
    {
        reservoir.serialize(out);
    }

    @Override
    public DifferentialEntropyStateStrategy clone()
    {
        return new UnweightedReservoirSampleStateStrategy(this);
    }

    @Override
    public DifferentialEntropyStateStrategy cloneEmpty()
    {
        return new UnweightedReservoirSampleStateStrategy(reservoir.getMaxSamples());
    }
}
