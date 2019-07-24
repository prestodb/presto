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

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

/*
Tmp Ami
 */
public class UnweightedReservoirSampleStateStrategy
        implements StateStrategy
{
    protected final WeightedDoubleReservoirSample reservoir;

    public UnweightedReservoirSampleStateStrategy(long size)
    {
        reservoir = new WeightedDoubleReservoirSample((int) size);
    }

    protected UnweightedReservoirSampleStateStrategy(UnweightedReservoirSampleStateStrategy other)
    {
        reservoir = other.getReservoir().clone();
    }

    public UnweightedReservoirSampleStateStrategy(SliceInput input)
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
        reservoir.mergeWith(((UnweightedReservoirSampleStateStrategy) other).reservoir);
    }

    @Override
    public void add(double value, double weight)
    {
        reservoir.add(value, weight);
    }

    @Override
    public double calculateEntropy()
    {
        return 0;
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

    public WeightedDoubleReservoirSample getReservoir()
    {
        return reservoir;
    }

    @Override
    public StateStrategy clone()
    {
        return new UnweightedReservoirSampleStateStrategy(this);
    }
}
