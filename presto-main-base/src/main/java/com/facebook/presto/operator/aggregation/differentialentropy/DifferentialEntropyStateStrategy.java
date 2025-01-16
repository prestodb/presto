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

import com.facebook.presto.spi.PrestoException;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.SizeOf;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;

/**
 * Interface for different strategies for calculating entropy: MLE (maximum likelihood
 * estimator) using NumericHistogram, jacknife estimates using a fixed histogram, compressed
 * counting and Renyi entropy, and so forth.
 */
public interface DifferentialEntropyStateStrategy
        extends Cloneable
{
    @VisibleForTesting
    String FIXED_HISTOGRAM_MLE_METHOD_NAME = "fixed_histogram_mle";
    @VisibleForTesting
    String FIXED_HISTOGRAM_JACKNIFE_METHOD_NAME = "fixed_histogram_jacknife";

    static DifferentialEntropyStateStrategy getStrategy(
            DifferentialEntropyStateStrategy strategy,
            long size,
            double sample,
            double weight,
            String method,
            double min,
            double max)
    {
        if (strategy == null) {
            switch (method) {
                case DifferentialEntropyStateStrategy.FIXED_HISTOGRAM_MLE_METHOD_NAME:
                    strategy = new FixedHistogramMleStateStrategy(size, min, max);
                    break;
                case DifferentialEntropyStateStrategy.FIXED_HISTOGRAM_JACKNIFE_METHOD_NAME:
                    strategy = new FixedHistogramJacknifeStateStrategy(size, min, max);
                    break;
                default:
                    throw new PrestoException(
                            INVALID_FUNCTION_ARGUMENT,
                            format("In differential_entropy UDF, invalid method: %s", method));
            }
        }
        else {
            switch (method) {
                case DifferentialEntropyStateStrategy.FIXED_HISTOGRAM_MLE_METHOD_NAME:
                    if (!(strategy instanceof FixedHistogramMleStateStrategy)) {
                        throw new PrestoException(
                                INVALID_FUNCTION_ARGUMENT,
                                format("In differential_entropy, strategy class is not compatible with entropy method: %s %s", strategy.getClass().getSimpleName(), method));
                    }
                    break;
                case DifferentialEntropyStateStrategy.FIXED_HISTOGRAM_JACKNIFE_METHOD_NAME:
                    if (!(strategy instanceof FixedHistogramJacknifeStateStrategy)) {
                        throw new PrestoException(
                                INVALID_FUNCTION_ARGUMENT,
                                format("In differential_entropy, strategy class is not compatible with entropy method: %s %s", strategy.getClass().getSimpleName(), method));
                    }
                    break;
                default:
                    throw new PrestoException(
                            INVALID_FUNCTION_ARGUMENT,
                            format("In differential_entropy, unknown entropy method: %s", method));
            }
        }
        strategy.validateParameters(size, sample, weight, min, max);
        return strategy;
    }

    static DifferentialEntropyStateStrategy getStrategy(
            DifferentialEntropyStateStrategy strategy,
            long size,
            double sample,
            double weight)
    {
        if (strategy == null) {
            strategy = new WeightedReservoirSampleStateStrategy(size);
        }
        else {
            verify(strategy instanceof WeightedReservoirSampleStateStrategy,
                    format("In differential entropy, expected WeightedReservoirSampleStateStrategy, got: %s", strategy.getClass().getSimpleName()));
        }
        strategy.validateParameters(size, sample, weight);
        return strategy;
    }

    static DifferentialEntropyStateStrategy getStrategy(
            DifferentialEntropyStateStrategy strategy,
            long size,
            double sample)
    {
        if (strategy == null) {
            strategy = new UnweightedReservoirSampleStateStrategy(size);
        }
        else {
            verify(strategy instanceof UnweightedReservoirSampleStateStrategy,
                    format("In differential entropy, expected UnweightedReservoirSampleStateStrategy, got: %s", strategy.getClass().getSimpleName()));
        }
        return strategy;
    }

    default void add(double sample)
    {
        verify(false, format("Unweighted unsupported for type: %s", getClass().getSimpleName()));
    }

    default void add(double sample, double weight)
    {
        verify(false, format("Weighted unsupported for type: %s", getClass().getSimpleName()));
    }

    double calculateEntropy();

    long getEstimatedSize();

    static int getRequiredBytesForSerialization(DifferentialEntropyStateStrategy strategy)
    {
        return SizeOf.SIZE_OF_INT + // magic hash
                SizeOf.SIZE_OF_INT + // method
                (strategy == null ? 0 : strategy.getRequiredBytesForSpecificSerialization());
    }

    int getRequiredBytesForSpecificSerialization();

    void serialize(SliceOutput out);

    void mergeWith(DifferentialEntropyStateStrategy other);

    DifferentialEntropyStateStrategy clone();

    default void validateParameters(long size, double sample, double weight, double min, double max)
    {
        throw new UnsupportedOperationException(
                format("In differential_entropy UDF, unsupported arguments for type: %s", getClass().getSimpleName()));
    }

    default void validateParameters(long size, double sample, double weight)
    {
        throw new UnsupportedOperationException(
                format("In differential_entropy UDF, unsupported arguments for type: %s", getClass().getSimpleName()));
    }

    default void validateParameters(long size, double sample)
    {
        throw new UnsupportedOperationException(
                format("In differential_entropy UDF, unsupported arguments for type: %s", getClass().getSimpleName()));
    }

    static void serialize(DifferentialEntropyStateStrategy strategy, SliceOutput sliceOut)
    {
        sliceOut.appendInt(DifferentialEntropyStateStrategy.class.getSimpleName().hashCode());
        if (strategy == null) {
            sliceOut.appendInt(0);
            return;
        }

        if (strategy instanceof UnweightedReservoirSampleStateStrategy) {
            sliceOut.appendInt(1);
        }
        else if (strategy instanceof WeightedReservoirSampleStateStrategy) {
            sliceOut.appendInt(2);
        }
        else if (strategy instanceof FixedHistogramMleStateStrategy) {
            sliceOut.appendInt(3);
        }
        else if (strategy instanceof FixedHistogramJacknifeStateStrategy) {
            sliceOut.appendInt(4);
        }
        else {
            verify(false, format("Strategy cannot be serialized: %s", strategy.getClass().getSimpleName()));
        }

        strategy.serialize(sliceOut);
    }

    static DifferentialEntropyStateStrategy deserialize(SliceInput input)
    {
        verify(
                input.readInt() == DifferentialEntropyStateStrategy.class.getSimpleName().hashCode(),
                "magic failed");
        int method = input.readInt();
        switch (method) {
            case 0:
                return null;
            case 1:
                return UnweightedReservoirSampleStateStrategy.deserialize(input);
            case 2:
                return WeightedReservoirSampleStateStrategy.deserialize(input);
            case 3:
                return FixedHistogramMleStateStrategy.deserialize(input);
            case 4:
                return FixedHistogramJacknifeStateStrategy.deserialize(input);
            default:
                verify(false, format("In differential_entropy UDF, Unknown method code when deserializing: %s", method));
                return null;
        }
    }

    static void combine(
            DifferentialEntropyStateStrategy strategy,
            DifferentialEntropyStateStrategy otherStrategy)
    {
        verify(strategy.getClass() == otherStrategy.getClass(),
                format("In combine, %s != %s", strategy.getClass().getSimpleName(), otherStrategy.getClass().getSimpleName()));

        strategy.mergeWith(otherStrategy);
    }

    DifferentialEntropyStateStrategy cloneEmpty();

    double getTotalPopulationWeight();
}
