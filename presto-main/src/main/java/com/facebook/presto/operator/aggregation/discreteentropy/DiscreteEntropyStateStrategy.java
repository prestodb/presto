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
package com.facebook.presto.operator.aggregation.discreteentropy;

import com.facebook.presto.operator.aggregation.differentialentropy.DifferentialEntropyStateStrategy;
import com.facebook.presto.operator.aggregation.differentialentropy.FixedHistogramJacknifeStateStrategy;
import com.facebook.presto.operator.aggregation.differentialentropy.FixedHistogramMleStateStrategy;
import com.facebook.presto.operator.aggregation.differentialentropy.UnweightedReservoirSampleStateStrategy;
import com.facebook.presto.operator.aggregation.differentialentropy.WeightedReservoirSampleStateStrategy;
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
public interface DiscreteEntropyStateStrategy
        extends Cloneable
{
    @VisibleForTesting
    String MLE_METHOD_NAME = "mle";
    @VisibleForTesting
    String JACKNIFE_METHOD_NAME = "jacknife";

    static DiscreteEntropyStateStrategy getStrategy(
            DiscreteEntropyStateStrategy strategy,
            double weight,
            String method)
    {
        if (strategy == null) {
            switch (method) {
                case DiscreteEntropyStateStrategy.MLE_METHOD_NAME:
                    strategy = new WeightedMleStateStrategy();
                    break;
                case DiscreteEntropyStateStrategy.JACKNIFE_METHOD_NAME:
                    strategy = new WeightedJacknifeStateStrategy();
                    break;
                default:
                    throw new PrestoException(
                            INVALID_FUNCTION_ARGUMENT,
                            format("In discrete_entropy UDF, invalid method: %s", method));
            }
        }
        else {
            switch (method) {
                case DiscreteEntropyStateStrategy.MLE_METHOD_NAME:
                    if (!(strategy instanceof WeightedMleStateStrategy)) {
                        throw new PrestoException(
                                INVALID_FUNCTION_ARGUMENT,
                                format("In differential_entropy, strategy class is not compatible with entropy method: %s %s", strategy.getClass().getSimpleName(), method));
                    }
                    break;
                case DiscreteEntropyStateStrategy.JACKNIFE_METHOD_NAME:
                    if (!(strategy instanceof WeightedJacknifeStateStrategy)) {
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
        validateWeight(weight);
        return strategy;
    }

    static DiscreteEntropyStateStrategy getStrategy(
            DiscreteEntropyStateStrategy strategy,
            double weight)
    {
        if (strategy == null) {
            strategy = new WeightedMleStateStrategy();
        }
        else {
            if (!(strategy instanceof WeightedMleStateStrategy)) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        format("In differential_entropy, strategy class is not compatible with entropy method: %s", strategy.getClass().getSimpleName()));
            }
        }
        validateWeight(weight);
        return strategy;
    }

    static DiscreteEntropyStateStrategy getStrategy(
            DiscreteEntropyStateStrategy strategy,
            String method)
    {
        if (strategy == null) {
            switch (method) {
                case DiscreteEntropyStateStrategy.MLE_METHOD_NAME:
                    strategy = new UnweightedMleStateStrategy();
                    break;
                case DiscreteEntropyStateStrategy.JACKNIFE_METHOD_NAME:
                    strategy = new UnweightedJacknifeStateStrategy();
                    break;
                default:
                    throw new PrestoException(
                            INVALID_FUNCTION_ARGUMENT,
                            format("In discrete_entropy UDF, invalid method: %s", method));
            }
        }
        else {
            switch (method) {
                case DiscreteEntropyStateStrategy.MLE_METHOD_NAME:
                    if (!(strategy instanceof UnweightedMleStateStrategy)) {
                        throw new PrestoException(
                                INVALID_FUNCTION_ARGUMENT,
                                format("In differential_entropy, strategy class is not compatible with entropy method: %s %s", strategy.getClass().getSimpleName(), method));
                    }
                    break;
                case DiscreteEntropyStateStrategy.JACKNIFE_METHOD_NAME:
                    if (!(strategy instanceof UnweightedJacknifeStateStrategy)) {
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
        return strategy;
    }

    static DiscreteEntropyStateStrategy getStrategy(
            DiscreteEntropyStateStrategy strategy)
    {
        if (strategy == null) {
            strategy = new UnweightedMleStateStrategy();
        }
        else {
            if (!(strategy instanceof UnweightedMleStateStrategy)) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        format("In differential_entropy, strategy class is not compatible with entropy method: %s %s", strategy.getClass().getSimpleName()));
            }
        }
        return strategy;
    }

    static DiscreteEntropyStateStrategy getStrategy(
            DiscreteEntropyStateStrategy strategy,
            DifferentialEntropyStateStrategy differentialStrategy)
    {
        if (strategy == null) {
            if (differentialStrategy instanceof FixedHistogramMleStateStrategy) {
                strategy = new UnweightedMleStateStrategy();
            }
            else if (differentialStrategy instanceof FixedHistogramJacknifeStateStrategy) {
                strategy = new UnweightedJacknifeStateStrategy();
            }
            else if (differentialStrategy instanceof UnweightedReservoirSampleStateStrategy) {
                strategy = new UnweightedMleStateStrategy();
            }
            else {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        format(
                                "In discrete_entropy UDF, invalid differential entropy class: %s",
                                differentialStrategy.getClass().getSimpleName()));
            }
        }
        return strategy;
    }

    static DiscreteEntropyStateStrategy getStrategy(
            DiscreteEntropyStateStrategy strategy,
            DifferentialEntropyStateStrategy differentialStrategy,
            double weight)
    {
        if (strategy == null) {
            if (differentialStrategy instanceof FixedHistogramMleStateStrategy) {
                strategy = new WeightedMleStateStrategy();
            }
            else if (differentialStrategy instanceof FixedHistogramJacknifeStateStrategy) {
                strategy = new WeightedJacknifeStateStrategy();
            }
            else if (differentialStrategy instanceof WeightedReservoirSampleStateStrategy) {
                strategy = new WeightedMleStateStrategy();
            }
            else {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        format(
                                "In discrete_entropy UDF, invalid differential entropy class: %s",
                                differentialStrategy.getClass().getSimpleName()));
            }
        }
        return strategy;
    }

    default void add(int sample, double weight)
    {
        verify(false, format("Weighted unsupported for type: %s", getClass().getSimpleName()));
    }

    default void add(int sample)
    {
        verify(false, format("Unweighted unsupported for type: %s", getClass().getSimpleName()));
    }

    double calculateEntropy();

    long getEstimatedSize();

    static int getRequiredBytesForSerialization(DiscreteEntropyStateStrategy strategy)
    {
        return SizeOf.SIZE_OF_INT + // magic hash
                SizeOf.SIZE_OF_INT + // method
                (strategy == null ? 0 : strategy.getRequiredBytesForSpecificSerialization()); // strategy
    }

    int getRequiredBytesForSpecificSerialization();

    static void serialize(DiscreteEntropyStateStrategy strategy, SliceOutput sliceOut)
    {
        sliceOut.appendInt(DiscreteEntropyStateStrategy.class.getSimpleName().hashCode());
        if (strategy == null) {
            sliceOut.appendInt(0);
            return;
        }

        if (strategy instanceof UnweightedMleStateStrategy) {
            sliceOut.appendInt(1);
        }
        else if (strategy instanceof WeightedMleStateStrategy) {
            sliceOut.appendInt(2);
        }
        else if (strategy instanceof UnweightedJacknifeStateStrategy) {
            sliceOut.appendInt(3);
        }
        else if (strategy instanceof WeightedJacknifeStateStrategy) {
            sliceOut.appendInt(4);
        }
        else {
            verify(false, format("Strategy cannot be serialized: %s", strategy.getClass().getSimpleName()));
        }

        strategy.serialize(sliceOut);
    }

    static DiscreteEntropyStateStrategy deserialize(SliceInput input)
    {
        int hash = input.readInt();
        verify(
                 hash == DiscreteEntropyStateStrategy.class.getSimpleName().hashCode(),
                "magic failed");
        int method = input.readInt();
        DiscreteEntropyStateStrategy strategy = null;
        switch (method) {
            case 0:
                strategy = null;
                break;
            case 1:
                strategy = UnweightedMleStateStrategy.deserialize(input);
                break;
            case 2:
                strategy = WeightedMleStateStrategy.deserialize(input);
                break;
            case 3:
                strategy = UnweightedJacknifeStateStrategy.deserialize(input);
                break;
            case 4:
                strategy = WeightedJacknifeStateStrategy.deserialize(input);
                break;
            default:
                verify(
                        false,
                        format("method unknown when deserializing: %s", method));
        }
        return strategy;
    }

    static void combine(
            DiscreteEntropyStateStrategy strategy,
            DiscreteEntropyStateStrategy otherStrategy)
    {
        verify(strategy.getClass() == otherStrategy.getClass(),
                format("In combine, %s != %s", strategy.getClass().getSimpleName(), otherStrategy.getClass().getSimpleName()));

        strategy.mergeWith(otherStrategy);
    }

    void serialize(SliceOutput out);

    void mergeWith(DiscreteEntropyStateStrategy other);

    DiscreteEntropyStateStrategy clone();

    static void validateWeight(double weight)
    {
        if (weight < 0.0) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    format("Weight cannot be negative: %s", weight));
        }
    }
}
