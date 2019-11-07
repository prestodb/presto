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

import io.airlift.slice.SliceOutput;

import static java.lang.String.format;

/**
 * Interface for different strategies for calculating entropy: MLE (maximum likelihood
 * estimator) using NumericHistogram, jacknife estimates using a fixed histogram, compressed
 * counting and Renyi entropy, and so forth.
 */
public interface DifferentialEntropyStateStrategy
        extends Cloneable
{
    void add(double sample, double weight);

    double calculateEntropy();

    long getEstimatedSize();

    int getRequiredBytesForSerialization();

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

    DifferentialEntropyStateStrategy cloneEmpty();
}
