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
package com.facebook.presto.operator;

import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import java.util.List;

public class AggregationFunctionDefinition
{
    public static AggregationFunctionDefinition aggregation(AggregationFunction function, List<Integer> inputs, Optional<Integer> mask, Optional<Integer> sampleWeight, double confidence)
    {
        Preconditions.checkNotNull(function, "function is null");
        Preconditions.checkNotNull(inputs, "inputs is null");

        return new AggregationFunctionDefinition(function, inputs, mask, sampleWeight, confidence);
    }

    private final AggregationFunction function;
    private final List<Integer> inputs;
    private final Optional<Integer> mask;
    private final Optional<Integer> sampleWeight;
    private final double confidence;

    AggregationFunctionDefinition(AggregationFunction function, List<Integer> inputs, Optional<Integer> mask, Optional<Integer> sampleWeight, double confidence)
    {
        this.function = function;
        this.inputs = inputs;
        this.mask = mask;
        this.sampleWeight = sampleWeight;
        this.confidence = confidence;
    }

    public AggregationFunction getFunction()
    {
        return function;
    }

    public List<Integer> getInputs()
    {
        return inputs;
    }

    public Optional<Integer> getMask()
    {
        return mask;
    }

    public Optional<Integer> getSampleWeight()
    {
        return sampleWeight;
    }

    public double getConfidence()
    {
        return confidence;
    }
}
