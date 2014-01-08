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
import com.facebook.presto.sql.tree.Input;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import java.util.List;

public class AggregationFunctionDefinition
{
    public static AggregationFunctionDefinition aggregation(AggregationFunction function, List<Input> inputs, Optional<Input> mask, Optional<Input> sampleWeight)
    {
        Preconditions.checkNotNull(function, "function is null");
        Preconditions.checkNotNull(inputs, "inputs is null");

        return new AggregationFunctionDefinition(function, inputs, mask, sampleWeight);
    }

    private final AggregationFunction function;
    private final List<Input> inputs;
    private final Optional<Input> mask;
    private final Optional<Input> sampleWeight;

    AggregationFunctionDefinition(AggregationFunction function, List<Input> inputs, Optional<Input> mask, Optional<Input> sampleWeight)
    {
        this.function = function;
        this.inputs = inputs;
        this.mask = mask;
        this.sampleWeight = sampleWeight;
    }

    public AggregationFunction getFunction()
    {
        return function;
    }

    public List<Input> getInputs()
    {
        return inputs;
    }

    public Optional<Input> getMask()
    {
        return mask;
    }

    public Optional<Input> getSampleWeight()
    {
        return sampleWeight;
    }
}
