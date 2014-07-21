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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.spi.type.Type;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class GenericAggregationFunction
        implements AggregationFunction
{
    private final List<Type> parameterTypes;
    private final Type intermediateType;
    private final Type finalType;
    private final boolean decomposable;
    private final AccumulatorFactory accumulatorFactory;

    public GenericAggregationFunction(List<Type> parameterTypes, Type intermediateType, Type finalType, boolean decomposable, AccumulatorFactory accumulatorFactory)
    {
        this.parameterTypes = ImmutableList.copyOf(checkNotNull(parameterTypes, "parameterTypes is null"));
        this.intermediateType = checkNotNull(intermediateType, "intermediateType is null");
        this.finalType = checkNotNull(finalType, "finalType is null");
        this.decomposable = decomposable;
        this.accumulatorFactory = checkNotNull(accumulatorFactory, "accumulatorFactory is null");
    }

    @Override
    public List<Type> getParameterTypes()
    {
        return parameterTypes;
    }

    @Override
    public Type getFinalType()
    {
        return finalType;
    }

    @Override
    public Type getIntermediateType()
    {
        return intermediateType;
    }

    @Override
    public boolean isDecomposable()
    {
        return decomposable;
    }

    @Override
    public Accumulator createAggregation(Optional<Integer> maskChannel, Optional<Integer> sampleWeight, double confidence, int... argumentChannels)
    {
        checkArgument(argumentChannels.length == parameterTypes.size(), "Expected %d input channels, but got %d", parameterTypes.size(), argumentChannels.length);
        return accumulatorFactory.createAggregation(maskChannel, sampleWeight, confidence, argumentChannels);
    }

    @Override
    public Accumulator createIntermediateAggregation(double confidence)
    {
        return accumulatorFactory.createIntermediateAggregation(confidence);
    }

    @Override
    public GroupedAccumulator createGroupedAggregation(Optional<Integer> maskChannel, Optional<Integer> sampleWeight, double confidence, int... argumentChannels)
    {
        checkArgument(argumentChannels.length == parameterTypes.size(), "Expected %d input channels, but got %d", parameterTypes.size(), argumentChannels.length);
        return accumulatorFactory.createGroupedAggregation(maskChannel, sampleWeight, confidence, argumentChannels);
    }

    @Override
    public GroupedAccumulator createGroupedIntermediateAggregation(double confidence)
    {
        return accumulatorFactory.createGroupedIntermediateAggregation(confidence);
    }
}
