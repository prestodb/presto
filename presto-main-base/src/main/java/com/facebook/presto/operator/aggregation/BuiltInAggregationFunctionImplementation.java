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

import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.JavaAggregationFunctionImplementation;
import com.facebook.presto.spi.function.aggregation.Accumulator;
import com.facebook.presto.spi.function.aggregation.AggregationMetadata;
import com.facebook.presto.spi.function.aggregation.GroupedAccumulator;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public final class BuiltInAggregationFunctionImplementation
        implements JavaAggregationFunctionImplementation
{
    private final String name;
    private final List<Type> parameterTypes;
    private final List<Type> intermediateType;
    private final Type finalType;
    private final List<Class> lambdaInterfaces;
    private final boolean decomposable;
    private final boolean orderSensitive;

    private final AggregationMetadata aggregationMetadata;

    private final Class<? extends Accumulator> accumulatorClass;

    private final Class<? extends GroupedAccumulator> groupedAccumulatorClass;

    public BuiltInAggregationFunctionImplementation(
            String name,
            List<Type> parameterTypes,
            List<Type> intermediateType,
            Type finalType,
            boolean decomposable,
            boolean orderSensitive,
            AggregationMetadata aggregationMetadata,
            Class<? extends Accumulator> accumulatorClass,
            Class<? extends GroupedAccumulator> groupedAccumulatorClass)
    {
        this(
                name,
                parameterTypes,
                intermediateType,
                finalType,
                decomposable,
                orderSensitive,
                aggregationMetadata,
                accumulatorClass,
                groupedAccumulatorClass,
                ImmutableList.of());
    }

    public BuiltInAggregationFunctionImplementation(
            String name,
            List<Type> parameterTypes,
            List<Type> intermediateType,
            Type finalType,
            boolean decomposable,
            boolean orderSensitive,
            AggregationMetadata aggregationMetadata,
            Class<? extends Accumulator> accumulatorClass,
            Class<? extends GroupedAccumulator> groupedAccumulatorClass,
            List<Class> lambdaInterfaces)
    {
        this.name = requireNonNull(name, "name is null");
        checkArgument(!name.isEmpty(), "name is empty");
        this.parameterTypes = ImmutableList.copyOf(requireNonNull(parameterTypes, "parameterTypes is null"));
        this.intermediateType = requireNonNull(intermediateType, "intermediateType is null");
        this.finalType = requireNonNull(finalType, "finalType is null");
        this.decomposable = decomposable;
        this.orderSensitive = orderSensitive;
        this.aggregationMetadata = aggregationMetadata;
        this.accumulatorClass = accumulatorClass;
        this.groupedAccumulatorClass = groupedAccumulatorClass;
        this.lambdaInterfaces = ImmutableList.copyOf(lambdaInterfaces);
    }

    public String name()
    {
        return name;
    }

    public List<Type> getParameterTypes()
    {
        return parameterTypes;
    }

    public Type getFinalType()
    {
        return finalType;
    }

    public Type getIntermediateType()
    {
        if (intermediateType.size() == 1) {
            return getOnlyElement(intermediateType);
        }
        else {
            return RowType.withDefaultFieldNames(intermediateType);
        }
    }

    public List<Class> getLambdaInterfaces()
    {
        return lambdaInterfaces;
    }

    /**
     * Indicates that the aggregation can be decomposed, and run as partial aggregations followed by a final aggregation to combine the intermediate results
     */
    public boolean isDecomposable()
    {
        return decomposable;
    }

    /**
     * Indicates that the aggregation is sensitive to input order
     */
    public boolean isOrderSensitive()
    {
        return orderSensitive;
    }

    public AggregationMetadata getAggregationMetadata()
    {
        return aggregationMetadata;
    }

    public Class<? extends Accumulator> getAccumulatorClass()
    {
        return accumulatorClass;
    }

    public Class<? extends GroupedAccumulator> getGroupedAccumulatorClass()
    {
        return groupedAccumulatorClass;
    }
}
