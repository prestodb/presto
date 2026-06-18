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

package com.facebook.presto.hive.functions.aggregation;

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.JavaAggregationFunctionImplementation;
import com.facebook.presto.spi.function.aggregation.Accumulator;
import com.facebook.presto.spi.function.aggregation.AggregationMetadata;
import com.facebook.presto.spi.function.aggregation.GroupedAccumulator;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.functions.gen.AccumulatorCompiler.generateAccumulatorClass;
import static com.facebook.presto.hive.functions.gen.AggregationUtils.createInputParameterMetadata;
import static com.facebook.presto.hive.functions.gen.AggregationUtils.generateAggregationName;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class HiveAggregationFunctionImplementation
        implements JavaAggregationFunctionImplementation
{
    private final HiveAggregationFunctionDescription aggregationFunctionDescription;
    private final AggregationMetadata aggregationMetadata;
    private final Class<? extends Accumulator> accumulatorClass;
    private final Class<? extends GroupedAccumulator> groupedAccumulatorClass;

    public HiveAggregationFunctionImplementation(
            HiveAggregationFunctionDescription aggregationFunctionDescription,
            HiveAccumulatorFunctions accumulatorFunctions,
            HiveAccumulatorStateDescription accumulatorStateDescription)
    {
        this.aggregationFunctionDescription = requireNonNull(aggregationFunctionDescription);
        DynamicClassLoader classLoader = new DynamicClassLoader(this.getClass().getClassLoader());
        this.aggregationMetadata = new AggregationMetadata(
                generateAggregationName(aggregationFunctionDescription.getName(),
                        aggregationFunctionDescription.getFinalType().getTypeSignature(),
                        aggregationFunctionDescription.getParameterTypes().stream().map(Type::getTypeSignature).collect(Collectors.toList())),
                createInputParameterMetadata(aggregationFunctionDescription.getParameterTypes()),
                accumulatorFunctions.getInputFunction(),
                accumulatorFunctions.getCombineFunction(),
                accumulatorFunctions.getOutputFunction(),
                ImmutableList.of(new AggregationMetadata.AccumulatorStateDescriptor(
                        accumulatorStateDescription.getAccumulatorStateInterface(),
                        accumulatorStateDescription.getAccumulatorStateSerializer(),
                        accumulatorStateDescription.getAccumulatorStateFactory())),
                aggregationFunctionDescription.getFinalType());
        this.accumulatorClass = generateAccumulatorClass(
                Accumulator.class,
                aggregationMetadata,
                classLoader);

        this.groupedAccumulatorClass = generateAccumulatorClass(
                GroupedAccumulator.class,
                aggregationMetadata,
                classLoader);
    }

    @Override
    public boolean isDecomposable()
    {
        return aggregationFunctionDescription.isDecomposable();
    }

    @Override
    public List<Type> getParameterTypes()
    {
        return aggregationFunctionDescription.getParameterTypes();
    }

    @Override
    public Type getIntermediateType()
    {
        List<Type> intermediateType = aggregationFunctionDescription.getIntermediateTypes();
        if (intermediateType.size() == 1) {
            return getOnlyElement(intermediateType);
        }
        else {
            return RowType.withDefaultFieldNames(intermediateType);
        }
    }

    @Override
    public Type getFinalType()
    {
        return aggregationFunctionDescription.getFinalType();
    }

    @Override
    public boolean isOrderSensitive()
    {
        return aggregationFunctionDescription.isOrderSensitive();
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
