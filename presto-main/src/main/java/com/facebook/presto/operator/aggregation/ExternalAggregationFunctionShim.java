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

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import com.facebook.presto.spi.function.AggregationFunctionDescription;
import com.facebook.presto.spi.function.AggregationFunctionImplementation;
import com.facebook.presto.spi.function.ExternalAggregationFunctionImplementation;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.operator.aggregation.AccumulatorCompiler.generateAccumulatorFactoryBinder;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;

public final class ExternalAggregationFunctionShim
{
    private ExternalAggregationFunctionShim()
    {
    }

    public static InternalAggregationFunction create(AggregationFunctionImplementation implementation)
    {
        if (implementation instanceof ExternalAggregationFunctionImplementation) {
            ExternalAggregationFunctionImplementation impl = (ExternalAggregationFunctionImplementation) implementation;
            DynamicClassLoader classLoader = new DynamicClassLoader(implementation.getClass().getClassLoader());
            AggregationFunctionDescription descriptor = impl.getAggregationFunctionDescription();
            AggregationMetadata metadata = new AggregationMetadata(
                    generateAggregationName(descriptor.getName(),
                            descriptor.getFinalType().getTypeSignature(),
                            descriptor.getParameterTypes().stream().map(Type::getTypeSignature).collect(Collectors.toList())),
                    createInputParameterMetadata(descriptor.getParameterTypes()),
                    impl.getAccumulatorFunctions().getInputFunction(),
                    impl.getAccumulatorFunctions().getCombineFunction(),
                    impl.getAccumulatorFunctions().getOutputFunction(),
                    ImmutableList.of(new AccumulatorStateDescriptor(
                            impl.getAccumulatorStateDescription().getAccumulatorStateInterface(),
                            impl.getAccumulatorStateDescription().getAccumulatorStateSerializer(),
                            impl.getAccumulatorStateDescription().getAccumulatorStateFactory())),
                    descriptor.getFinalType());
            return new InternalAggregationFunction(descriptor.getName(),
                    descriptor.getParameterTypes(),
                    descriptor.getIntermediateTypes(),
                    descriptor.getFinalType(),
                    descriptor.isDecomposable(),
                    descriptor.isOrderSensitive(),
                    generateAccumulatorFactoryBinder(metadata, classLoader));
        }
        throw new UnsupportedOperationException("Unsupported type " + implementation);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(List<Type> inputTypes)
    {
        Builder<ParameterMetadata> builder = new Builder<>();
        builder.add(new ParameterMetadata(STATE));
        for (Type inputType : inputTypes) {
            builder.add(new ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, inputType));
        }
        builder.add(new ParameterMetadata(BLOCK_INDEX));
        return builder.build();
    }
}
