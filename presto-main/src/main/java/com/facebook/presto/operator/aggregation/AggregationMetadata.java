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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AggregationMetadata
{
    public static final Set<Class<?>> SUPPORTED_PARAMETER_TYPES = ImmutableSet.of(Block.class, long.class, double.class, boolean.class, Slice.class);

    private final String name;
    private final List<ParameterMetadata> inputMetadata;
    private final MethodHandle inputFunction;
    private final MethodHandle combineFunction;
    private final MethodHandle outputFunction;
    private final AccumulatorStateSerializer<?> stateSerializer;
    private final AccumulatorStateFactory<?> stateFactory;
    private final Type outputType;

    public AggregationMetadata(
            String name,
            List<ParameterMetadata> inputMetadata,
            MethodHandle inputFunction,
            MethodHandle combineFunction,
            MethodHandle outputFunction,
            Class<?> stateInterface,
            AccumulatorStateSerializer<?> stateSerializer,
            AccumulatorStateFactory<?> stateFactory,
            Type outputType)
    {
        this.outputType = requireNonNull(outputType);
        this.inputMetadata = ImmutableList.copyOf(requireNonNull(inputMetadata, "inputMetadata is null"));
        this.name = requireNonNull(name, "name is null");
        this.inputFunction = requireNonNull(inputFunction, "inputFunction is null");
        this.combineFunction = requireNonNull(combineFunction, "combineFunction is null");
        this.outputFunction = requireNonNull(outputFunction, "outputFunction is null");
        this.stateSerializer = requireNonNull(stateSerializer, "stateSerializer is null");
        this.stateFactory = requireNonNull(stateFactory, "stateFactory is null");

        verifyInputFunctionSignature(inputFunction, inputMetadata, stateInterface);
        verifyCombineFunction(combineFunction, stateInterface);
        verifyExactOutputFunction(outputFunction, stateInterface);
    }

    public Type getOutputType()
    {
        return outputType;
    }

    public List<ParameterMetadata> getInputMetadata()
    {
        return inputMetadata;
    }

    public String getName()
    {
        return name;
    }

    public MethodHandle getInputFunction()
    {
        return inputFunction;
    }

    public MethodHandle getCombineFunction()
    {
        return combineFunction;
    }

    public MethodHandle getOutputFunction()
    {
        return outputFunction;
    }

    public AccumulatorStateSerializer<?> getStateSerializer()
    {
        return stateSerializer;
    }

    public AccumulatorStateFactory<?> getStateFactory()
    {
        return stateFactory;
    }

    private static void verifyInputFunctionSignature(MethodHandle method, List<ParameterMetadata> parameterMetadatas, Class<?> stateInterface)
    {
        Class<?>[] parameters = method.type().parameterArray();
        checkArgument(stateInterface == parameters[0], "First argument of aggregation input function must be %s", stateInterface.getSimpleName());
        checkArgument(parameters.length > 0, "Aggregation input function must have at least one parameter");
        checkArgument(parameterMetadatas.get(0).getParameterType() == STATE, "First parameter must be state");
        for (int i = 1; i < parameters.length; i++) {
            ParameterMetadata metadata = parameterMetadatas.get(i);
            switch (metadata.getParameterType()) {
                case BLOCK_INPUT_CHANNEL:
                case NULLABLE_BLOCK_INPUT_CHANNEL:
                    checkArgument(parameters[i] == Block.class, "Parameter must be Block if it has @BlockPosition");
                    break;
                case INPUT_CHANNEL:
                    checkArgument(SUPPORTED_PARAMETER_TYPES.contains(parameters[i]), "Unsupported type: %s", parameters[i].getSimpleName());
                    checkArgument(parameters[i] == metadata.getSqlType().getJavaType(),
                            "Expected method %s parameter %s type to be %s (%s)",
                            method,
                            i,
                            metadata.getSqlType().getJavaType().getName(),
                            metadata.getSqlType());
                    break;
                case BLOCK_INDEX:
                    checkArgument(parameters[i] == int.class, "Block index parameter must be an int");
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported parameter: " + metadata.getParameterType());
            }
        }
    }

    private static void verifyCombineFunction(MethodHandle method, Class<?> stateInterface)
    {
        Class<?>[] parameterTypes = method.type().parameterArray();
        checkArgument(parameterTypes.length == 2 && parameterTypes[0] == stateInterface && parameterTypes[1] == stateInterface, "Combine function must have the signature (%s, %s)", stateInterface.getSimpleName(), stateInterface.getSimpleName());
    }

    private static void verifyExactOutputFunction(MethodHandle method, Class<?> stateInterface)
    {
        if (method == null) {
            return;
        }
        Class<?>[] parameterTypes = method.type().parameterArray();
        checkArgument(parameterTypes.length == 2 && parameterTypes[0] == stateInterface && parameterTypes[1] == BlockBuilder.class, "Output function must have the signature (%s, BlockBuilder)", stateInterface.getSimpleName());
    }

    public static int countInputChannels(List<ParameterMetadata> metadatas)
    {
        int parameters = 0;
        for (ParameterMetadata metadata : metadatas) {
            if (metadata.getParameterType() == INPUT_CHANNEL ||
                    metadata.getParameterType() == BLOCK_INPUT_CHANNEL ||
                    metadata.getParameterType() == NULLABLE_BLOCK_INPUT_CHANNEL) {
                parameters++;
            }
        }

        return parameters;
    }

    public static class ParameterMetadata
    {
        private final ParameterType parameterType;
        private final Type sqlType;

        public ParameterMetadata(ParameterType parameterType)
        {
            this(parameterType, null);
        }

        public ParameterMetadata(ParameterType parameterType, Type sqlType)
        {
            checkArgument((sqlType == null) == (parameterType == BLOCK_INDEX || parameterType == STATE),
                    "sqlType must be provided only for input channels");
            this.parameterType = parameterType;
            this.sqlType = sqlType;
        }

        public static ParameterMetadata fromSqlType(Type sqlType, boolean isBlock, boolean isNullable, String methodName)
        {
            if (isBlock) {
                if (isNullable) {
                    return new ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, sqlType);
                }
                else {
                    return new ParameterMetadata(BLOCK_INPUT_CHANNEL, sqlType);
                }
            }
            else {
                if (isNullable) {
                    throw new IllegalArgumentException(methodName + " contains a parameter with @NullablePosition that is not @BlockPosition");
                }
                else {
                    return new ParameterMetadata(INPUT_CHANNEL, sqlType);
                }
            }
        }

        public static ParameterMetadata forBlockIndexParameter()
        {
            return new ParameterMetadata(BLOCK_INDEX);
        }

        public static ParameterMetadata forStateParameter()
        {
            return new ParameterMetadata(STATE);
        }

        public ParameterType getParameterType()
        {
            return parameterType;
        }

        public Type getSqlType()
        {
            return sqlType;
        }

        public enum ParameterType
        {
            INPUT_CHANNEL,
            BLOCK_INPUT_CHANNEL,
            NULLABLE_BLOCK_INPUT_CHANNEL,
            BLOCK_INDEX,
            STATE
        }
    }
}
