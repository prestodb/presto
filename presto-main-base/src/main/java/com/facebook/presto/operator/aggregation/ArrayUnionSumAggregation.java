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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.state.ArrayUnionSumState;
import com.facebook.presto.operator.aggregation.state.ArrayUnionSumStateFactory;
import com.facebook.presto.operator.aggregation.state.ArrayUnionSumStateSerializer;
import com.facebook.presto.spi.function.aggregation.Accumulator;
import com.facebook.presto.spi.function.aggregation.AggregationMetadata;
import com.facebook.presto.spi.function.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import com.facebook.presto.spi.function.aggregation.GroupedAccumulator;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.type.StandardTypes.ARRAY;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.function.Signature.nonDecimalNumericTypeParameter;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class ArrayUnionSumAggregation
        extends SqlAggregationFunction
{
    public static final String NAME = "array_union_sum";
    public static final ArrayUnionSumAggregation ARRAY_UNION_SUM = new ArrayUnionSumAggregation();

    private static final MethodHandle INPUT_FUNCTION = methodHandle(ArrayUnionSumAggregation.class, "input", Type.class, ArrayUnionSumState.class, Block.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(ArrayUnionSumAggregation.class, "combine", ArrayUnionSumState.class, ArrayUnionSumState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(ArrayUnionSumAggregation.class, "output", ArrayUnionSumState.class, BlockBuilder.class);

    public ArrayUnionSumAggregation()
    {
        super(NAME,
                ImmutableList.of(nonDecimalNumericTypeParameter("T")),
                ImmutableList.of(),
                parseTypeSignature("array<T>"),
                ImmutableList.of(parseTypeSignature("array<T>")));
    }

    @Override
    public String getDescription()
    {
        return "Aggregate all the arrays into a single array summing the values at each index";
    }

    @Override
    public BuiltInAggregationFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type elementType = boundVariables.getTypeVariable("T");
        ArrayType outputType = (ArrayType) functionAndTypeManager.getParameterizedType(ARRAY, ImmutableList.of(
                TypeSignatureParameter.of(elementType.getTypeSignature())));

        return generateAggregation(elementType, outputType);
    }

    private static BuiltInAggregationFunctionImplementation generateAggregation(Type elementType, ArrayType outputType)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(ArrayUnionSumAggregation.class.getClassLoader());
        List<Type> inputTypes = ImmutableList.of(outputType);
        ArrayUnionSumStateSerializer stateSerializer = new ArrayUnionSumStateSerializer(outputType);
        Type intermediateType = stateSerializer.getSerializedType();

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, outputType.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(outputType),
                INPUT_FUNCTION.bindTo(elementType),
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        ArrayUnionSumState.class,
                        stateSerializer,
                        new ArrayUnionSumStateFactory(elementType))),
                outputType);

        Class<? extends Accumulator> accumulatorClass = AccumulatorCompiler.generateAccumulatorClass(
                Accumulator.class,
                metadata,
                classLoader);
        Class<? extends GroupedAccumulator> groupedAccumulatorClass = AccumulatorCompiler.generateAccumulatorClass(
                GroupedAccumulator.class,
                metadata,
                classLoader);
        return new BuiltInAggregationFunctionImplementation(NAME, inputTypes, ImmutableList.of(intermediateType), outputType,
                true, false, metadata, accumulatorClass, groupedAccumulatorClass);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type inputType)
    {
        return ImmutableList.of(
                new ParameterMetadata(STATE),
                new ParameterMetadata(INPUT_CHANNEL, inputType));
    }

    public static void input(Type elementType, ArrayUnionSumState state, Block arrayBlock)
    {
        if (arrayBlock == null) {
            return;
        }

        ArrayUnionSumResult arrayUnionSumResult = state.get();
        long startSize;

        if (arrayUnionSumResult == null) {
            startSize = 0;
            arrayUnionSumResult = ArrayUnionSumResult.create(elementType, state.getAdder(), arrayBlock);
            state.set(arrayUnionSumResult);
        }
        else {
            startSize = arrayUnionSumResult.getRetainedSizeInBytes();
            state.set(state.get().unionSum(arrayBlock));
            arrayUnionSumResult = state.get();
        }

        state.addMemoryUsage(arrayUnionSumResult.getRetainedSizeInBytes() - startSize);
    }

    public static void combine(ArrayUnionSumState state, ArrayUnionSumState otherState)
    {
        if (state.get() == null) {
            state.set(otherState.get());
            if (otherState.get() != null) {
                state.addMemoryUsage(otherState.get().getRetainedSizeInBytes());
            }
            return;
        }

        long startSize = state.get().getRetainedSizeInBytes();
        state.set(state.get().unionSum(otherState.get()));
        state.addMemoryUsage(state.get().getRetainedSizeInBytes() - startSize);
    }

    public static void output(ArrayUnionSumState state, BlockBuilder out)
    {
        ArrayUnionSumResult arrayUnionSumResult = state.get();
        if (arrayUnionSumResult == null) {
            out.appendNull();
        }
        else {
            arrayUnionSumResult.serialize(out);
        }
    }
}
