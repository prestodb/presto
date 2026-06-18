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
package com.facebook.presto.operator.aggregation.arrayagg;

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.AccumulatorCompiler;
import com.facebook.presto.operator.aggregation.BuiltInAggregationFunctionImplementation;
import com.facebook.presto.operator.aggregation.MapAggregationFunction;
import com.facebook.presto.operator.aggregation.SetOfValues;
import com.facebook.presto.operator.aggregation.state.SetAggregationState;
import com.facebook.presto.operator.aggregation.state.SetAggregationStateFactory;
import com.facebook.presto.spi.function.aggregation.Accumulator;
import com.facebook.presto.spi.function.aggregation.AggregationMetadata;
import com.facebook.presto.spi.function.aggregation.GroupedAccumulator;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class SetAggregationFunction
        extends SqlAggregationFunction
{
    public static final SetAggregationFunction SET_AGG = new SetAggregationFunction();
    public static final String NAME = "set_agg";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(SetAggregationFunction.class, "input", Type.class, MethodHandle.class, SetAggregationState.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(SetAggregationFunction.class, "combine", SetAggregationState.class, SetAggregationState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(SetAggregationFunction.class, "output", SetAggregationState.class, BlockBuilder.class);

    public SetAggregationFunction()
    {
        super(NAME,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature("array(T)"),
                ImmutableList.of(parseTypeSignature("T")));
    }

    @Override
    public String getDescription()
    {
        return "Aggregates distinct values into a single array";
    }

    @Override
    public boolean isCalledOnNullInput()
    {
        return true;
    }

    @Override
    public BuiltInAggregationFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type inputType = boundVariables.getTypeVariable("T");
        ArrayType outputType = (ArrayType) functionAndTypeManager.getParameterizedType(StandardTypes.ARRAY, ImmutableList.of(TypeSignatureParameter.of(inputType.getTypeSignature())));
        MethodHandle distinctFromMethodHandle = functionAndTypeManager.getJavaScalarFunctionImplementation(functionAndTypeManager.resolveOperator(IS_DISTINCT_FROM, fromTypes(inputType, inputType))).getMethodHandle();
        return generateAggregation(inputType, outputType, distinctFromMethodHandle);
    }

    private static BuiltInAggregationFunctionImplementation generateAggregation(Type inputType, ArrayType outputType, MethodHandle distinctFromMethodHandle)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(MapAggregationFunction.class.getClassLoader());
        List<Type> inputTypes = ImmutableList.of(inputType);
        SetAggregationStateSerializer stateSerializer = new SetAggregationStateSerializer(inputType, distinctFromMethodHandle);
        Type intermediateType = stateSerializer.getSerializedType();

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, outputType.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(inputType),
                INPUT_FUNCTION.bindTo(inputType).bindTo(distinctFromMethodHandle),
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                ImmutableList.of(new AggregationMetadata.AccumulatorStateDescriptor(
                        SetAggregationState.class,
                        stateSerializer,
                        new SetAggregationStateFactory(inputType))),
                outputType);

        Class<? extends Accumulator> accumulatorClass = AccumulatorCompiler.generateAccumulatorClass(
                Accumulator.class,
                metadata,
                classLoader);
        Class<? extends GroupedAccumulator> groupedAccumulatorClass = AccumulatorCompiler.generateAccumulatorClass(
                GroupedAccumulator.class,
                metadata,
                classLoader);
        return new

                BuiltInAggregationFunctionImplementation(NAME, inputTypes, ImmutableList.of(intermediateType), outputType,
                true, true, metadata, accumulatorClass, groupedAccumulatorClass);
    }

    private static List<AggregationMetadata.ParameterMetadata> createInputParameterMetadata(Type valueType)
    {
        return ImmutableList.of(new AggregationMetadata.ParameterMetadata(STATE),
                new AggregationMetadata.ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, valueType),
                new AggregationMetadata.ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(
            Type type,
            MethodHandle distinctFromMethodHandle,
            SetAggregationState state,
            Block block,
            int position)
    {
        SetOfValues set = state.get();
        if (set == null) {
            set = new SetOfValues(type, distinctFromMethodHandle);
            state.set(set);
        }

        long startSize = set.estimatedInMemorySize();
        set.add(block, position);
        state.addMemoryUsage(set.estimatedInMemorySize() - startSize);
    }

    public static void combine(
            SetAggregationState state,
            SetAggregationState otherState)
    {
        if (state.get() != null && otherState.get() != null) {
            SetOfValues otherSet = otherState.get();
            Block otherValues = otherSet.getvalues();

            SetOfValues set = state.get();
            long startSize = set.estimatedInMemorySize();
            for (int i = 0; i < otherValues.getPositionCount(); i++) {
                set.add(otherValues, i);
            }
            state.addMemoryUsage(set.estimatedInMemorySize() - startSize);
        }
        else if (state.get() == null) {
            state.set(otherState.get());
        }
    }

    public static void output(
            SetAggregationState state,
            BlockBuilder out)
    {
        SetOfValues set = state.get();
        if (set == null) {
            out.appendNull();
        }
        else {
            set.serialize(out);
        }
    }
}
