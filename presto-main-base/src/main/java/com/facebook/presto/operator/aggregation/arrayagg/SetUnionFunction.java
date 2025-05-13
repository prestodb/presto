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

public class SetUnionFunction
        extends SqlAggregationFunction
{
    public static final SetUnionFunction SET_UNION = new SetUnionFunction();
    public static final String NAME = "set_union";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(SetUnionFunction.class, "input", Type.class, ArrayType.class, MethodHandle.class, SetAggregationState.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(SetUnionFunction.class, "combine", SetAggregationState.class, SetAggregationState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(SetUnionFunction.class, "output", SetAggregationState.class, BlockBuilder.class);

    public SetUnionFunction()
    {
        super(NAME,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature("array(T)"),
                ImmutableList.of(parseTypeSignature("array(T)")));
    }

    @Override
    public String getDescription()
    {
        return "Given a column of array type, return an array of all the unique values contained in each of the arrays in the column";
    }

    @Override
    public boolean isCalledOnNullInput()
    {
        return true;
    }

    @Override
    public BuiltInAggregationFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type elementType = boundVariables.getTypeVariable("T");
        ArrayType inputType = (ArrayType) functionAndTypeManager.getParameterizedType(StandardTypes.ARRAY, ImmutableList.of(TypeSignatureParameter.of(elementType.getTypeSignature())));
        ArrayType outputType = (ArrayType) functionAndTypeManager.getParameterizedType(StandardTypes.ARRAY, ImmutableList.of(TypeSignatureParameter.of(elementType.getTypeSignature())));
        MethodHandle distinctFromMethodHandle = functionAndTypeManager.getJavaScalarFunctionImplementation(functionAndTypeManager.resolveOperator(IS_DISTINCT_FROM, fromTypes(elementType, elementType))).getMethodHandle();
        return generateAggregation(elementType, inputType, outputType, distinctFromMethodHandle);
    }

    private static BuiltInAggregationFunctionImplementation generateAggregation(Type elementType, ArrayType inputType, ArrayType outputType, MethodHandle distinctFromMethodHandle)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(MapAggregationFunction.class.getClassLoader());
        List<Type> inputTypes = ImmutableList.of(inputType);
        SetAggregationStateSerializer stateSerializer = new SetAggregationStateSerializer(elementType, distinctFromMethodHandle);
        Type intermediateType = stateSerializer.getSerializedType();

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, outputType.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(inputType),
                INPUT_FUNCTION.bindTo(elementType).bindTo(inputType).bindTo(distinctFromMethodHandle),
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                ImmutableList.of(new AggregationMetadata.AccumulatorStateDescriptor(
                        SetAggregationState.class,
                        stateSerializer,
                        new SetAggregationStateFactory(elementType))),
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
            Type elementType,
            ArrayType arrayType,
            MethodHandle elementIsDistinctFrom,
            SetAggregationState state,
            Block inputBlock,
            int position)
    {
        SetOfValues set = state.get();
        if (set == null) {
            set = new SetOfValues(elementType, elementIsDistinctFrom);
            state.set(set);
        }

        long startSize = set.estimatedInMemorySize();
        Block arrayBlock = arrayType.getObject(inputBlock, position);
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            set.add(arrayBlock, i);
        }
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
