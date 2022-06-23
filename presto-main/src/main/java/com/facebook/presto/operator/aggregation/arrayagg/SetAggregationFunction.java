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
import com.facebook.presto.operator.aggregation.AggregationMetadata;
import com.facebook.presto.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import com.facebook.presto.operator.aggregation.GenericAccumulatorFactoryBinder;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.operator.aggregation.SetOfValues;
import com.facebook.presto.operator.aggregation.state.SetAggregationState;
import com.facebook.presto.operator.aggregation.state.SetAggregationStateFactory;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class SetAggregationFunction
        extends SqlAggregationFunction
{
    public static final SetAggregationFunction SET_AGG = new SetAggregationFunction();

    private static final String NAME = "set_agg";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(SetAggregationFunction.class, "input", Type.class, SetAggregationState.class, Block.class, int.class);
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
        return "return an array of values";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type type = boundVariables.getTypeVariable("T");
        ArrayType outputType = (ArrayType) functionAndTypeManager.getParameterizedType(StandardTypes.ARRAY, ImmutableList.of(
                TypeSignatureParameter.of(type.getTypeSignature())));
        return generateAggregation(type, outputType);
    }

    @Override
    public boolean isCalledOnNullInput()
    {
        return true;
    }

    private static InternalAggregationFunction generateAggregation(Type type, ArrayType outputType)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(SetAggregationFunction.class.getClassLoader());

        List<Type> inputTypes = ImmutableList.of(type);
        AccumulatorStateSerializer<?> stateSerializer = new SetAggregationStateSerializer(outputType);
        AccumulatorStateFactory<?> stateFactory = new SetAggregationStateFactory(type);

        Type intermediateType = stateSerializer.getSerializedType();
        List<ParameterMetadata> inputParameterMetadata = createInputParameterMetadata(type);
        Class<? extends AccumulatorState> stateInterface = SetAggregationState.class;
        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, outputType.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                inputParameterMetadata,
                INPUT_FUNCTION.bindTo(type),
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        stateInterface,
                        stateSerializer,
                        stateFactory)),
                outputType);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, ImmutableList.of(intermediateType), outputType, true, true, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type value)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, value), new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(Type type, SetAggregationState state, Block block, int position)
    {
        SetOfValues set = state.get();
        if (set == null) {
            set = new SetOfValues(type);
            state.set(set);
        }

        long startSize = set.estimatedInMemorySize();
        set.add(block, position);
        state.addMemoryUsage(set.estimatedInMemorySize() - startSize);
    }

    public static void combine(SetAggregationState state, SetAggregationState otherState)
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

    public static void output(SetAggregationState state, BlockBuilder out)
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
