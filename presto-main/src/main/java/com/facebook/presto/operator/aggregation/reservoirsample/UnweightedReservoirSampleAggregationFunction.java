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
package com.facebook.presto.operator.aggregation.reservoirsample;

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.AccumulatorCompiler;
import com.facebook.presto.operator.aggregation.AggregationMetadata;
import com.facebook.presto.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import com.facebook.presto.operator.aggregation.GenericAccumulatorFactoryBinder;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.function.Signature.orderableTypeParameter;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class UnweightedReservoirSampleAggregationFunction
        extends SqlAggregationFunction
{
    public static final UnweightedReservoirSampleAggregationFunction RESERVOIR_SAMPLE = new UnweightedReservoirSampleAggregationFunction();

    private static final MethodHandle INT_INPUT_FUNCTION = methodHandle(UnweightedReservoirSampleAggregationFunction.class, "input", UnweightedIntReservoirSampleState.class, int.class);

    private static final MethodHandle INT_COMBINE_FUNCTION = methodHandle(UnweightedReservoirSampleAggregationFunction.class, "combine", UnweightedIntReservoirSampleState.class, UnweightedIntReservoirSampleState.class);

    private static final MethodHandle INT_OUTPUT_FUNCTION = methodHandle(UnweightedIntReservoirSampleState.class, "write", Type.class, UnweightedIntReservoirSampleState.class, BlockBuilder.class);

    protected UnweightedReservoirSampleAggregationFunction()
    {
        super("reservoir_sample",
                ImmutableList.of(orderableTypeParameter("E")),
                ImmutableList.of(),
                parseTypeSignature("array(E)"),
                ImmutableList.of(parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature("E")));
        System.out.println("Created unweighted singleton");
    }

    @Override
    public String getDescription()
    {
        return "Returns a reservoir sample of the argument.";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionManager functionManager)
    {
        Type type = boundVariables.getTypeVariable("E");
        return generateAggregation(type, arity == 3);
    }

    protected InternalAggregationFunction generateAggregation(Type type, boolean weighted)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(WeightedReservoirSampleAggregationFunction.class.getClassLoader());

        List<Type> inputTypes = ImmutableList.of(type);

        MethodHandle inputFunction;
        MethodHandle combineFunction;
        MethodHandle outputFunction;
        Class<? extends AccumulatorState> stateInterface;
        AccumulatorStateSerializer<?> stateSerializer;

        // Tmp Ami
        if (type.getJavaType() == int.class) {
            stateInterface = UnweightedIntReservoirSampleState.class;
            stateSerializer = StateCompiler.generateStateSerializer(stateInterface, classLoader);
            inputFunction = INT_INPUT_FUNCTION;
            combineFunction = INT_COMBINE_FUNCTION;
            outputFunction = INT_OUTPUT_FUNCTION;
        }
        else {
            // Tmp Ami
            stateInterface = UnweightedIntReservoirSampleState.class;
            stateSerializer = StateCompiler.generateStateSerializer(stateInterface, classLoader);
            inputFunction = INT_INPUT_FUNCTION;
            combineFunction = INT_COMBINE_FUNCTION;
            outputFunction = INT_OUTPUT_FUNCTION;
        }

        AccumulatorStateFactory<?> stateFactory = StateCompiler.generateStateFactory(stateInterface, classLoader);

        Type intermediateType = stateSerializer.getSerializedType();
        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(getSignature().getNameSuffix(), type.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createParameterMetadata(type),
                inputFunction,
                combineFunction,
                outputFunction,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        stateInterface,
                        stateSerializer,
                        stateFactory)),
                type);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(getSignature().getNameSuffix(), inputTypes, ImmutableList.of(intermediateType), type, true, false, factory);
    }

    private static List<ParameterMetadata> createParameterMetadata(Type type)
    {
        return ImmutableList.of(
                new ParameterMetadata(STATE),
                new ParameterMetadata(INPUT_CHANNEL, type));
        // Tmp Ami
        /*
        if (type.getJavaType().isPrimitive()) {
            return ImmutableList.of(
                    new ParameterMetadata(STATE),
                    new ParameterMetadata(INPUT_CHANNEL, type));
        }
        else {
            return ImmutableList.of(
                    new ParameterMetadata(STATE),
                    new ParameterMetadata(BLOCK_INPUT_CHANNEL, type),
                    new ParameterMetadata(BLOCK_INDEX));
            return ImmutableList.of(
                    new ParameterMetadata(STATE),
                    new ParameterMetadata(INPUT_CHANNEL, type));
        }
        */
    }

    public static void input(UnweightedIntReservoirSampleState state, int value)
    {
        UnweightedIntReservoirSample reservoir = state.getReservoir();
        reservoir.add(value);
        state.setReservoir(reservoir);
    }

    public static void combine(UnweightedIntReservoirSampleState state, UnweightedIntReservoirSampleState otherState)
    {
        UnweightedIntReservoirSample reservoir = state.getReservoir();
        UnweightedIntReservoirSample otherReservoir = otherState.getReservoir();
        if (state == null && otherState != null) {
            state.setReservoir(otherReservoir);
            return;
        }
        if (otherReservoir == null) {
            return;
        }
        reservoir.mergeWith(otherReservoir);
        state.setReservoir(reservoir);
    }
}
