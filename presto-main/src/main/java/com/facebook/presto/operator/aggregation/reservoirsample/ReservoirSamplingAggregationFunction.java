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
import io.airlift.bytecode.DynamicClassLoader;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.function.Signature.orderableTypeParameter;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class ReservoirSamplingAggregationFunction
        extends SqlAggregationFunction
{
    public static final ReservoirSamplingAggregationFunction RESERVOIR_SAMPLING_AGGREGATION_FUNCTION =
            new ReservoirSamplingAggregationFunction();

    private static final String NAME = "reservoir_sample";

    protected ReservoirSamplingAggregationFunction()
    {
        super(NAME,
                ImmutableList.of(orderableTypeParameter("E")),
                ImmutableList.of(),
                parseTypeSignature("array(E)"),
                ImmutableList.of(parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature("E")));
    }

    @Override
    public String getDescription()
    {
        return "Reservoir (unweighted and weighted) sampling";
    }

    private static final MethodHandle LONG_INPUT_FUNCTION = methodHandle(
            ReservoirSamplingAggregationFunction.class,
            "input",
            ReservoirSampleState.class,
            long.class,
            long.class);
    // private static final MethodHandle DOUBLE_INPUT_FUNCTION = methodHandle(ReservoirSamplingAggregationFunction.class, "input", MethodHandle.class, NullableDoubleState.class, double.class);
    // private static final MethodHandle BOOLEAN_INPUT_FUNCTION = methodHandle(ReservoirSamplingAggregationFunction.class, "input", MethodHandle.class, NullableBooleanState.class, boolean.class);

    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(
            ReservoirSamplingAggregationFunction.class,
            "write",
            ReservoirSampleState.class,
            BlockBuilder.class);
    // private static final MethodHandle DOUBLE_OUTPUT_FUNCTION = methodHandle(NullableDoubleState.class, "write", Type.class, NullableDoubleState.class, BlockBuilder.class);
    // private static final MethodHandle BOOLEAN_OUTPUT_FUNCTION = methodHandle(NullableDoubleState.class, "write", Type.class, NullableDoubleState.class, BlockBuilder.class);

    private static final MethodHandle COMBINE_FUNCTION = methodHandle(
            ReservoirSamplingAggregationFunction.class,
            "combine",
            ReservoirSampleState.class,
            ReservoirSampleState.class);
    // private static final MethodHandle DOUBLE_COMBINE_FUNCTION = methodHandle(ReservoirSamplingAggregationFunction.class, "combine", MethodHandle.class, NullableDoubleState.class, NullableDoubleState.class);
    // private static final MethodHandle BOOLEAN_COMBINE_FUNCTION = methodHandle(ReservoirSamplingAggregationFunction.class, "combine", MethodHandle.class, NullableBooleanState.class, NullableBooleanState.class);

    @Override
    public InternalAggregationFunction specialize(
            BoundVariables boundVariables,
            int arity,
            TypeManager typeManager,
            FunctionManager functionManager)
    {
        Type type = boundVariables.getTypeVariable("E");
        return generateAggregation(type);
    }

    protected InternalAggregationFunction generateAggregation(Type type)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(ReservoirSamplingAggregationFunction.class.getClassLoader());

        List<Type> inputTypes = ImmutableList.of(type);

        MethodHandle inputFunction;
        MethodHandle combineFunction;
        MethodHandle outputFunction;
        Class<? extends AccumulatorState> stateInterface;
        AccumulatorStateSerializer<?> stateSerializer;

        stateInterface = ReservoirSampleState.class;
        stateSerializer = StateCompiler.generateStateSerializer(stateInterface, classLoader);
        inputFunction = LONG_INPUT_FUNCTION;
        combineFunction = COMBINE_FUNCTION;
        outputFunction = OUTPUT_FUNCTION;
        /*
        if (type.getJavaType() == long.class) {
            stateInterface = ReservoirSampleState.class;
            stateSerializer = StateCompiler.generateStateSerializer(stateInterface, classLoader);
            inputFunction = LONG_INPUT_FUNCTION;
            combineFunction = COMBINE_FUNCTION;
            outputFunction = OUTPUT_FUNCTION;
        }
        else if (type.getJavaType() == double.class) {
            stateInterface = ReservoirSampleState.class;
            stateSerializer = StateCompiler.generateStateSerializer(stateInterface, classLoader);
            inputFunction = DOUBLE_INPUT_FUNCTION;
            combineFunction = DOUBLE_COMBINE_FUNCTION;
            outputFunction = DOUBLE_OUTPUT_FUNCTION;
        }
        else if (type.getJavaType() == boolean.class) {
            stateInterface = ReservoirSampleState.class;
            stateSerializer = StateCompiler.generateStateSerializer(stateInterface, classLoader);
            inputFunction = BOOLEAN_INPUT_FUNCTION;
            combineFunction = BOOLEAN_COMBINE_FUNCTION;
            outputFunction = BOOLEAN_OUTPUT_FUNCTION;
        }
        else {
            // native container type is Slice or Block
            stateInterface = BlockPositionState.class;
            stateSerializer = new BlockPositionStateSerializer(type);
            inputFunction = min ? BLOCK_POSITION_MIN_INPUT_FUNCTION.bindTo(type) : BLOCK_POSITION_MAX_INPUT_FUNCTION.bindTo(type);
            combineFunction = min ? BLOCK_POSITION_MIN_COMBINE_FUNCTION.bindTo(type) : BLOCK_POSITION_MAX_COMBINE_FUNCTION.bindTo(type);
            outputFunction = BLOCK_POSITION_OUTPUT_FUNCTION.bindTo(type);
        }
         */

        AccumulatorStateFactory<?> stateFactory = StateCompiler.generateStateFactory(stateInterface, classLoader);

        Type intermediateType = stateSerializer.getSerializedType();
        final List<ParameterMetadata> parametersMetadata = createParameterMetadata(type);
        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(getSignature().getName(), type.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                parametersMetadata,
                inputFunction,
                combineFunction,
                outputFunction,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        stateInterface,
                        stateSerializer,
                        stateFactory)),
                type);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(getSignature().getName(), inputTypes, ImmutableList.of(intermediateType), type, true, false, factory);
    }

    private static List<ParameterMetadata> createParameterMetadata(Type type)
    {
        if (type.getJavaType().isPrimitive()) {
            return ImmutableList.of(
                    new AggregationMetadata.ParameterMetadata(STATE),
                    new AggregationMetadata.ParameterMetadata(INPUT_CHANNEL, BIGINT),
                    new AggregationMetadata.ParameterMetadata(INPUT_CHANNEL, BIGINT));
        }
        else {
            return ImmutableList.of(
                    new ParameterMetadata(STATE),
                    new ParameterMetadata(BLOCK_INPUT_CHANNEL, type),
                    new ParameterMetadata(BLOCK_INDEX));
        }
    }

    public static void input(ReservoirSampleState state, long size, double value)
    {
        updateSampleIfNeeded(state, size, null);
        state.getSample().add(value, null);
    }

    public static void input(ReservoirSampleState state, long size, double value, Double weight)
    {
        updateSampleIfNeeded(state, size, weight);
        state.getSample().add(value, weight);
    }

    public static void input(ReservoirSampleState state, long size, long value)
    {
        updateSampleIfNeeded(state, size, null);
        state.getSample().add(value, null);
    }

    public static void input(ReservoirSampleState state, long size, long value, Double weight)
    {
        updateSampleIfNeeded(state, size, weight);
        state.getSample().add(value, weight);
    }

    public static void input(ReservoirSampleState state, long size, boolean value)
    {
        updateSampleIfNeeded(state, size, null);
        state.getSample().add(value, null);
    }

    public static void input(ReservoirSampleState state, long size, boolean value, Double weight)
    {
        updateSampleIfNeeded(state, size, weight);
        state.getSample().add(value, weight);
    }

    public static void write(ReservoirSampleState state, BlockBuilder out)
    {
        System.out.println("Writing");
        if (state == null || state.getSample() == null) {
            out.appendNull();
            return;
        }
        BlockBuilder entryOut = out.beginBlockEntry();
        state.getSample().write(entryOut);
        out.closeEntry();
    }

    public static void combine(ReservoirSampleState state, ReservoirSampleState otherState)
    {
        if (state.getSample() == null) {
            state.setSample(otherState.getSample());
            return;
        }
        if (otherState.getSample() == null) {
            return;
        }
        ReservoirSampleStateSerializer.combine(state.getSample(), otherState.getSample());
    }

    private static void updateSampleIfNeeded(ReservoirSampleState state, long size, Double weight)
    {
        AbstractReservoirSample sample = state.getSample();
        if (sample == null) {
            sample = ReservoirSampleStateSerializer.create(size, weight);
            state.setSample(sample);
        }
        // Tmp Ami - verify ReservoirSampleStateSerializer.
    }
}
