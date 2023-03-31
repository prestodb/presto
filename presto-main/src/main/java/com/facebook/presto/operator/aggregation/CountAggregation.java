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
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.state.LongState;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.aggregation.Accumulator;
import com.facebook.presto.spi.function.aggregation.AggregationMetadata;
import com.facebook.presto.spi.function.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import com.facebook.presto.spi.function.aggregation.GroupedAccumulator;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_SIZE;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class CountAggregation
        extends SqlAggregationFunction
{
    public static final CountAggregation COUNT_AGGREGATION = new CountAggregation();
    private static final String NAME = "count";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(CountAggregation.class, "input", LongState.class);
    private static final MethodHandle BLOCK_INPUT_FUNCTION = methodHandle(CountAggregation.class, "blockInput", LongState.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(CountAggregation.class, "combine", LongState.class, LongState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(CountAggregation.class, "output", LongState.class, BlockBuilder.class);

    public CountAggregation()
    {
        super(NAME,
                ImmutableList.of(),
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.BIGINT),
                ImmutableList.of());
    }

    private static BuiltInAggregationFunctionImplementation generateAggregation()
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(CountAggregation.class.getClassLoader());

        AccumulatorStateSerializer<LongState> stateSerializer = StateCompiler.generateStateSerializer(LongState.class, classLoader);
        AccumulatorStateFactory<LongState> stateFactory = StateCompiler.generateStateFactory(LongState.class, classLoader);
        Type intermediateType = stateSerializer.getSerializedType();

        List<Type> inputTypes = ImmutableList.of();

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, BIGINT.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(),
                createBlockInputParameterMetadata(),
                INPUT_FUNCTION,
                BLOCK_INPUT_FUNCTION,
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        LongState.class,
                        stateSerializer,
                        stateFactory)),
                BIGINT);

        Class<? extends Accumulator> accumulatorClass = AccumulatorCompiler.generateAccumulatorClass(
                Accumulator.class,
                metadata,
                classLoader);
        Class<? extends GroupedAccumulator> groupedAccumulatorClass = AccumulatorCompiler.generateAccumulatorClass(
                GroupedAccumulator.class,
                metadata,
                classLoader);
        return new BuiltInAggregationFunctionImplementation(NAME, inputTypes, ImmutableList.of(intermediateType), BIGINT,
                true, false, metadata, accumulatorClass, groupedAccumulatorClass);
    }

    private static List<ParameterMetadata> createInputParameterMetadata()
    {
        return ImmutableList.of(new ParameterMetadata(STATE));
    }

    private static List<ParameterMetadata> createBlockInputParameterMetadata()
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(BLOCK_SIZE));
    }

    public static void input(LongState state)
    {
        state.setLong(state.getLong() + 1);
    }

    public static void blockInput(LongState state, int value)
    {
        state.setLong(state.getLong() + value);
    }

    public static void combine(LongState state, LongState otherState)
    {
        state.setLong(state.getLong() + otherState.getLong());
    }

    public static void output(LongState state, BlockBuilder out)
    {
        BIGINT.writeLong(out, state.getLong());
    }

    @Override
    public String getDescription()
    {
        return "";
    }

    @Override
    public BuiltInAggregationFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        return generateAggregation();
    }

    @Override
    public boolean isCalledOnNullInput()
    {
        return true;
    }
}
