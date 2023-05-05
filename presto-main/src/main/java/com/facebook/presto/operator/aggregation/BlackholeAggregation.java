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
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.state.NullableLongState;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.function.aggregation.Accumulator;
import com.facebook.presto.spi.function.aggregation.AggregationMetadata;
import com.facebook.presto.spi.function.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import com.facebook.presto.spi.function.aggregation.GroupedAccumulator;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.function.Signature.comparableTypeParameter;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;

/**
 * Aggregating function that does nothing on the input data and always returns
 * the "OK" varchar value.
 *
 * It works similar to the CHECKSUM function, but it does not compute the hash to
 * minimize resource usage. It can be used when you need to scan all data in a table.
 */
public class BlackholeAggregation
        extends SqlAggregationFunction
{
    public static final BlackholeAggregation BLACKHOLE_AGGREGATION = new BlackholeAggregation();
    private static final String NAME = "blackhole";
    private static final Slice OUTPUT = utf8Slice("OK");
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(BlackholeAggregation.class, "output", NullableLongState.class, BlockBuilder.class);
    private static final MethodHandle INPUT_FUNCTION = methodHandle(BlackholeAggregation.class, "input", Type.class, NullableLongState.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(BlackholeAggregation.class, "combine", NullableLongState.class, NullableLongState.class);

    public BlackholeAggregation()
    {
        super(NAME,
                ImmutableList.of(comparableTypeParameter("T")),
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.VARBINARY),
                ImmutableList.of(parseTypeSignature("T")));
    }

    @Override
    public String getDescription()
    {
        return "Consumes given values as a blackhole.";
    }

    @Override
    public BuiltInAggregationFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type valueType = boundVariables.getTypeVariable("T");
        return generateAggregation(valueType);
    }

    private static BuiltInAggregationFunctionImplementation generateAggregation(Type type)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(BlackholeAggregation.class.getClassLoader());

        List<Type> inputTypes = ImmutableList.of(type);

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, type.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(type),
                INPUT_FUNCTION.bindTo(type),
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        NullableLongState.class,
                        StateCompiler.generateStateSerializer(NullableLongState.class, classLoader),
                        StateCompiler.generateStateFactory(NullableLongState.class, classLoader))),
                VARBINARY);

        Class<? extends Accumulator> accumulatorClass = AccumulatorCompiler.generateAccumulatorClass(
                Accumulator.class,
                metadata,
                classLoader);
        Class<? extends GroupedAccumulator> groupedAccumulatorClass = AccumulatorCompiler.generateAccumulatorClass(
                GroupedAccumulator.class,
                metadata,
                classLoader);
        return new BuiltInAggregationFunctionImplementation(NAME, inputTypes, ImmutableList.of(BIGINT), VARBINARY,
                true, false, metadata, accumulatorClass, groupedAccumulatorClass);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type type)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, type), new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(Type type, NullableLongState state, Block block, int position)
    {
        // do nothing
    }

    public static void combine(NullableLongState state, NullableLongState otherState)
    {
        // do nothing
    }

    public static void output(NullableLongState state, BlockBuilder out)
    {
        VARBINARY.writeSlice(out, OUTPUT);
    }
}
