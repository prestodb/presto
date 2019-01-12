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
package io.prestosql.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.DynamicClassLoader;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.SqlAggregationFunction;
import io.prestosql.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.prestosql.operator.aggregation.state.LongState;
import io.prestosql.operator.aggregation.state.StateCompiler;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AccumulatorStateFactory;
import io.prestosql.spi.function.AccumulatorStateSerializer;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static io.prestosql.operator.aggregation.AggregationUtils.generateAggregationName;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.util.Reflection.methodHandle;

public class CountColumn
        extends SqlAggregationFunction
{
    public static final CountColumn COUNT_COLUMN = new CountColumn();
    private static final String NAME = "count";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(CountColumn.class, "input", LongState.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(CountColumn.class, "combine", LongState.class, LongState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(CountColumn.class, "output", LongState.class, BlockBuilder.class);

    public CountColumn()
    {
        super(NAME,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.BIGINT),
                ImmutableList.of(parseTypeSignature("T")));
    }

    @Override
    public String getDescription()
    {
        return "Counts the non-null values";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = boundVariables.getTypeVariable("T");
        return generateAggregation(type);
    }

    private static InternalAggregationFunction generateAggregation(Type type)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(CountColumn.class.getClassLoader());

        AccumulatorStateSerializer<LongState> stateSerializer = StateCompiler.generateStateSerializer(LongState.class, classLoader);
        AccumulatorStateFactory<LongState> stateFactory = StateCompiler.generateStateFactory(LongState.class, classLoader);
        Type intermediateType = stateSerializer.getSerializedType();

        List<Type> inputTypes = ImmutableList.of(type);

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, BIGINT.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(type),
                INPUT_FUNCTION,
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        LongState.class,
                        stateSerializer,
                        stateFactory)),
                BIGINT);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, ImmutableList.of(intermediateType), BIGINT, true, false, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type type)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(BLOCK_INPUT_CHANNEL, type), new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(LongState state, Block block, int index)
    {
        state.setLong(state.getLong() + 1);
    }

    public static void combine(LongState state, LongState otherState)
    {
        state.setLong(state.getLong() + otherState.getLong());
    }

    public static void output(LongState state, BlockBuilder out)
    {
        BIGINT.writeLong(out, state.getLong());
    }
}
