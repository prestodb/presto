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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.DynamicClassLoader;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.SqlAggregationFunction;
import io.prestosql.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.prestosql.operator.aggregation.state.NullableLongState;
import io.prestosql.operator.aggregation.state.StateCompiler;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.wrappedLongArray;
import static io.prestosql.metadata.Signature.comparableTypeParameter;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static io.prestosql.operator.aggregation.AggregationUtils.generateAggregationName;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.util.Reflection.methodHandle;

public class ChecksumAggregationFunction
        extends SqlAggregationFunction
{
    public static final ChecksumAggregationFunction CHECKSUM_AGGREGATION = new ChecksumAggregationFunction();
    @VisibleForTesting
    public static final long PRIME64 = 0x9E3779B185EBCA87L;
    private static final String NAME = "checksum";
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(ChecksumAggregationFunction.class, "output", NullableLongState.class, BlockBuilder.class);
    private static final MethodHandle INPUT_FUNCTION = methodHandle(ChecksumAggregationFunction.class, "input", Type.class, NullableLongState.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(ChecksumAggregationFunction.class, "combine", NullableLongState.class, NullableLongState.class);

    public ChecksumAggregationFunction()
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
        return "Checksum of the given values";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type valueType = boundVariables.getTypeVariable("T");
        return generateAggregation(valueType);
    }

    private static InternalAggregationFunction generateAggregation(Type type)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(ChecksumAggregationFunction.class.getClassLoader());

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

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, ImmutableList.of(BIGINT), VARBINARY, true, false, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type type)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, type), new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(Type type, NullableLongState state, Block block, int position)
    {
        state.setNull(false);
        if (block.isNull(position)) {
            state.setLong(state.getLong() + PRIME64);
        }
        else {
            state.setLong(state.getLong() + type.hash(block, position) * PRIME64);
        }
    }

    public static void combine(NullableLongState state, NullableLongState otherState)
    {
        state.setNull(state.isNull() && otherState.isNull());
        state.setLong(state.getLong() + otherState.getLong());
    }

    public static void output(NullableLongState state, BlockBuilder out)
    {
        if (state.isNull()) {
            out.appendNull();
        }
        else {
            VARBINARY.writeSlice(out, wrappedLongArray(state.getLong()));
        }
    }
}
