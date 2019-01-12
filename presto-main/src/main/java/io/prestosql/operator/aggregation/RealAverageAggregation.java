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
import io.prestosql.operator.aggregation.state.DoubleState;
import io.prestosql.operator.aggregation.state.LongState;
import io.prestosql.operator.aggregation.state.StateCompiler;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AccumulatorState;
import io.prestosql.spi.function.AccumulatorStateSerializer;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static io.prestosql.operator.aggregation.AggregationUtils.generateAggregationName;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.util.Reflection.methodHandle;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;

public class RealAverageAggregation
        extends SqlAggregationFunction
{
    public static final RealAverageAggregation REAL_AVERAGE_AGGREGATION = new RealAverageAggregation();
    private static final String NAME = "avg";

    private static final MethodHandle INPUT_FUNCTION = methodHandle(RealAverageAggregation.class, "input", LongState.class, DoubleState.class, long.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(RealAverageAggregation.class, "combine", LongState.class, DoubleState.class, LongState.class, DoubleState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(RealAverageAggregation.class, "output", LongState.class, DoubleState.class, BlockBuilder.class);

    protected RealAverageAggregation()
    {
        super(NAME,
                ImmutableList.of(),
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.REAL),
                ImmutableList.of(parseTypeSignature(StandardTypes.REAL)));
    }

    @Override
    public String getDescription()
    {
        return "Returns the average value of the argument";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(AverageAggregations.class.getClassLoader());
        Class<? extends AccumulatorState> longStateInterface = LongState.class;
        Class<? extends AccumulatorState> doubleStateInterface = DoubleState.class;
        AccumulatorStateSerializer<?> longStateSerializer = StateCompiler.generateStateSerializer(longStateInterface, classLoader);
        AccumulatorStateSerializer<?> doubleStateSerializer = StateCompiler.generateStateSerializer(doubleStateInterface, classLoader);

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, parseTypeSignature(StandardTypes.REAL), ImmutableList.of(parseTypeSignature(StandardTypes.REAL))),
                ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(STATE), new ParameterMetadata(INPUT_CHANNEL, REAL)),
                INPUT_FUNCTION,
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                ImmutableList.of(
                        new AccumulatorStateDescriptor(
                                longStateInterface,
                                longStateSerializer,
                                StateCompiler.generateStateFactory(longStateInterface, classLoader)),
                        new AccumulatorStateDescriptor(
                                doubleStateInterface,
                                doubleStateSerializer,
                                StateCompiler.generateStateFactory(doubleStateInterface, classLoader))),
                REAL);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(
                NAME,
                ImmutableList.of(REAL),
                ImmutableList.of(
                        longStateSerializer.getSerializedType(),
                        doubleStateSerializer.getSerializedType()),
                REAL,
                true,
                false,
                factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type value)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(BLOCK_INPUT_CHANNEL, value), new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(LongState count, DoubleState sum, long value)
    {
        count.setLong(count.getLong() + 1);
        sum.setDouble(sum.getDouble() + intBitsToFloat((int) value));
    }

    public static void combine(LongState count, DoubleState sum, LongState otherCount, DoubleState otherSum)
    {
        count.setLong(count.getLong() + otherCount.getLong());
        sum.setDouble(sum.getDouble() + otherSum.getDouble());
    }

    public static void output(LongState count, DoubleState sum, BlockBuilder out)
    {
        if (count.getLong() == 0) {
            out.appendNull();
        }
        else {
            REAL.writeLong(out, floatToIntBits((float) (sum.getDouble() / count.getLong())));
        }
    }
}
