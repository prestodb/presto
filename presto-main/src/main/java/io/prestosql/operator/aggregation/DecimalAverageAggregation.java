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
import com.google.common.collect.ImmutableSet;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.slice.Slice;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.SqlAggregationFunction;
import io.prestosql.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.prestosql.operator.aggregation.state.LongDecimalWithOverflowAndLongState;
import io.prestosql.operator.aggregation.state.LongDecimalWithOverflowAndLongStateFactory;
import io.prestosql.operator.aggregation.state.LongDecimalWithOverflowAndLongStateSerializer;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AccumulatorState;
import io.prestosql.spi.function.AccumulatorStateSerializer;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.UnscaledDecimal128Arithmetic;

import java.lang.invoke.MethodHandle;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.metadata.SignatureBinder.applyBoundVariables;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static io.prestosql.operator.aggregation.AggregationUtils.generateAggregationName;
import static io.prestosql.spi.type.Decimals.writeBigDecimal;
import static io.prestosql.spi.type.Decimals.writeShortDecimal;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.UnscaledDecimal128Arithmetic.UNSCALED_DECIMAL_128_SLICE_LENGTH;
import static io.prestosql.util.Reflection.methodHandle;
import static java.math.BigDecimal.ROUND_HALF_UP;

public class DecimalAverageAggregation
        extends SqlAggregationFunction
{
    public static final DecimalAverageAggregation DECIMAL_AVERAGE_AGGREGATION = new DecimalAverageAggregation();

    private static final String NAME = "avg";
    private static final MethodHandle SHORT_DECIMAL_INPUT_FUNCTION = methodHandle(DecimalAverageAggregation.class, "inputShortDecimal", Type.class, LongDecimalWithOverflowAndLongState.class, Block.class, int.class);
    private static final MethodHandle LONG_DECIMAL_INPUT_FUNCTION = methodHandle(DecimalAverageAggregation.class, "inputLongDecimal", Type.class, LongDecimalWithOverflowAndLongState.class, Block.class, int.class);

    private static final MethodHandle SHORT_DECIMAL_OUTPUT_FUNCTION = methodHandle(DecimalAverageAggregation.class, "outputShortDecimal", DecimalType.class, LongDecimalWithOverflowAndLongState.class, BlockBuilder.class);
    private static final MethodHandle LONG_DECIMAL_OUTPUT_FUNCTION = methodHandle(DecimalAverageAggregation.class, "outputLongDecimal", DecimalType.class, LongDecimalWithOverflowAndLongState.class, BlockBuilder.class);

    private static final MethodHandle COMBINE_FUNCTION = methodHandle(DecimalAverageAggregation.class, "combine", LongDecimalWithOverflowAndLongState.class, LongDecimalWithOverflowAndLongState.class);

    private static final BigInteger TWO = new BigInteger("2");

    public DecimalAverageAggregation()
    {
        super(NAME,
                ImmutableList.of(),
                ImmutableList.of(),
                parseTypeSignature("decimal(p,s)", ImmutableSet.of("p", "s")),
                ImmutableList.of(parseTypeSignature("decimal(p,s)", ImmutableSet.of("p", "s"))));
    }

    @Override
    public String getDescription()
    {
        return "Calculates the average value";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = typeManager.getType(getOnlyElement(applyBoundVariables(getSignature().getArgumentTypes(), boundVariables)));
        return generateAggregation(type);
    }

    private static InternalAggregationFunction generateAggregation(Type type)
    {
        checkArgument(type instanceof DecimalType, "type must be Decimal");
        DynamicClassLoader classLoader = new DynamicClassLoader(DecimalAverageAggregation.class.getClassLoader());
        List<Type> inputTypes = ImmutableList.of(type);
        MethodHandle inputFunction;
        MethodHandle outputFunction;
        Class<? extends AccumulatorState> stateInterface = LongDecimalWithOverflowAndLongState.class;
        AccumulatorStateSerializer<?> stateSerializer = new LongDecimalWithOverflowAndLongStateSerializer();

        if (((DecimalType) type).isShort()) {
            inputFunction = SHORT_DECIMAL_INPUT_FUNCTION;
            outputFunction = SHORT_DECIMAL_OUTPUT_FUNCTION;
        }
        else {
            inputFunction = LONG_DECIMAL_INPUT_FUNCTION;
            outputFunction = LONG_DECIMAL_OUTPUT_FUNCTION;
        }
        inputFunction = inputFunction.bindTo(type);
        outputFunction = outputFunction.bindTo(type);

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, type.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(type),
                inputFunction,
                COMBINE_FUNCTION,
                outputFunction,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        stateInterface,
                        stateSerializer,
                        new LongDecimalWithOverflowAndLongStateFactory())),
                type);

        Type intermediateType = stateSerializer.getSerializedType();
        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, ImmutableList.of(intermediateType), type, true, false, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type type)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(BLOCK_INPUT_CHANNEL, type), new ParameterMetadata(BLOCK_INDEX));
    }

    public static void inputShortDecimal(Type type, LongDecimalWithOverflowAndLongState state, Block block, int position)
    {
        accumulateValueInState(UnscaledDecimal128Arithmetic.unscaledDecimal(type.getLong(block, position)), state);
    }

    public static void inputLongDecimal(Type type, LongDecimalWithOverflowAndLongState state, Block block, int position)
    {
        accumulateValueInState(type.getSlice(block, position), state);
    }

    private static void accumulateValueInState(Slice unscaledValue, LongDecimalWithOverflowAndLongState state)
    {
        accumulateAndUpdateOverflow(unscaledValue, state);
        state.setLong(state.getLong() + 1);
    }

    private static void initializeIfNeeded(LongDecimalWithOverflowAndLongState state)
    {
        if (state.getLongDecimal() == null) {
            state.setLongDecimal(UnscaledDecimal128Arithmetic.unscaledDecimal());
        }
    }

    public static void combine(LongDecimalWithOverflowAndLongState state, LongDecimalWithOverflowAndLongState otherState)
    {
        state.setLong(state.getLong() + otherState.getLong());
        state.setOverflow(state.getOverflow() + otherState.getOverflow());

        if (state.getLongDecimal() == null) {
            state.setLongDecimal(otherState.getLongDecimal());
        }
        else {
            accumulateAndUpdateOverflow(otherState.getLongDecimal(), state);
        }
    }

    public static void outputShortDecimal(DecimalType type, LongDecimalWithOverflowAndLongState state, BlockBuilder out)
    {
        if (state.getLong() == 0) {
            out.appendNull();
        }
        else {
            writeShortDecimal(out, average(state, type).unscaledValue().longValueExact());
        }
    }

    public static void outputLongDecimal(DecimalType type, LongDecimalWithOverflowAndLongState state, BlockBuilder out)
    {
        if (state.getLong() == 0) {
            out.appendNull();
        }
        else {
            writeBigDecimal(type, out, average(state, type));
        }
    }

    @VisibleForTesting
    public static BigDecimal average(LongDecimalWithOverflowAndLongState state, DecimalType type)
    {
        BigDecimal sum = new BigDecimal(Decimals.decodeUnscaledValue(state.getLongDecimal()), type.getScale());
        BigDecimal count = BigDecimal.valueOf(state.getLong());

        if (state.getOverflow() != 0) {
            BigInteger overflowMultiplier = TWO.shiftLeft(UNSCALED_DECIMAL_128_SLICE_LENGTH * 8 - 2);
            BigInteger overflow = overflowMultiplier.multiply(BigInteger.valueOf(state.getOverflow()));
            sum = sum.add(new BigDecimal(overflow));
        }
        return sum.divide(count, type.getScale(), ROUND_HALF_UP);
    }

    private static void accumulateAndUpdateOverflow(Slice unscaledValue, LongDecimalWithOverflowAndLongState state)
    {
        initializeIfNeeded(state);
        Slice sum = state.getLongDecimal();
        long overflow = UnscaledDecimal128Arithmetic.addWithOverflow(sum, unscaledValue, sum);
        state.setOverflow(state.getOverflow() + overflow);
    }
}
