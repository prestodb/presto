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
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.UnscaledDecimal128Arithmetic;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import com.facebook.presto.operator.aggregation.state.LongDecimalWithOverflowAndLongState;
import com.facebook.presto.operator.aggregation.state.LongDecimalWithOverflowAndLongStateFactory;
import com.facebook.presto.operator.aggregation.state.LongDecimalWithOverflowAndLongStateSerializer;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import static com.facebook.presto.common.type.Decimals.MAX_PRECISION;
import static com.facebook.presto.common.type.Decimals.MAX_SHORT_PRECISION;
import static com.facebook.presto.common.type.Decimals.writeBigDecimal;
import static com.facebook.presto.common.type.Decimals.writeShortDecimal;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.UNSCALED_DECIMAL_128_SLICE_LENGTH;
import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.unscaledDecimal;
import static com.facebook.presto.metadata.SignatureBinder.applyBoundVariables;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.math.BigDecimal.ROUND_HALF_UP;

public class DecimalAverageAggregation
        extends SqlAggregationFunction
{
    // Constant references for short/long decimal types for use in operations that only manipulate unscaled values
    private static final DecimalType LONG_DECIMAL_TYPE = DecimalType.createDecimalType(MAX_PRECISION, 0);
    private static final DecimalType SHORT_DECIMAL_TYPE = DecimalType.createDecimalType(MAX_SHORT_PRECISION, 0);

    public static final DecimalAverageAggregation DECIMAL_AVERAGE_AGGREGATION = new DecimalAverageAggregation();

    private static final String NAME = "avg";
    private static final MethodHandle SHORT_DECIMAL_INPUT_FUNCTION = methodHandle(DecimalAverageAggregation.class, "inputShortDecimal", LongDecimalWithOverflowAndLongState.class, Block.class, int.class);
    private static final MethodHandle LONG_DECIMAL_INPUT_FUNCTION = methodHandle(DecimalAverageAggregation.class, "inputLongDecimal", LongDecimalWithOverflowAndLongState.class, Block.class, int.class);

    private static final MethodHandle SHORT_DECIMAL_OUTPUT_FUNCTION = methodHandle(DecimalAverageAggregation.class, "outputShortDecimal", DecimalType.class, LongDecimalWithOverflowAndLongState.class, BlockBuilder.class);
    private static final MethodHandle LONG_DECIMAL_OUTPUT_FUNCTION = methodHandle(DecimalAverageAggregation.class, "outputLongDecimal", DecimalType.class, LongDecimalWithOverflowAndLongState.class, BlockBuilder.class);

    private static final MethodHandle COMBINE_FUNCTION = methodHandle(DecimalAverageAggregation.class, "combine", LongDecimalWithOverflowAndLongState.class, LongDecimalWithOverflowAndLongState.class);

    private static final BigInteger OVERFLOW_MULTIPLIER = new BigInteger("2").shiftLeft(UNSCALED_DECIMAL_128_SLICE_LENGTH * 8 - 2);

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
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type type = getOnlyElement(applyBoundVariables(functionAndTypeManager, getSignature().getArgumentTypes(), boundVariables));
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

    public static void inputShortDecimal(LongDecimalWithOverflowAndLongState state, Block block, int position)
    {
        state.incrementLong();
        Slice currentSum = state.getLongDecimal();
        if (currentSum == null) {
            currentSum = unscaledDecimal();
            state.setLongDecimal(currentSum);
        }
        state.addOverflow(UnscaledDecimal128Arithmetic.addWithOverflow(currentSum, unscaledDecimal(SHORT_DECIMAL_TYPE.getLong(block, position)), currentSum));
    }

    public static void inputLongDecimal(LongDecimalWithOverflowAndLongState state, Block block, int position)
    {
        state.incrementLong();
        Slice currentSum = state.getLongDecimal();
        if (currentSum == null) {
            currentSum = unscaledDecimal();
            state.setLongDecimal(currentSum);
        }
        state.addOverflow(UnscaledDecimal128Arithmetic.addWithOverflow(currentSum, LONG_DECIMAL_TYPE.getSlice(block, position), currentSum));
    }

    public static void combine(LongDecimalWithOverflowAndLongState state, LongDecimalWithOverflowAndLongState otherState)
    {
        state.addLong(otherState.getLong());
        long overflowToAdd = otherState.getOverflow();
        Slice currentState = state.getLongDecimal();
        Slice otherDecimal = otherState.getLongDecimal();
        if (currentState == null) {
            state.setLongDecimal(otherDecimal);
        }
        else {
            overflowToAdd += UnscaledDecimal128Arithmetic.addWithOverflow(currentState, otherDecimal, currentState);
        }
        state.addOverflow(overflowToAdd);
    }

    public static void outputShortDecimal(DecimalType type, LongDecimalWithOverflowAndLongState state, BlockBuilder out)
    {
        long count = state.getLong();
        if (count == 0) {
            out.appendNull();
        }
        else {
            writeShortDecimal(out, average(count, state.getOverflow(), state.getLongDecimal(), type.getScale()).unscaledValue().longValueExact());
        }
    }

    public static void outputLongDecimal(DecimalType type, LongDecimalWithOverflowAndLongState state, BlockBuilder out)
    {
        long count = state.getLong();
        if (count == 0) {
            out.appendNull();
        }
        else {
            writeBigDecimal(type, out, average(count, state.getOverflow(), state.getLongDecimal(), type.getScale()));
        }
    }

    @VisibleForTesting
    public static BigDecimal average(LongDecimalWithOverflowAndLongState state, DecimalType type)
    {
        return average(state.getLong(), state.getOverflow(), state.getLongDecimal(), type.getScale());
    }

    private static BigDecimal average(long count, long overflow, Slice unscaledDecimal, int scale)
    {
        BigDecimal sum = new BigDecimal(Decimals.decodeUnscaledValue(unscaledDecimal), scale);
        if (overflow != 0) {
            sum = sum.add(new BigDecimal(OVERFLOW_MULTIPLIER.multiply(BigInteger.valueOf(overflow))));
        }
        return sum.divide(BigDecimal.valueOf(count), scale, ROUND_HALF_UP);
    }
}
