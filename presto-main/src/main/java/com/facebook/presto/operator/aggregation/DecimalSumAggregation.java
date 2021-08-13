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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.UnscaledDecimal128Arithmetic;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import com.facebook.presto.operator.aggregation.state.LongDecimalWithOverflowState;
import com.facebook.presto.operator.aggregation.state.LongDecimalWithOverflowStateFactory;
import com.facebook.presto.operator.aggregation.state.LongDecimalWithOverflowStateSerializer;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.FunctionKind;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.type.Decimals.MAX_PRECISION;
import static com.facebook.presto.common.type.Decimals.MAX_SHORT_PRECISION;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.throwIfOverflows;
import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.throwOverflowException;
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

public class DecimalSumAggregation
        extends SqlAggregationFunction
{
    // Constant references for short/long decimal types for use in operations that only manipulate unscaled values
    private static final DecimalType LONG_DECIMAL_TYPE = DecimalType.createDecimalType(MAX_PRECISION, 0);
    private static final DecimalType SHORT_DECIMAL_TYPE = DecimalType.createDecimalType(MAX_SHORT_PRECISION, 0);

    public static final DecimalSumAggregation DECIMAL_SUM_AGGREGATION = new DecimalSumAggregation();
    private static final String NAME = "sum";
    private static final MethodHandle SHORT_DECIMAL_INPUT_FUNCTION = methodHandle(DecimalSumAggregation.class, "inputShortDecimal", LongDecimalWithOverflowState.class, Block.class, int.class);
    private static final MethodHandle LONG_DECIMAL_INPUT_FUNCTION = methodHandle(DecimalSumAggregation.class, "inputLongDecimal", LongDecimalWithOverflowState.class, Block.class, int.class);

    private static final MethodHandle LONG_DECIMAL_OUTPUT_FUNCTION = methodHandle(DecimalSumAggregation.class, "outputLongDecimal", LongDecimalWithOverflowState.class, BlockBuilder.class);

    private static final MethodHandle COMBINE_FUNCTION = methodHandle(DecimalSumAggregation.class, "combine", LongDecimalWithOverflowState.class, LongDecimalWithOverflowState.class);

    public DecimalSumAggregation()
    {
        super(NAME,
                ImmutableList.of(),
                ImmutableList.of(),
                parseTypeSignature("decimal(38,s)", ImmutableSet.of("s")),
                ImmutableList.of(parseTypeSignature("decimal(p,s)", ImmutableSet.of("p", "s"))),
                FunctionKind.AGGREGATE);
    }

    @Override
    public String getDescription()
    {
        return "Calculates the sum over the input values";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type inputType = getOnlyElement(applyBoundVariables(functionAndTypeManager, getSignature().getArgumentTypes(), boundVariables));
        Type outputType = applyBoundVariables(functionAndTypeManager, getSignature().getReturnType(), boundVariables);
        return generateAggregation(inputType, outputType);
    }

    private static InternalAggregationFunction generateAggregation(Type inputType, Type outputType)
    {
        checkArgument(inputType instanceof DecimalType, "type must be Decimal");
        DynamicClassLoader classLoader = new DynamicClassLoader(DecimalSumAggregation.class.getClassLoader());
        List<Type> inputTypes = ImmutableList.of(inputType);
        MethodHandle inputFunction;
        Class<? extends AccumulatorState> stateInterface = LongDecimalWithOverflowState.class;
        AccumulatorStateSerializer<?> stateSerializer = new LongDecimalWithOverflowStateSerializer();

        if (((DecimalType) inputType).isShort()) {
            inputFunction = SHORT_DECIMAL_INPUT_FUNCTION;
        }
        else {
            inputFunction = LONG_DECIMAL_INPUT_FUNCTION;
        }

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, outputType.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(inputType),
                inputFunction,
                COMBINE_FUNCTION,
                LONG_DECIMAL_OUTPUT_FUNCTION,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        stateInterface,
                        stateSerializer,
                        new LongDecimalWithOverflowStateFactory())),
                outputType);

        Type intermediateType = stateSerializer.getSerializedType();
        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, ImmutableList.of(intermediateType), outputType, true, false, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type type)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(BLOCK_INPUT_CHANNEL, type), new ParameterMetadata(BLOCK_INDEX));
    }

    public static void inputShortDecimal(LongDecimalWithOverflowState state, Block block, int position)
    {
        Slice currentSum = state.getLongDecimal();
        if (currentSum == null) {
            currentSum = unscaledDecimal();
            state.setLongDecimal(currentSum);
        }
        state.addOverflow(UnscaledDecimal128Arithmetic.addWithOverflow(currentSum, unscaledDecimal(SHORT_DECIMAL_TYPE.getLong(block, position)), currentSum));
    }

    public static void inputLongDecimal(LongDecimalWithOverflowState state, Block block, int position)
    {
        Slice currentSum = state.getLongDecimal();
        if (currentSum == null) {
            currentSum = unscaledDecimal();
            state.setLongDecimal(currentSum);
        }
        state.addOverflow(UnscaledDecimal128Arithmetic.addWithOverflow(currentSum, LONG_DECIMAL_TYPE.getSlice(block, position), currentSum));
    }

    public static void combine(LongDecimalWithOverflowState state, LongDecimalWithOverflowState otherState)
    {
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

    public static void outputLongDecimal(LongDecimalWithOverflowState state, BlockBuilder out)
    {
        Slice decimal = state.getLongDecimal();
        if (decimal == null) {
            out.appendNull();
        }
        else {
            if (state.getOverflow() != 0) {
                throwOverflowException();
            }
            throwIfOverflows(decimal);
            LONG_DECIMAL_TYPE.writeSlice(out, decimal);
        }
    }
}
