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
import com.google.common.collect.ImmutableSet;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.slice.Slice;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.SqlAggregationFunction;
import io.prestosql.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.prestosql.operator.aggregation.state.LongDecimalWithOverflowState;
import io.prestosql.operator.aggregation.state.LongDecimalWithOverflowStateFactory;
import io.prestosql.operator.aggregation.state.LongDecimalWithOverflowStateSerializer;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AccumulatorState;
import io.prestosql.spi.function.AccumulatorStateSerializer;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.UnscaledDecimal128Arithmetic;

import java.lang.invoke.MethodHandle;
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
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.UnscaledDecimal128Arithmetic.throwIfOverflows;
import static io.prestosql.spi.type.UnscaledDecimal128Arithmetic.throwOverflowException;
import static io.prestosql.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimal;
import static io.prestosql.util.Reflection.methodHandle;

public class DecimalSumAggregation
        extends SqlAggregationFunction
{
    public static final DecimalSumAggregation DECIMAL_SUM_AGGREGATION = new DecimalSumAggregation();
    private static final String NAME = "sum";
    private static final MethodHandle SHORT_DECIMAL_INPUT_FUNCTION = methodHandle(DecimalSumAggregation.class, "inputShortDecimal", Type.class, LongDecimalWithOverflowState.class, Block.class, int.class);
    private static final MethodHandle LONG_DECIMAL_INPUT_FUNCTION = methodHandle(DecimalSumAggregation.class, "inputLongDecimal", Type.class, LongDecimalWithOverflowState.class, Block.class, int.class);

    private static final MethodHandle LONG_DECIMAL_OUTPUT_FUNCTION = methodHandle(DecimalSumAggregation.class, "outputLongDecimal", DecimalType.class, LongDecimalWithOverflowState.class, BlockBuilder.class);

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
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type inputType = typeManager.getType(getOnlyElement(applyBoundVariables(getSignature().getArgumentTypes(), boundVariables)));
        Type outputType = typeManager.getType(applyBoundVariables(getSignature().getReturnType(), boundVariables));
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
                inputFunction.bindTo(inputType),
                COMBINE_FUNCTION,
                LONG_DECIMAL_OUTPUT_FUNCTION.bindTo(outputType),
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

    public static void inputShortDecimal(Type type, LongDecimalWithOverflowState state, Block block, int position)
    {
        accumulateValueInState(unscaledDecimal(type.getLong(block, position)), state);
    }

    public static void inputLongDecimal(Type type, LongDecimalWithOverflowState state, Block block, int position)
    {
        accumulateValueInState(type.getSlice(block, position), state);
    }

    private static void accumulateValueInState(Slice unscaledDecimal, LongDecimalWithOverflowState state)
    {
        initializeIfNeeded(state);
        Slice sum = state.getLongDecimal();
        long overflow = UnscaledDecimal128Arithmetic.addWithOverflow(sum, unscaledDecimal, sum);
        state.setOverflow(state.getOverflow() + overflow);
    }

    private static void initializeIfNeeded(LongDecimalWithOverflowState state)
    {
        if (state.getLongDecimal() == null) {
            state.setLongDecimal(UnscaledDecimal128Arithmetic.unscaledDecimal());
        }
    }

    public static void combine(LongDecimalWithOverflowState state, LongDecimalWithOverflowState otherState)
    {
        state.setOverflow(state.getOverflow() + otherState.getOverflow());

        if (state.getLongDecimal() == null) {
            state.setLongDecimal(otherState.getLongDecimal());
        }
        else {
            accumulateValueInState(otherState.getLongDecimal(), state);
        }
    }

    public static void outputLongDecimal(DecimalType type, LongDecimalWithOverflowState state, BlockBuilder out)
    {
        if (state.getLongDecimal() == null) {
            out.appendNull();
        }
        else {
            if (state.getOverflow() != 0) {
                throwOverflowException();
            }
            throwIfOverflows(state.getLongDecimal());
            type.writeSlice(out, state.getLongDecimal());
        }
    }
}
