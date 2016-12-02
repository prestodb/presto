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
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.state.BigIntegerState;
import com.facebook.presto.operator.aggregation.state.BigIntegerStateFactory;
import com.facebook.presto.operator.aggregation.state.BigIntegerStateSerializer;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.lang.invoke.MethodHandle;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import static com.facebook.presto.metadata.SignatureBinder.applyBoundVariables;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.type.Decimals.checkOverflow;
import static com.facebook.presto.spi.type.Decimals.decodeUnscaledValue;
import static com.facebook.presto.spi.type.Decimals.writeBigDecimal;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;

public class DecimalSumAggregation
        extends SqlAggregationFunction
{
    public static final DecimalSumAggregation DECIMAL_SUM_AGGREGATION = new DecimalSumAggregation();
    private static final String NAME = "sum";
    private static final MethodHandle SHORT_DECIMAL_INPUT_FUNCTION = methodHandle(DecimalSumAggregation.class, "inputShortDecimal", Type.class, BigIntegerState.class, Block.class, int.class);
    private static final MethodHandle LONG_DECIMAL_INPUT_FUNCTION = methodHandle(DecimalSumAggregation.class, "inputLongDecimal", Type.class, BigIntegerState.class, Block.class, int.class);

    private static final MethodHandle LONG_DECIMAL_OUTPUT_FUNCTION = methodHandle(DecimalSumAggregation.class, "outputLongDecimal", DecimalType.class, BigIntegerState.class, BlockBuilder.class);

    private static final MethodHandle COMBINE_FUNCTION = methodHandle(DecimalSumAggregation.class, "combine", BigIntegerState.class, BigIntegerState.class);

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
        Class<? extends AccumulatorState> stateInterface = BigIntegerState.class;
        AccumulatorStateSerializer<?> stateSerializer = new BigIntegerStateSerializer();

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
                stateInterface,
                stateSerializer,
                new BigIntegerStateFactory(),
                outputType);

        Type intermediateType = stateSerializer.getSerializedType();
        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, intermediateType, outputType, true, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type type)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(BLOCK_INPUT_CHANNEL, type), new ParameterMetadata(BLOCK_INDEX));
    }

    public static void inputShortDecimal(Type type, BigIntegerState state, Block block, int position)
    {
        accumulateValueInState(BigInteger.valueOf(type.getLong(block, position)), state);
    }

    public static void inputLongDecimal(Type type, BigIntegerState state, Block block, int position)
    {
        accumulateValueInState(decodeUnscaledValue(type.getSlice(block, position)), state);
    }

    private static void accumulateValueInState(BigInteger value, BigIntegerState state)
    {
        initializeIfNeeded(state);
        state.setBigInteger(state.getBigInteger().add(value));
    }

    private static void initializeIfNeeded(BigIntegerState state)
    {
        if (state.getBigInteger() == null) {
            state.setBigInteger(BigInteger.valueOf(0));
        }
    }

    public static void combine(BigIntegerState state, BigIntegerState otherState)
    {
        if (state.getBigInteger() == null) {
            state.setBigInteger(otherState.getBigInteger());
        }
        else {
            state.setBigInteger(state.getBigInteger().add(otherState.getBigInteger()));
        }
    }

    public static void outputLongDecimal(DecimalType type, BigIntegerState state, BlockBuilder out)
    {
        if (state.getBigInteger() == null) {
            out.appendNull();
        }
        else {
            BigDecimal value = new BigDecimal(state.getBigInteger(), type.getScale());
            checkOverflow(state.getBigInteger());
            writeBigDecimal(type, out, value);
        }
    }
}
