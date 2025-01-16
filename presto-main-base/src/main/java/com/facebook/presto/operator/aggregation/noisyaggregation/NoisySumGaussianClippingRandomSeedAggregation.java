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
package com.facebook.presto.operator.aggregation.noisyaggregation;

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.AccumulatorCompiler;
import com.facebook.presto.operator.aggregation.BuiltInAggregationFunctionImplementation;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.aggregation.Accumulator;
import com.facebook.presto.spi.function.aggregation.AggregationMetadata;
import com.facebook.presto.spi.function.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import com.facebook.presto.spi.function.aggregation.GroupedAccumulator;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.Decimals.MAX_PRECISION;
import static com.facebook.presto.common.type.Decimals.MAX_SHORT_PRECISION;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.unscaledDecimal;
import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.unscaledDecimalToBigInteger;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.operator.aggregation.noisyaggregation.NoisyCountAndSumAggregationUtils.combineStates;
import static com.facebook.presto.operator.aggregation.noisyaggregation.NoisyCountAndSumAggregationUtils.updateState;
import static com.facebook.presto.operator.aggregation.noisyaggregation.NoisyCountAndSumAggregationUtils.writeNoisySumOutput;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Float.intBitsToFloat;

public class NoisySumGaussianClippingRandomSeedAggregation
        extends SqlAggregationFunction
{
    // Constant references for short/long decimal types for use in operations that only manipulate unscaled values
    private static final DecimalType LONG_DECIMAL_TYPE = DecimalType.createDecimalType(MAX_PRECISION, 0);
    private static final DecimalType SHORT_DECIMAL_TYPE = DecimalType.createDecimalType(MAX_SHORT_PRECISION, 0);

    public static final NoisySumGaussianClippingRandomSeedAggregation NOISY_SUM_GAUSSIAN_CLIPPING_RANDOM_SEED_AGGREGATION = new NoisySumGaussianClippingRandomSeedAggregation();
    private static final String NAME = "noisy_sum_gaussian";
    private static final MethodHandle SHORT_DECIMAL_INPUT_FUNCTION = methodHandle(NoisySumGaussianClippingRandomSeedAggregation.class, "inputShortDecimal", NoisyCountAndSumState.class, Block.class, Block.class, Block.class, Block.class, Block.class, int.class);
    private static final MethodHandle LONG_DECIMAL_INPUT_FUNCTION = methodHandle(NoisySumGaussianClippingRandomSeedAggregation.class, "inputLongDecimal", NoisyCountAndSumState.class, Block.class, Block.class, Block.class, Block.class, Block.class, int.class);
    private static final MethodHandle DOUBLE_INPUT_FUNCTION = methodHandle(NoisySumGaussianClippingRandomSeedAggregation.class, "inputDouble", NoisyCountAndSumState.class, Block.class, Block.class, Block.class, Block.class, Block.class, int.class);
    private static final MethodHandle REAL_INPUT_FUNCTION = methodHandle(NoisySumGaussianClippingRandomSeedAggregation.class, "inputReal", NoisyCountAndSumState.class, Block.class, Block.class, Block.class, Block.class, Block.class, int.class);
    private static final MethodHandle BIGINT_INPUT_FUNCTION = methodHandle(NoisySumGaussianClippingRandomSeedAggregation.class, "inputBigInt", NoisyCountAndSumState.class, Block.class, Block.class, Block.class, Block.class, Block.class, int.class);
    private static final MethodHandle INTEGER_INPUT_FUNCTION = methodHandle(NoisySumGaussianClippingRandomSeedAggregation.class, "inputInteger", NoisyCountAndSumState.class, Block.class, Block.class, Block.class, Block.class, Block.class, int.class);
    private static final MethodHandle SMALLINT_INPUT_FUNCTION = methodHandle(NoisySumGaussianClippingRandomSeedAggregation.class, "inputSmallInt", NoisyCountAndSumState.class, Block.class, Block.class, Block.class, Block.class, Block.class, int.class);
    private static final MethodHandle TINYINT_INPUT_FUNCTION = methodHandle(NoisySumGaussianClippingRandomSeedAggregation.class, "inputTinyInt", NoisyCountAndSumState.class, Block.class, Block.class, Block.class, Block.class, Block.class, int.class);

    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(NoisySumGaussianClippingRandomSeedAggregation.class, "output", NoisyCountAndSumState.class, BlockBuilder.class);

    private static final MethodHandle COMBINE_FUNCTION = methodHandle(NoisySumGaussianClippingRandomSeedAggregation.class, "combine", NoisyCountAndSumState.class, NoisyCountAndSumState.class);

    public NoisySumGaussianClippingRandomSeedAggregation()
    {
        super(NAME,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.DOUBLE),
                ImmutableList.of(parseTypeSignature("T"),
                        parseTypeSignature(StandardTypes.DOUBLE),
                        parseTypeSignature(StandardTypes.DOUBLE),
                        parseTypeSignature(StandardTypes.DOUBLE),
                        parseTypeSignature(StandardTypes.BIGINT)),
                FunctionKind.AGGREGATE);
    }

    @Override
    public String getDescription()
    {
        return "Calculates the sum over the input values where values are clipped to [lower, upper] range and then adds random Gaussian noise.  Random seed is used to seed random generator. This method does not use a secure random.";
    }

    @Override
    public BuiltInAggregationFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type type = boundVariables.getTypeVariable("T");
        return generateAggregation(type);
    }

    private static BuiltInAggregationFunctionImplementation generateAggregation(Type type)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(NoisySumGaussianClippingRandomSeedAggregation.class.getClassLoader());

        AccumulatorStateSerializer<?> stateSerializer = new NoisyCountAndSumStateSerializer();
        AccumulatorStateFactory<?> stateFactory = StateCompiler.generateStateFactory(NoisyCountAndSumState.class, classLoader);
        List<Type> inputTypes = ImmutableList.of(type, DOUBLE, DOUBLE, DOUBLE, BIGINT);

        MethodHandle inputFunction;
        if (type instanceof DecimalType) {
            inputFunction = ((DecimalType) type).isShort() ? SHORT_DECIMAL_INPUT_FUNCTION : LONG_DECIMAL_INPUT_FUNCTION;
        }
        else if (type instanceof TinyintType) {
            inputFunction = TINYINT_INPUT_FUNCTION;
        }
        else if (type instanceof SmallintType) {
            inputFunction = SMALLINT_INPUT_FUNCTION;
        }
        else if (type instanceof IntegerType) {
            inputFunction = INTEGER_INPUT_FUNCTION;
        }
        else if (type instanceof BigintType) {
            inputFunction = BIGINT_INPUT_FUNCTION;
        }
        else if (type instanceof RealType) {
            inputFunction = REAL_INPUT_FUNCTION;
        }
        else {
            inputFunction = DOUBLE_INPUT_FUNCTION;
        }

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, DOUBLE.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(type),
                inputFunction,
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        NoisyCountAndSumState.class,
                        stateSerializer,
                        stateFactory)),
                DOUBLE);

        Type intermediateType = stateSerializer.getSerializedType();

        Class<? extends Accumulator> accumulatorClass = AccumulatorCompiler.generateAccumulatorClass(
                Accumulator.class,
                metadata,
                classLoader);
        Class<? extends GroupedAccumulator> groupedAccumulatorClass = AccumulatorCompiler.generateAccumulatorClass(
                GroupedAccumulator.class,
                metadata,
                classLoader);
        return new BuiltInAggregationFunctionImplementation(NAME, inputTypes, ImmutableList.of(intermediateType), DOUBLE,
                true, false, metadata, accumulatorClass, groupedAccumulatorClass);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type type)
    {
        return ImmutableList.of(
                new ParameterMetadata(STATE),
                new ParameterMetadata(BLOCK_INPUT_CHANNEL, type),
                new ParameterMetadata(BLOCK_INPUT_CHANNEL, DOUBLE),
                new ParameterMetadata(BLOCK_INPUT_CHANNEL, DOUBLE),
                new ParameterMetadata(BLOCK_INPUT_CHANNEL, DOUBLE),
                new ParameterMetadata(BLOCK_INPUT_CHANNEL, BIGINT),
                new ParameterMetadata(BLOCK_INDEX));
    }

    public static void inputShortDecimal(NoisyCountAndSumState state, Block valueBlock, Block noiseScaleBlock, Block lowerBlock, Block upperBlock, Block randomSeedBlock, int position)
    {
        double value = unscaledDecimalToBigInteger(
                unscaledDecimal(SHORT_DECIMAL_TYPE.getLong(valueBlock, position)))
                .doubleValue();
        input(state, value, noiseScaleBlock, lowerBlock, upperBlock, randomSeedBlock, position);
    }

    public static void inputLongDecimal(NoisyCountAndSumState state, Block valueBlock, Block noiseScaleBlock, Block lowerBlock, Block upperBlock, Block randomSeedBlock, int position)
    {
        double value = unscaledDecimalToBigInteger(
                unscaledDecimal(LONG_DECIMAL_TYPE.getSlice(valueBlock, position)))
                .doubleValue();
        input(state, value, noiseScaleBlock, lowerBlock, upperBlock, randomSeedBlock, position);
    }

    public static void inputDouble(NoisyCountAndSumState state, Block valueBlock, Block noiseScaleBlock, Block lowerBlock, Block upperBlock, Block randomSeedBlock, int position)
    {
        double value = DOUBLE.getDouble(valueBlock, position);
        input(state, value, noiseScaleBlock, lowerBlock, upperBlock, randomSeedBlock, position);
    }

    public static void inputReal(NoisyCountAndSumState state, Block valueBlock, Block noiseScaleBlock, Block lowerBlock, Block upperBlock, Block randomSeedBlock, int position)
    {
        double value = intBitsToFloat((int) REAL.getLong(valueBlock, position));
        input(state, value, noiseScaleBlock, lowerBlock, upperBlock, randomSeedBlock, position);
    }

    public static void inputBigInt(NoisyCountAndSumState state, Block valueBlock, Block noiseScaleBlock, Block lowerBlock, Block upperBlock, Block randomSeedBlock, int position)
    {
        double value = BIGINT.getLong(valueBlock, position);
        input(state, value, noiseScaleBlock, lowerBlock, upperBlock, randomSeedBlock, position);
    }

    public static void inputInteger(NoisyCountAndSumState state, Block valueBlock, Block noiseScaleBlock, Block lowerBlock, Block upperBlock, Block randomSeedBlock, int position)
    {
        double value = INTEGER.getLong(valueBlock, position);
        input(state, value, noiseScaleBlock, lowerBlock, upperBlock, randomSeedBlock, position);
    }

    public static void inputSmallInt(NoisyCountAndSumState state, Block valueBlock, Block noiseScaleBlock, Block lowerBlock, Block upperBlock, Block randomSeedBlock, int position)
    {
        double value = SMALLINT.getLong(valueBlock, position);
        input(state, value, noiseScaleBlock, lowerBlock, upperBlock, randomSeedBlock, position);
    }

    public static void inputTinyInt(NoisyCountAndSumState state, Block valueBlock, Block noiseScaleBlock, Block lowerBlock, Block upperBlock, Block randomSeedBlock, int position)
    {
        double value = TINYINT.getLong(valueBlock, position);
        input(state, value, noiseScaleBlock, lowerBlock, upperBlock, randomSeedBlock, position);
    }

    private static void input(NoisyCountAndSumState state, double value, Block noiseScaleBlock, Block lowerBlock, Block upperBlock, Block randomSeedBlock, int position)
    {
        double noiseScale = DOUBLE.getDouble(noiseScaleBlock, position);
        double lower = DOUBLE.getDouble(lowerBlock, position);
        double upper = DOUBLE.getDouble(upperBlock, position);
        long randomSeed = BIGINT.getLong(randomSeedBlock, position);

        updateState(state, value, noiseScale, lower, upper, randomSeed);
    }

    public static void combine(NoisyCountAndSumState state, NoisyCountAndSumState otherState)
    {
        combineStates(state, otherState);
    }

    public static void output(NoisyCountAndSumState state, BlockBuilder out)
    {
        writeNoisySumOutput(state, out);
    }
}
