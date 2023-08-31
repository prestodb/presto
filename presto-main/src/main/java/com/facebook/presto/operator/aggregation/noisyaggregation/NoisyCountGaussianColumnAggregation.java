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
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.AccumulatorCompiler;
import com.facebook.presto.operator.aggregation.BuiltInAggregationFunctionImplementation;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.aggregation.Accumulator;
import com.facebook.presto.spi.function.aggregation.AggregationMetadata;
import com.facebook.presto.spi.function.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import com.facebook.presto.spi.function.aggregation.GroupedAccumulator;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.security.SecureRandom;
import java.util.List;
import java.util.Random;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.operator.aggregation.noisyaggregation.NoisyCountGaussianColumnAggregationUtils.computeNoisyCount;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * Add a random Gaussian noise to true count with a given value of standard deviation of the noise.
 * If one needs to replace COUNT(*), NOISY_COUNT_GAUSSIAN(1, noiseScale) should be used.
 * <p>
 * This function behaves similarly to COUNT.
 * So, in the case of empty input, this function returns 0 if there is no grouping,
 * and returns NULL if there is grouping
 * <p>
 * Optional randomSeed is used to get a fixed value of noise, often for reproducibility purposes.
 * If randomSeed is omitted or 0, SecureRandom is used. If randomSeed > 0 is provided, Random is used.
 * <p>
 * Function signature is NOISY_COUNT_GAUSSIAN(x, noiseScale[, randomSeed])
 * - x: input column/value
 * - noiseScale: standard deviation of noise
 * - randomSeed: (optional) random seed
 */
public class NoisyCountGaussianColumnAggregation
        extends SqlAggregationFunction
{
    public static final NoisyCountGaussianColumnAggregation NOISY_COUNT_GAUSSIAN_AGGREGATION = new NoisyCountGaussianColumnAggregation();
    private static final String NAME = "noisy_count_gaussian";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(NoisyCountGaussianColumnAggregation.class, "input", CountScaleState.class, Block.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(NoisyCountGaussianColumnAggregation.class, "combine", CountScaleState.class, CountScaleState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(NoisyCountGaussianColumnAggregation.class, "output", CountScaleState.class, BlockBuilder.class);

    public NoisyCountGaussianColumnAggregation()
    {
        super(NAME,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.BIGINT),
                ImmutableList.of(parseTypeSignature("T"), DOUBLE.getTypeSignature()));
    }

    @Override
    public String getDescription()
    {
        return "Counts the non-null values and then add Gaussian noise to the true count. The noisy count is post-processed to be non-negative and rounded to bigint. Noise is from a secure random.";
    }

    @Override
    public BuiltInAggregationFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type type = boundVariables.getTypeVariable("T");
        return generateAggregation(type);
    }

    private static BuiltInAggregationFunctionImplementation generateAggregation(Type type)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(NoisyCountGaussianColumnAggregation.class.getClassLoader());

        AccumulatorStateSerializer<CountScaleState> stateSerializer = StateCompiler.generateStateSerializer(CountScaleState.class, classLoader);
        AccumulatorStateFactory<CountScaleState> stateFactory = StateCompiler.generateStateFactory(CountScaleState.class, classLoader);
        Type intermediateType = stateSerializer.getSerializedType();

        List<Type> inputTypes = ImmutableList.of(type, DOUBLE);

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, BIGINT.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(type),
                INPUT_FUNCTION,
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        CountScaleState.class,
                        stateSerializer,
                        stateFactory)),
                BIGINT);

        Class<? extends Accumulator> accumulatorClass = AccumulatorCompiler.generateAccumulatorClass(
                Accumulator.class,
                metadata,
                classLoader);
        Class<? extends GroupedAccumulator> groupedAccumulatorClass = AccumulatorCompiler.generateAccumulatorClass(
                GroupedAccumulator.class,
                metadata,
                classLoader);
        return new BuiltInAggregationFunctionImplementation(NAME, inputTypes, ImmutableList.of(intermediateType), BIGINT,
                true, false, metadata, accumulatorClass, groupedAccumulatorClass);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type type)
    {
        return ImmutableList.of(
                new ParameterMetadata(STATE),
                new ParameterMetadata(BLOCK_INPUT_CHANNEL, type),
                new ParameterMetadata(BLOCK_INPUT_CHANNEL, DOUBLE),
                new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(CountScaleState state, Block valueBlock, Block noiseScaleBlock, int index)
    {
        double noiseScale = DOUBLE.getDouble(noiseScaleBlock, index);
        if (noiseScale < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Noise scale must be >= 0");
        }
        // Update count and retain scale and random seed
        state.setCount(state.getCount() + 1);
        state.setNoiseScale(noiseScale);
    }

    public static void combine(CountScaleState state, CountScaleState otherState)
    {
        state.setCount(state.getCount() + otherState.getCount());
        state.setNoiseScale(state.getNoiseScale() > 0 ? state.getNoiseScale() : otherState.getNoiseScale()); // noise scale should be > 0
    }

    public static void output(CountScaleState state, BlockBuilder out)
    {
        if (state.getCount() == 0) {
            out.appendNull();
            return;
        }

        Random random = new SecureRandom();
        long noisyCountFixedSignAndType = computeNoisyCount(state.getCount(), state.getNoiseScale(), random);
        BIGINT.writeLong(out, noisyCountFixedSignAndType);
    }
}
