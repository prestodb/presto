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
package com.facebook.presto.operator.aggregation.estimatendv;

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.AccumulatorCompiler;
import com.facebook.presto.operator.aggregation.BuiltInAggregationFunctionImplementation;
import com.facebook.presto.operator.aggregation.histogram.Histogram;
import com.facebook.presto.operator.aggregation.histogram.HistogramStateSerializer;
import com.facebook.presto.spi.function.aggregation.Accumulator;
import com.facebook.presto.spi.function.aggregation.AggregationMetadata;
import com.facebook.presto.spi.function.aggregation.GroupedAccumulator;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.function.Signature.comparableTypeParameter;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * Implementation of Chao's estimator from Chao 1984, using counts of values that appear exactly once and twice
 * Before running NDV estimator, first run a select count(*) group by col query to get the frequency of each
 * distinct value in col.
 * Collect the counts of values with each frequency and store in the Map field freqDict
 * Use the counts of values with frequency 1 and 2 to do the computation:
 * d_chao = d + (f_1)^2/(2*(f_2))
 * Return birthday problem solution if there are no values observed of frequency 2
 * Also make insane bets (10x) when every point observed is almost unique, which could be good or bad
 */
public class NDVEstimatorFunction
        extends SqlAggregationFunction
{
    public static final NDVEstimatorFunction NDV_ESTIMATOR_FUNCTION = new NDVEstimatorFunction();

    public static final String NAME = "ndv_estimator";
    public static final int EXPECTED_SIZE_FOR_HASHING = 10;
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(NDVEstimatorFunction.class, "output", NDVEstimatorState.class, BlockBuilder.class);
    private static final MethodHandle INPUT_FUNCTION = methodHandle(NDVEstimatorFunction.class, "input", Type.class, NDVEstimatorState.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(NDVEstimatorFunction.class, "combine", NDVEstimatorState.class, NDVEstimatorState.class);

    public NDVEstimatorFunction()
    {
        super(NAME,
                ImmutableList.of(comparableTypeParameter("K")),
                ImmutableList.of(),
                parseTypeSignature("bigint"),
                ImmutableList.of(parseTypeSignature("K")));
    }

    private static BuiltInAggregationFunctionImplementation generateAggregation(Type type, Type intermediateMapType)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(NDVEstimatorFunction.class.getClassLoader());
        List<Type> inputTypes = ImmutableList.of(type);
        HistogramStateSerializer stateSerializer = new HistogramStateSerializer(type, intermediateMapType);
        Type intermediateType = stateSerializer.getSerializedType();
        MethodHandle inputFunction = INPUT_FUNCTION.bindTo(type);
        List<AggregationMetadata.ParameterMetadata> inputParameterMetadata = createInputParameterMetadata(type);

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, BigintType.BIGINT.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                inputParameterMetadata,
                inputFunction,
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                ImmutableList.of(new AggregationMetadata.AccumulatorStateDescriptor(
                        NDVEstimatorState.class,
                        stateSerializer,
                        new NDVEstimatorStateFactory(type, EXPECTED_SIZE_FOR_HASHING))),
                BigintType.BIGINT);

        Class<? extends Accumulator> accumulatorClass = AccumulatorCompiler.generateAccumulatorClass(
                Accumulator.class,
                metadata,
                classLoader);
        Class<? extends GroupedAccumulator> groupedAccumulatorClass = AccumulatorCompiler.generateAccumulatorClass(
                GroupedAccumulator.class,
                metadata,
                classLoader);
        return new BuiltInAggregationFunctionImplementation(NAME, inputTypes, ImmutableList.of(intermediateType),
                BigintType.BIGINT, true, false, metadata, accumulatorClass, groupedAccumulatorClass);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type type)
    {
        return ImmutableList.of(new ParameterMetadata(STATE),
                new ParameterMetadata(BLOCK_INPUT_CHANNEL, type),
                new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(Type type, NDVEstimatorState state, Block key, int position)
    {
        Histogram.input(type, state, key, position);
    }

    public static void combine(NDVEstimatorState state, NDVEstimatorState otherState)
    {
        Histogram.combine(state, otherState);
    }

    public static void output(NDVEstimatorState state, BlockBuilder out)
    {
        BigintType.BIGINT.writeLong(out, state.estimate());
    }

    @Override
    public BuiltInAggregationFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type keyType = boundVariables.getTypeVariable("K");
        Type intermediateMapType = functionAndTypeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(keyType.getTypeSignature()),
                TypeSignatureParameter.of(BigintType.BIGINT.getTypeSignature())));
        return generateAggregation(keyType, intermediateMapType);
    }

    @Override
    public String getDescription()
    {
        return "an estimate of the number of distinct values in a larger dataset using a subset of the data";
    }
}
