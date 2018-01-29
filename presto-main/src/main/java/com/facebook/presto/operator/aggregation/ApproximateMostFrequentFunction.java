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

import com.facebook.presto.ExceededMemoryLimitException;
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.MapType;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.String.format;

/**
 * Aggregation function that approximates the frequency of the top-K elements for Zipfian distributions.
 * This function keeps counts for a "frequent" subset of elements and assumes all other elements
 * once fewer than the least-frequent "frequent" element.
 *
 * The algorithm is based loosely on:
 *
 * .. code-block:: none
 *
 * Ahmed Metwally, Divyakant Agrawal, and Amr El Abbadi,
 * "Efficient Computation of Frequent and Top-*k* Elements in Data Streams",
 * https://icmi.cs.ucsb.edu/research/tech_reports/reports/2005-23.pdf
 */
public class ApproximateMostFrequentFunction
        extends SqlAggregationFunction
{
    public static final ApproximateMostFrequentFunction APPROXIMATE_MOST_FREQUENT = new ApproximateMostFrequentFunction();
    public static final String NAME = "approx_most_frequent";
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(ApproximateMostFrequentFunction.class, "output", ApproximateMostFrequentState.class, BlockBuilder.class);
    private static final MethodHandle INPUT_FUNCTION = methodHandle(ApproximateMostFrequentFunction.class, "input", Type.class, ApproximateMostFrequentState.class, long.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(ApproximateMostFrequentFunction.class, "combine", ApproximateMostFrequentState.class, ApproximateMostFrequentState.class);

    public static final int EXPECTED_SIZE_FOR_HASHING = 10;

    public ApproximateMostFrequentFunction()
    {
        super(NAME,
                ImmutableList.of(comparableTypeParameter("K")),
                ImmutableList.of(),
                parseTypeSignature("map(K,bigint)"),
                ImmutableList.of(parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature("K")));
    }

    @Override
    public String getDescription()
    {
        return "Count the number of times each value occurs, mixing up counts for infrequent values after max buckets";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = boundVariables.getTypeVariable("K");
        return generateAggregation(keyType, BigintType.BIGINT);
    }

    private static InternalAggregationFunction generateAggregation(Type keyType, Type valueType)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(ApproximateMostFrequentFunction.class.getClassLoader());
        List<Type> inputTypes = ImmutableList.of(BIGINT, keyType);
        Type outputType = new MapType(keyType, valueType);
        AccumulatorStateSerializer<ApproximateMostFrequentState> stateSerializer = new ApproximateMostFrequentStateSerializer(keyType);
        Type intermediateType = stateSerializer.getSerializedType();

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, outputType.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(keyType),
                INPUT_FUNCTION.bindTo(keyType),
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                ApproximateMostFrequentState.class,
                stateSerializer,
                new ApproximateMostFrequentStateFactory(),
                outputType);

        GenericAccumulatorFactoryBinder factory = new AccumulatorCompiler().generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, intermediateType, outputType, true, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type keyType)
    {
        return ImmutableList.of(new ParameterMetadata(STATE),
                new ParameterMetadata(INPUT_CHANNEL, BIGINT),
                new ParameterMetadata(BLOCK_INPUT_CHANNEL, keyType),
                new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(Type type, ApproximateMostFrequentState state, long buckets, Block key, int position)
    {
        TypedApproximateMostFrequentHistogram typedHistogram = state.get();
        if (typedHistogram == null) {
            checkCondition(buckets >= 2, INVALID_FUNCTION_ARGUMENT, "approx_most_frequent bucket count must be greater than one");
            typedHistogram = new TypedApproximateMostFrequentHistogram(type, EXPECTED_SIZE_FOR_HASHING, Ints.checkedCast(buckets));
            state.set(typedHistogram);
        }

        try {
            typedHistogram.add(position, key, 1L);
        }
        catch (ExceededMemoryLimitException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("The result of histogram may not exceed %s", e.getMaxMemory()));
        }
    }

    public static void combine(ApproximateMostFrequentState state, ApproximateMostFrequentState otherState)
    {
        if (state.get() != null && otherState.get() != null) {
            TypedApproximateMostFrequentHistogram typedHistogram = state.get();
            try {
                typedHistogram.addAll(otherState.get());
            }
            catch (ExceededMemoryLimitException e) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("The result of histogram may not exceed %s", e.getMaxMemory()));
            }
        }
        else if (state.get() == null) {
            state.set(otherState.get());
        }
    }

    public static void output(ApproximateMostFrequentState state, BlockBuilder out)
    {
        TypedApproximateMostFrequentHistogram typedHistogram = state.get();
        if (typedHistogram == null) {
            out.appendNull();
        }
        else {
            typedHistogram.writeMapTo(out);
        }
    }
}
