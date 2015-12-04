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
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.metadata.*;
import com.facebook.presto.operator.aggregation.state.*;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.*;
import com.facebook.presto.spi.type.*;
import com.facebook.presto.type.*;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.lang.invoke.MethodHandle;
import java.util.*;

import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.*;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.String.format;

public class TruncatedHistogram
        extends SqlAggregationFunction
{
    public static final TruncatedHistogram TRUNCATED_HISTOGRAM = new TruncatedHistogram();
    public static final String NAME = "truncated_histogram";
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(TruncatedHistogram.class, "output", TruncatedHistogramState.class, BlockBuilder.class);
    private static final MethodHandle INPUT_FUNCTION = methodHandle(TruncatedHistogram.class, "input", Type.class, TruncatedHistogramState.class, long.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(TruncatedHistogram.class, "combine", TruncatedHistogramState.class, TruncatedHistogramState.class);

    public static final int EXPECTED_SIZE_FOR_HASHING = 10;

    public TruncatedHistogram()
    {
        super(NAME, ImmutableList.of(comparableTypeParameter("K")), "map<K,bigint>", ImmutableList.of(StandardTypes.BIGINT, "K"));
    }

    @Override
    public String getDescription()
    {
        return "Count the number of times each value occurs, mixing up counts for infrequent values after max buckets";
    }

    @Override
    public InternalAggregationFunction specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = types.get("K");
        Type valueType = BigintType.BIGINT;
        return generateAggregation(keyType, valueType);
    }

    private static InternalAggregationFunction generateAggregation(Type keyType, Type valueType)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(TruncatedHistogram.class.getClassLoader());
        List<Type> inputTypes = ImmutableList.of(BIGINT, keyType);
        Type outputType = new MapType(keyType, valueType);
        AccumulatorStateSerializer<TruncatedHistogramState> stateSerializer = new TruncatedHistogramState.Serializer(keyType);
        Type intermediateType = stateSerializer.getSerializedType();
        MethodHandle inputFunction = INPUT_FUNCTION.bindTo(keyType);

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, outputType, inputTypes),
                createInputParameterMetadata(keyType),
                inputFunction,
                null,
                null,
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                TruncatedHistogramState.class,
                stateSerializer,
                new TruncatedHistogramState.Factory(),
                outputType,
                false);

        GenericAccumulatorFactoryBinder factory = new AccumulatorCompiler().generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, intermediateType, outputType, true, false, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type keyType)
    {
        return ImmutableList.of(new ParameterMetadata(STATE),
                new ParameterMetadata(INPUT_CHANNEL, BIGINT),
                new ParameterMetadata(BLOCK_INPUT_CHANNEL, keyType),
                new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(Type type, TruncatedHistogramState state, long buckets, Block key, int position)
    {
        TypedTruncatedHistogram typedHistogram = state.get();
        if (typedHistogram == null) {
            checkCondition(buckets >= 2, INVALID_FUNCTION_ARGUMENT, "truncated_histogram bucket count must be greater than one");
            typedHistogram = new TypedTruncatedHistogram(type, EXPECTED_SIZE_FOR_HASHING, Ints.checkedCast(buckets));
            state.set(typedHistogram);
        }

        try {
            typedHistogram.add(position, key, 1L);
        }
        catch (ExceededMemoryLimitException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("The result of histogram may not exceed %s", e.getMaxMemory()));
        }
    }

    public static void combine(TruncatedHistogramState state, TruncatedHistogramState otherState)
    {
        if (state.get() != null && otherState.get() != null) {
            TypedTruncatedHistogram typedHistogram = state.get();
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

    public static void output(TruncatedHistogramState state, BlockBuilder out)
    {
        TypedTruncatedHistogram typedHistogram = state.get();
        if (typedHistogram == null) {
            out.appendNull();
        }
        else {
            typedHistogram.writeMapTo(out);
        }
    }

}
