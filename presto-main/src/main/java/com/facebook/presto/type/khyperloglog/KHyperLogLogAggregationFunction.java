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
package com.facebook.presto.type.khyperloglog;

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.AccumulatorCompiler;
import com.facebook.presto.operator.aggregation.BuiltInAggregationFunctionImplementation;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.aggregation.Accumulator;
import com.facebook.presto.spi.function.aggregation.AggregationMetadata;
import com.facebook.presto.spi.function.aggregation.GroupedAccumulator;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.type.khyperloglog.KHyperLogLogType.K_HYPER_LOG_LOG;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;

public final class KHyperLogLogAggregationFunction
        extends SqlAggregationFunction
{
    private static final String NAME = "khyperloglog_agg";
    private static final KHyperLogLogStateSerializer SERIALIZER = new KHyperLogLogStateSerializer();
    private static final MethodHandle LONG_LONG_INPUT_FUNCTION = methodHandle(KHyperLogLogAggregationFunction.class, "input", KHyperLogLogState.class, long.class, long.class);
    private static final MethodHandle SLICE_LONG_INPUT_FUNCTION = methodHandle(KHyperLogLogAggregationFunction.class, "input", KHyperLogLogState.class, Slice.class, long.class);
    private static final MethodHandle DOUBLE_LONG_INPUT_FUNCTION = methodHandle(KHyperLogLogAggregationFunction.class, "input", KHyperLogLogState.class, double.class, long.class);
    private static final MethodHandle LONG_SLICE_INPUT_FUNCTION = methodHandle(KHyperLogLogAggregationFunction.class, "input", KHyperLogLogState.class, long.class, Slice.class);
    private static final MethodHandle SLICE_SLICE_INPUT_FUNCTION = methodHandle(KHyperLogLogAggregationFunction.class, "input", KHyperLogLogState.class, Slice.class, Slice.class);
    private static final MethodHandle DOUBLE_SLICE_INPUT_FUNCTION = methodHandle(KHyperLogLogAggregationFunction.class, "input", KHyperLogLogState.class, double.class, Slice.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(KHyperLogLogAggregationFunction.class, "output", KHyperLogLogState.class, BlockBuilder.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(KHyperLogLogAggregationFunction.class, "combine", KHyperLogLogState.class, KHyperLogLogState.class);
    private final long groupLimit;

    public KHyperLogLogAggregationFunction(long groupLimit)
    {
        super(NAME, ImmutableList.of(typeVariable("E"), typeVariable("T")), ImmutableList.of(), K_HYPER_LOG_LOG.getTypeSignature(), ImmutableList.of(parseTypeSignature("E"), parseTypeSignature("T")));
        this.groupLimit = groupLimit;
    }

    public static String getFunctionName()
    {
        return NAME;
    }

    @Override
    public BuiltInAggregationFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type firstInputType = boundVariables.getTypeVariable("E");
        Type secondInputType = boundVariables.getTypeVariable("T");
        return generateAggregation(firstInputType, secondInputType);
    }

    private BuiltInAggregationFunctionImplementation generateAggregation(Type firstInputType, Type secondInputType)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(KHyperLogLogAggregationFunction.class.getClassLoader());
        List<Type> inputTypes = ImmutableList.of(firstInputType, secondInputType);
        Class<? extends AccumulatorState> stateInterface = KHyperLogLogState.class;
        AccumulatorStateSerializer<?> stateSerializer = new KHyperLogLogStateSerializer();
        MethodHandle inputFunction = getMethodHandle(firstInputType, secondInputType);

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, K_HYPER_LOG_LOG.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                ImmutableList.of(new AggregationMetadata.ParameterMetadata(STATE), new AggregationMetadata.ParameterMetadata(INPUT_CHANNEL, firstInputType), new AggregationMetadata.ParameterMetadata(INPUT_CHANNEL, secondInputType)),
                inputFunction,
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                ImmutableList.of(new AggregationMetadata.AccumulatorStateDescriptor(
                        stateInterface,
                        stateSerializer,
                        new KHyperLogLogStateFactory(groupLimit))),
                K_HYPER_LOG_LOG);

        Type intermediateType = stateSerializer.getSerializedType();

        Class<? extends Accumulator> accumulatorClass = AccumulatorCompiler.generateAccumulatorClass(
                Accumulator.class,
                metadata,
                classLoader);
        Class<? extends GroupedAccumulator> groupedAccumulatorClass = AccumulatorCompiler.generateAccumulatorClass(
                GroupedAccumulator.class,
                metadata,
                classLoader);
        return new BuiltInAggregationFunctionImplementation(NAME, inputTypes, ImmutableList.of(intermediateType), K_HYPER_LOG_LOG,
                true, false, metadata, accumulatorClass, groupedAccumulatorClass);
    }

    private static MethodHandle getMethodHandle(Type firstInputType, Type secondInputType)
    {
        MethodHandle inputFunction;
        if (firstInputType.getJavaType() == long.class && secondInputType.getJavaType() == long.class) {
            inputFunction = LONG_LONG_INPUT_FUNCTION;
        }
        else if (firstInputType.getJavaType() == Slice.class && secondInputType.getJavaType() == long.class) {
            inputFunction = SLICE_LONG_INPUT_FUNCTION;
        }
        else if (firstInputType.getJavaType() == double.class && secondInputType.getJavaType() == long.class) {
            inputFunction = DOUBLE_LONG_INPUT_FUNCTION;
        }
        else if (firstInputType.getJavaType() == long.class && secondInputType.getJavaType() == Slice.class) {
            inputFunction = LONG_SLICE_INPUT_FUNCTION;
        }
        else if (firstInputType.getJavaType() == Slice.class && secondInputType.getJavaType() == Slice.class) {
            inputFunction = SLICE_SLICE_INPUT_FUNCTION;
        }
        else if (firstInputType.getJavaType() == double.class && secondInputType.getJavaType() == Slice.class) {
            inputFunction = DOUBLE_SLICE_INPUT_FUNCTION;
        }
        else {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "input types for khyperloglog_agg are not supported");
        }
        return inputFunction;
    }

    @Override
    public String getDescription()
    {
        return "Returns the KHyperLogLog sketch that represents the relationship between columns x and y. The MinHash structure summarizes x and the HyperLogLog sketches represent y values linked to x values.";
    }

    public static void input(KHyperLogLogState state, long value, long uii)
    {
        if (state.getKHLL() == null) {
            state.setKHLL(new KHyperLogLog());
        }
        state.getKHLL().add(value, uii);
    }

    public static void input(KHyperLogLogState state, Slice value, long uii)
    {
        if (state.getKHLL() == null) {
            state.setKHLL(new KHyperLogLog());
        }
        state.getKHLL().add(value, uii);
    }

    public static void input(KHyperLogLogState state, double value, long uii)
    {
        input(state, Double.doubleToLongBits(value), uii);
    }

    public static void input(KHyperLogLogState state, long value, Slice uii)
    {
        input(state, value, XxHash64.hash(uii));
    }

    public static void input(KHyperLogLogState state, Slice value, Slice uii)
    {
        input(state, value, XxHash64.hash(uii));
    }

    public static void input(KHyperLogLogState state, double value, Slice uii)
    {
        input(state, Double.doubleToLongBits(value), XxHash64.hash(uii));
    }

    public static void combine(KHyperLogLogState state, KHyperLogLogState otherState)
    {
        if (state.getKHLL() == null) {
            KHyperLogLog copy = new KHyperLogLog();
            copy.mergeWith(otherState.getKHLL());
            state.setKHLL(copy);
        }
        else {
            state.getKHLL().mergeWith(otherState.getKHLL());
        }
    }

    public static void output(KHyperLogLogState state, BlockBuilder out)
    {
        SERIALIZER.serialize(state, out);
    }
}
