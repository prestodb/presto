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
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.aggregation.Accumulator;
import com.facebook.presto.spi.function.aggregation.AggregationMetadata;
import com.facebook.presto.spi.function.aggregation.GroupedAccumulator;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.type.khyperloglog.KHyperLogLogType.K_HYPER_LOG_LOG;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;

public final class MergeKHyperLogLogWithLimitAggregationFunction
        extends SqlAggregationFunction
{
    private static final String NAME = "merge";

    private static final MethodHandle INPUT_FUNCTION = methodHandle(MergeKHyperLogLogWithLimitAggregationFunction.class, "input", KHyperLogLogState.class, Slice.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(MergeKHyperLogLogWithLimitAggregationFunction.class, "output", KHyperLogLogState.class, BlockBuilder.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(MergeKHyperLogLogWithLimitAggregationFunction.class, "combine", KHyperLogLogState.class, KHyperLogLogState.class);

    private final long groupLimit;

    public MergeKHyperLogLogWithLimitAggregationFunction(long groupLimit)
    {
        super(NAME, ImmutableList.of(), ImmutableList.of(), K_HYPER_LOG_LOG.getTypeSignature(), ImmutableList.of(K_HYPER_LOG_LOG.getTypeSignature()));
        this.groupLimit = groupLimit;
    }

    public static void input(@AggregationState KHyperLogLogState state, @SqlType(KHyperLogLogType.NAME) Slice value)
    {
        KHyperLogLog instance = KHyperLogLog.newInstance(value);
        merge(state, instance);
    }

    public static void combine(@AggregationState KHyperLogLogState state, @AggregationState KHyperLogLogState otherState)
    {
        merge(state, otherState.getKHLL());
    }

    private static void merge(@AggregationState KHyperLogLogState state, KHyperLogLog instance)
    {
        if (state.getKHLL() == null) {
            state.setKHLL(instance);
        }
        else {
            state.getKHLL().mergeWith(instance);
        }
    }

    public static void output(@AggregationState KHyperLogLogState state, BlockBuilder out)
    {
        if (state.getKHLL() == null) {
            out.appendNull();
        }
        else {
            K_HYPER_LOG_LOG.writeSlice(out, state.getKHLL().serialize());
        }
    }

    @Override
    public BuiltInAggregationFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(MergeKHyperLogLogWithLimitAggregationFunction.class.getClassLoader());
        List<Type> inputTypes = ImmutableList.of(K_HYPER_LOG_LOG);
        Class<? extends AccumulatorState> stateInterface = KHyperLogLogState.class;
        AccumulatorStateSerializer<?> stateSerializer = new KHyperLogLogStateSerializer();

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, K_HYPER_LOG_LOG.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                ImmutableList.of(new AggregationMetadata.ParameterMetadata(STATE), new AggregationMetadata.ParameterMetadata(INPUT_CHANNEL, K_HYPER_LOG_LOG)),
                INPUT_FUNCTION,
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

    @Override
    public String getDescription()
    {
        return "Merge KHyperLogLog sketches";
    }
}
