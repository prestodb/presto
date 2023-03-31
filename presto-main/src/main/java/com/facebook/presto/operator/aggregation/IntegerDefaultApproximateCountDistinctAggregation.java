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

import com.facebook.airlift.stats.cardinality.HyperLogLog;
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.state.HyperLogLogState;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.aggregation.Accumulator;
import com.facebook.presto.spi.function.aggregation.AggregationMetadata;
import com.facebook.presto.spi.function.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import com.facebook.presto.spi.function.aggregation.GroupedAccumulator;
import com.facebook.presto.type.IntegerOperators;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.util.Failures.internalError;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class IntegerDefaultApproximateCountDistinctAggregation
        extends SqlAggregationFunction
{
    public static final double DEFAULT_STANDARD_ERROR = 0.023;
    public static final IntegerDefaultApproximateCountDistinctAggregation INTEGER_DEFAULT_APPROXIMATE_COUNT_DISTINCT_AGGREGATION = new IntegerDefaultApproximateCountDistinctAggregation();
    private static final String NAME = "approx_distinct";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(IntegerDefaultApproximateCountDistinctAggregation.class, "input", HyperLogLogState.class, long.class);
    private static final MethodHandle BLOCK_INPUT_FUNCTION = methodHandle(IntegerDefaultApproximateCountDistinctAggregation.class, "blockInput", HyperLogLogState.class, Block.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(IntegerDefaultApproximateCountDistinctAggregation.class, "combine", HyperLogLogState.class, HyperLogLogState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(IntegerDefaultApproximateCountDistinctAggregation.class, "output", HyperLogLogState.class, BlockBuilder.class);

    public IntegerDefaultApproximateCountDistinctAggregation()
    {
        super(NAME,
                ImmutableList.of(),
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.BIGINT),
                ImmutableList.of(parseTypeSignature(StandardTypes.INTEGER)));
    }

    private static BuiltInAggregationFunctionImplementation generateAggregation(Type type)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(IntegerDefaultApproximateCountDistinctAggregation.class.getClassLoader());

        AccumulatorStateSerializer<HyperLogLogState> stateSerializer = StateCompiler.generateStateSerializer(HyperLogLogState.class, classLoader);
        AccumulatorStateFactory<HyperLogLogState> stateFactory = StateCompiler.generateStateFactory(HyperLogLogState.class, classLoader);
        Type intermediateType = stateSerializer.getSerializedType();

        List<Type> inputTypes = ImmutableList.of(type);

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, BIGINT.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(type),
                createBlockInputParameterMetadata(type),
                INPUT_FUNCTION,
                BLOCK_INPUT_FUNCTION,
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        HyperLogLogState.class,
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
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(INPUT_CHANNEL, type));
    }

    private static List<ParameterMetadata> createBlockInputParameterMetadata(Type type)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, type));
    }

    public static void input(HyperLogLogState state, long value)
    {
        HyperLogLog hll = HyperLogLogUtils.getOrCreateHyperLogLog(state, DEFAULT_STANDARD_ERROR);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        long hash;
        try {
            hash = IntegerOperators.xxHash64(value);
        }
        catch (Throwable t) {
            throw internalError(t);
        }
        hll.addHash(hash);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    public static void blockInput(HyperLogLogState state, Block value)
    {
        HyperLogLog hll = HyperLogLogUtils.getOrCreateHyperLogLog(state, DEFAULT_STANDARD_ERROR);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        if (value.mayHaveNull()) {
            for (int i = 0; i < value.getPositionCount(); ++i) {
                if (value.isNull(i)) {
                    continue;
                }
                long hash;
                try {
                    hash = IntegerOperators.xxHash64(INTEGER.getLong(value, i));
                }
                catch (Throwable t) {
                    throw internalError(t);
                }
                hll.addHash(hash);
            }
        }
        else {
            for (int i = 0; i < value.getPositionCount(); ++i) {
                long hash;
                try {
                    hash = IntegerOperators.xxHash64(INTEGER.getLong(value, i));
                }
                catch (Throwable t) {
                    throw internalError(t);
                }
                hll.addHash(hash);
            }
        }
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    public static void combine(HyperLogLogState state, HyperLogLogState otherState)
    {
        HyperLogLogUtils.mergeState(state, otherState.getHyperLogLog());
    }

    public static void output(HyperLogLogState state, BlockBuilder out)
    {
        HyperLogLog hyperLogLog = state.getHyperLogLog();
        if (hyperLogLog == null) {
            BIGINT.writeLong(out, 0);
        }
        else {
            BIGINT.writeLong(out, hyperLogLog.cardinality());
        }
    }

    @Override
    public String getDescription()
    {
        return "Approximate distinct count for integer";
    }

    @Override
    public BuiltInAggregationFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        return generateAggregation(INTEGER);
    }

    @Override
    public boolean isCalledOnNullInput()
    {
        return true;
    }
}
