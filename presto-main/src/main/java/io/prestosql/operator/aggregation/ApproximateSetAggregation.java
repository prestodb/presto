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

import io.airlift.slice.Slice;
import io.airlift.stats.cardinality.HyperLogLog;
import io.prestosql.operator.aggregation.state.HyperLogLogState;
import io.prestosql.operator.aggregation.state.StateCompiler;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AccumulatorStateSerializer;
import io.prestosql.spi.function.AggregationFunction;
import io.prestosql.spi.function.AggregationState;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.function.InputFunction;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

@AggregationFunction("approx_set")
public final class ApproximateSetAggregation
{
    private static final int NUMBER_OF_BUCKETS = 4096;
    private static final AccumulatorStateSerializer<HyperLogLogState> SERIALIZER = StateCompiler.generateStateSerializer(HyperLogLogState.class);

    private ApproximateSetAggregation() {}

    public static HyperLogLog newHyperLogLog()
    {
        return HyperLogLog.newInstance(NUMBER_OF_BUCKETS);
    }

    @InputFunction
    public static void input(@AggregationState HyperLogLogState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        HyperLogLog hll = getOrCreateHyperLogLog(state);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(Double.doubleToLongBits(value));
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    @LiteralParameters("x")
    public static void input(@AggregationState HyperLogLogState state, @SqlType("varchar(x)") Slice value)
    {
        HyperLogLog hll = getOrCreateHyperLogLog(state);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(value);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    public static void input(@AggregationState HyperLogLogState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        HyperLogLog hll = getOrCreateHyperLogLog(state);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(value);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    private static HyperLogLog getOrCreateHyperLogLog(@AggregationState HyperLogLogState state)
    {
        HyperLogLog hll = state.getHyperLogLog();
        if (hll == null) {
            hll = newHyperLogLog();
            state.setHyperLogLog(hll);
            state.addMemoryUsage(hll.estimatedInMemorySize());
        }
        return hll;
    }

    @CombineFunction
    public static void combineState(@AggregationState HyperLogLogState state, @AggregationState HyperLogLogState otherState)
    {
        HyperLogLog input = otherState.getHyperLogLog();

        HyperLogLog previous = state.getHyperLogLog();
        if (previous == null) {
            state.setHyperLogLog(input);
            state.addMemoryUsage(input.estimatedInMemorySize());
        }
        else {
            state.addMemoryUsage(-previous.estimatedInMemorySize());
            previous.mergeWith(input);
            state.addMemoryUsage(previous.estimatedInMemorySize());
        }
    }

    @OutputFunction(StandardTypes.HYPER_LOG_LOG)
    public static void evaluateFinal(@AggregationState HyperLogLogState state, BlockBuilder out)
    {
        SERIALIZER.serialize(state, out);
    }
}
