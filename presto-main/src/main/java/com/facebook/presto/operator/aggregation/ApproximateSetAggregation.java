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

import com.facebook.presto.operator.aggregation.state.HyperLogLogState;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.stats.cardinality.HyperLogLog;

@AggregationFunction("approx_set")
public final class ApproximateSetAggregation
{
    private static final int NUMBER_OF_BUCKETS = 4096;
    private static final AccumulatorStateSerializer<HyperLogLogState> SERIALIZER = new StateCompiler().generateStateSerializer(HyperLogLogState.class);

    private ApproximateSetAggregation() {}

    public static HyperLogLog newHyperLogLog()
    {
        return HyperLogLog.newInstance(NUMBER_OF_BUCKETS);
    }

    @InputFunction
    public static void input(HyperLogLogState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        HyperLogLog hll = getOrCreateHyperLogLog(state);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(Double.doubleToLongBits(value));
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    @LiteralParameters("x")
    public static void input(HyperLogLogState state, @SqlType("varchar(x)") Slice value)
    {
        HyperLogLog hll = getOrCreateHyperLogLog(state);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(value);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    public static void input(HyperLogLogState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        HyperLogLog hll = getOrCreateHyperLogLog(state);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(value);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    private static HyperLogLog getOrCreateHyperLogLog(HyperLogLogState state)
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
    public static void combineState(HyperLogLogState state, HyperLogLogState otherState)
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
    public static void evaluateFinal(HyperLogLogState state, BlockBuilder out)
    {
        SERIALIZER.serialize(state, out);
    }
}
