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
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.operator.aggregation.state.HyperLogLogState;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

@AggregationFunction("approx_set")
public final class ApproximateSetAggregation
{
    public static final double DEFAULT_STANDARD_ERROR = 0.01625;
    private static final AccumulatorStateSerializer<HyperLogLogState> SERIALIZER = StateCompiler.generateStateSerializer(HyperLogLogState.class);

    private ApproximateSetAggregation() {}

    @InputFunction
    public static void input(@AggregationState HyperLogLogState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        input(state, value, DEFAULT_STANDARD_ERROR);
    }

    @InputFunction
    @LiteralParameters("x")
    public static void input(@AggregationState HyperLogLogState state, @SqlType("varchar(x)") Slice value)
    {
        input(state, value, DEFAULT_STANDARD_ERROR);
    }

    @InputFunction
    public static void input(@AggregationState HyperLogLogState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        input(state, value, DEFAULT_STANDARD_ERROR);
    }

    @InputFunction
    public static void input(@AggregationState HyperLogLogState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.DOUBLE) double maxStandardError)
    {
        input(state, Double.doubleToLongBits(value), maxStandardError);
    }

    @InputFunction
    @LiteralParameters("x")
    public static void input(@AggregationState HyperLogLogState state, @SqlType("varchar(x)") Slice value, @SqlType(StandardTypes.DOUBLE) double maxStandardError)
    {
        HyperLogLog hll = HyperLogLogUtils.getOrCreateHyperLogLog(state, maxStandardError);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(value);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    public static void input(@AggregationState HyperLogLogState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.DOUBLE) double maxStandardError)
    {
        HyperLogLog hll = HyperLogLogUtils.getOrCreateHyperLogLog(state, maxStandardError);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(value);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @CombineFunction
    public static void combineState(@AggregationState HyperLogLogState state, @AggregationState HyperLogLogState otherState)
    {
        HyperLogLogUtils.mergeState(state, otherState.getHyperLogLog());
    }

    @OutputFunction(StandardTypes.HYPER_LOG_LOG)
    public static void evaluateFinal(@AggregationState HyperLogLogState state, BlockBuilder out)
    {
        SERIALIZER.serialize(state, out);
    }
}
