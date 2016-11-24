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
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.stats.cardinality.HyperLogLog;

@AggregationFunction("merge")
public final class MergeHyperLogLogAggregation
{
    private static final AccumulatorStateSerializer<HyperLogLogState> serializer = StateCompiler.generateStateSerializer(HyperLogLogState.class);

    private MergeHyperLogLogAggregation() {}

    @InputFunction
    public static void input(HyperLogLogState state, @SqlType(StandardTypes.HYPER_LOG_LOG) Slice value)
    {
        HyperLogLog input = HyperLogLog.newInstance(value);
        merge(state, input);
    }

    @CombineFunction
    public static void combine(HyperLogLogState state, HyperLogLogState otherState)
    {
        merge(state, otherState.getHyperLogLog());
    }

    private static void merge(HyperLogLogState state, HyperLogLog input)
    {
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
    public static void output(HyperLogLogState state, BlockBuilder out)
    {
        serializer.serialize(state, out);
    }
}
