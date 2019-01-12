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
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

@AggregationFunction("merge")
public final class MergeHyperLogLogAggregation
{
    private static final AccumulatorStateSerializer<HyperLogLogState> serializer = StateCompiler.generateStateSerializer(HyperLogLogState.class);

    private MergeHyperLogLogAggregation() {}

    @InputFunction
    public static void input(@AggregationState HyperLogLogState state, @SqlType(StandardTypes.HYPER_LOG_LOG) Slice value)
    {
        HyperLogLog input = HyperLogLog.newInstance(value);
        merge(state, input);
    }

    @CombineFunction
    public static void combine(@AggregationState HyperLogLogState state, @AggregationState HyperLogLogState otherState)
    {
        merge(state, otherState.getHyperLogLog());
    }

    private static void merge(@AggregationState HyperLogLogState state, HyperLogLog input)
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
    public static void output(@AggregationState HyperLogLogState state, BlockBuilder out)
    {
        serializer.serialize(state, out);
    }
}
