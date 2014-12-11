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
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.SqlType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.stats.cardinality.HyperLogLog;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

@AggregationFunction("approx_distinct")
public final class ApproximateCountDistinctAggregations
{
    public static final InternalAggregationFunction LONG_APPROXIMATE_COUNT_DISTINCT_AGGREGATIONS = new AggregationCompiler().generateAggregationFunction(ApproximateCountDistinctAggregations.class, BIGINT, ImmutableList.<Type>of(BIGINT));
    public static final InternalAggregationFunction DOUBLE_APPROXIMATE_COUNT_DISTINCT_AGGREGATIONS = new AggregationCompiler().generateAggregationFunction(ApproximateCountDistinctAggregations.class, BIGINT, ImmutableList.<Type>of(DOUBLE));
    public static final InternalAggregationFunction VARBINARY_APPROXIMATE_COUNT_DISTINCT_AGGREGATIONS = new AggregationCompiler().generateAggregationFunction(ApproximateCountDistinctAggregations.class, BIGINT, ImmutableList.<Type>of(VARCHAR));
    private static final int NUMBER_OF_BUCKETS = 2048;

    private ApproximateCountDistinctAggregations() {}

    @InputFunction
    public static void input(HyperLogLogState state, @SqlType(StandardTypes.VARCHAR) Slice value)
    {
        HyperLogLog hll = getOrCreateHyperLogLog(state);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(value);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    public static void input(HyperLogLogState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        input(state, Double.doubleToLongBits(value));
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
            hll = HyperLogLog.newInstance(NUMBER_OF_BUCKETS);
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

    @OutputFunction(StandardTypes.BIGINT)
    public static void evaluateFinal(HyperLogLogState state, BlockBuilder out)
    {
        HyperLogLog hyperLogLog = state.getHyperLogLog();
        if (hyperLogLog == null) {
            BIGINT.writeLong(out, 0);
        }
        else {
            BIGINT.writeLong(out, hyperLogLog.cardinality());
        }
    }

    @VisibleForTesting
    public static double getStandardError()
    {
        return 1.04 / Math.sqrt(NUMBER_OF_BUCKETS);
    }
}
