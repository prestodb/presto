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

import com.facebook.presto.operator.aggregation.state.SliceState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.SqlType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Murmur3;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

@AggregationFunction("approx_distinct")
public final class ApproximateCountDistinctAggregations
{
    public static final InternalAggregationFunction LONG_APPROXIMATE_COUNT_DISTINCT_AGGREGATIONS = new AggregationCompiler().generateAggregationFunction(ApproximateCountDistinctAggregations.class, BIGINT, ImmutableList.<Type>of(BIGINT));
    public static final InternalAggregationFunction DOUBLE_APPROXIMATE_COUNT_DISTINCT_AGGREGATIONS = new AggregationCompiler().generateAggregationFunction(ApproximateCountDistinctAggregations.class, BIGINT, ImmutableList.<Type>of(DOUBLE));
    public static final InternalAggregationFunction VARBINARY_APPROXIMATE_COUNT_DISTINCT_AGGREGATIONS = new AggregationCompiler().generateAggregationFunction(ApproximateCountDistinctAggregations.class, BIGINT, ImmutableList.<Type>of(VARCHAR));
    private static final HyperLogLog ESTIMATOR = new HyperLogLog(2048);

    private ApproximateCountDistinctAggregations() {}

    @InputFunction
    public static void input(SliceState state, @SqlType(StandardTypes.VARCHAR) Slice value)
    {
        update(state, Murmur3.hash64(value));
    }

    @InputFunction
    public static void input(SliceState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        update(state, Murmur3.hash64(Double.doubleToLongBits(value)));
    }

    @InputFunction
    public static void input(SliceState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        update(state, Murmur3.hash64(value));
    }

    private static void update(SliceState state, long hash)
    {
        if (state.getSlice() == null) {
            state.setSlice(Slices.allocate(ESTIMATOR.getSizeInBytes()));
        }
        ESTIMATOR.update(hash, state.getSlice(), 0);
    }

    @CombineFunction
    public static void combine(SliceState state, SliceState otherState)
    {
        if (state.getSlice() == null) {
            state.setSlice(otherState.getSlice());
        }
        else {
            ESTIMATOR.mergeInto(state.getSlice(), 0, otherState.getSlice(), 0);
        }
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void output(SliceState state, BlockBuilder out)
    {
        if (state.getSlice() != null) {
            BIGINT.writeLong(out, ESTIMATOR.estimate(state.getSlice(), 0));
        }
        else {
            BIGINT.writeLong(out, 0);
        }
    }

    @VisibleForTesting
    public static double getStandardError()
    {
        return ESTIMATOR.getStandardError();
    }
}
