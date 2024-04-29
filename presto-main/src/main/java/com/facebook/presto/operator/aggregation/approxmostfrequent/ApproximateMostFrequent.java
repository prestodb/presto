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
package com.facebook.presto.operator.aggregation.approxmostfrequent;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.approxmostfrequent.stream.StreamSummary;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.BlockIndex;
import com.facebook.presto.spi.function.BlockPosition;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Failures.checkCondition;
import static java.lang.Math.toIntExact;

/**
 * <p>
 * Aggregation function that computes the top-K frequent elements approximately. Approximate estimation of the function enables us to pick up the frequent
 * values with less memory. Larger input "capacity" improves the accuracy of underlying algorithm with sacrificing the memory capacity.
 * </p>
 *
 * <p>
 * The algorithm is based loosely on:
 * <a href="https://dl.acm.org/doi/10.1007/978-3-540-30570-5_27">Efficient Computation of Frequent and Top-*k* Elements in Data Streams</a>
 * by Ahmed Metwally, Divyakant Agrawal, and Amr El Abbadi
 * </p>
 */
@AggregationFunction(value = "approx_most_frequent")
@Description("Computes the top frequent elements approximately")
public final class ApproximateMostFrequent
{
    private ApproximateMostFrequent()
    {}

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @TypeParameter("T") Type type,
            @AggregationState ApproximateMostFrequentState state,
            @SqlType(BIGINT) long buckets,
            @BlockPosition @SqlType("T") Block valueBlock,
            @BlockIndex int valueIndex,
            @SqlType(BIGINT) long capacity)
    {
        StreamSummary streamSummary = state.getStateSummary();
        if (streamSummary == null) {
            checkCondition(buckets > 1, INVALID_FUNCTION_ARGUMENT, "approx_most_frequent bucket count must be greater than one, input bucket count: %s", buckets);
            streamSummary = new StreamSummary(type, toIntExact(buckets), toIntExact(capacity));
            state.setStateSummary(streamSummary);
        }
        streamSummary.add(valueBlock, valueIndex, 1L);
    }

    @CombineFunction
    public static void combine(
            @AggregationState ApproximateMostFrequentState state,
            @AggregationState ApproximateMostFrequentState otherState)
    {
        StreamSummary streamSummary = state.getStateSummary();
        if (streamSummary == null) {
            state.setStateSummary(otherState.getStateSummary());
        }
        else {
            streamSummary.merge(otherState.getStateSummary());
        }
    }

    @OutputFunction("map(T,bigint)")
    public static void output(@AggregationState ApproximateMostFrequentState state, BlockBuilder out)
    {
        if (state.getStateSummary() == null) {
            out.appendNull();
        }
        else {
            state.getStateSummary().topK(out);
        }
    }
}
