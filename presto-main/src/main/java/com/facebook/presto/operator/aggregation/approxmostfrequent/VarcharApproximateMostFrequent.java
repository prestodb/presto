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

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.operator.aggregation.state.StringApproximateMostFrequentStateFactory;
import com.facebook.presto.operator.aggregation.state.StringApproximateMostFrequentStateSerializer;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateMetadata;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Failures.checkCondition;
import static java.lang.Math.toIntExact;

/**
 *  <p>
 *  Aggregation function that approximates the frequency of the top-K elements.
 *  This function keeps counts for a "frequent" subset of elements and assumes all other elements
 *  once fewer than the least-frequent "frequent" element.
 *  </p>
 *
 * <p>
 * The algorithm is based loosely on:
 * <a href="https://dl.acm.org/doi/10.1007/978-3-540-30570-5_27">Efficient Computation of Frequent and Top-*k* Elements in Data Streams</a>
 * by Ahmed Metwally, Divyakant Agrawal, and Amr El Abbadi
 * </p>
 */
@AggregationFunction("approx_most_frequent")
public final class VarcharApproximateMostFrequent
{
    private VarcharApproximateMostFrequent() {}

    @AccumulatorStateMetadata(stateSerializerClass = StringApproximateMostFrequentStateSerializer.class, stateFactoryClass = StringApproximateMostFrequentStateFactory.class)
    public interface State
            extends AccumulatorState
    {
        ApproximateMostFrequentHistogram<Slice> get();
        void set(ApproximateMostFrequentHistogram<Slice> value);
    }

    @InputFunction
    public static void input(@AggregationState State state, @SqlType(BIGINT) long buckets, @SqlType(VARCHAR) Slice value, @SqlType(BIGINT) long capacity)
    {
        ApproximateMostFrequentHistogram<Slice> histogram = state.get();
        if (histogram == null) {
            checkCondition(buckets >= 2, INVALID_FUNCTION_ARGUMENT, "approx_most_frequent bucket count must be greater than one");
            histogram = new ApproximateMostFrequentHistogram<>(
                    toIntExact(buckets),
                    toIntExact(capacity),
                    StringApproximateMostFrequentStateSerializer::serializeBucket,
                    StringApproximateMostFrequentStateSerializer::deserializeBucket);
            state.set(histogram);
        }

        histogram.add(value);
    }

    @CombineFunction
    public static void combine(@AggregationState State state, @AggregationState State otherState)
    {
        ApproximateMostFrequentHistogram<Slice> otherHistogram = otherState.get();

        ApproximateMostFrequentHistogram<Slice> histogram = state.get();
        if (histogram == null) {
            state.set(otherHistogram);
        }
        else {
            histogram.merge(otherHistogram);
        }
    }

    @OutputFunction("map(varchar,bigint)")
    public static void output(@AggregationState State state, BlockBuilder out)
    {
        if (state.get() == null) {
            out.appendNull();
        }
        else {
            BlockBuilder entryBuilder = out.beginBlockEntry();
            state.get().forEachBucket((key, value) -> {
                VarcharType.VARCHAR.writeSlice(entryBuilder, key);
                BigintType.BIGINT.writeLong(entryBuilder, value);
            });
            out.closeEntry();
        }
    }
}
