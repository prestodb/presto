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

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.operator.aggregation.mostfrequent.state.TopElementsHistogram;
import com.facebook.presto.operator.aggregation.mostfrequent.state.TopElementsState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

import java.util.Map;

@AggregationFunction("approx_most_frequent")
public class ApproximateMostFrequentAggregations
{
    private static final double DEFAULT_CONFIDENCE = 0.99;

    private ApproximateMostFrequentAggregations()
    {
    }

    @Description("Get the most frequently occuring items with approx counts. Using default error ratio and confidence")
    @InputFunction
    public static void input(
            @AggregationState TopElementsState state,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice,
            @SqlType(StandardTypes.DOUBLE) double minPercentShare)
    {
        input(state, slice, 1, minPercentShare);
    }

    @Description("Get the most frequently occuring items with approx counts. Use given error ratio and default confidence")
    @InputFunction
    public static void input(
            @AggregationState TopElementsState state,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice,
            @SqlType(StandardTypes.DOUBLE) double minPercentShare,
            @SqlType(StandardTypes.DOUBLE) double error)
    {
        input(state, slice, 1, minPercentShare, error);
    }

    @Description("Get the most frequently occuring items with approx counts. Using given error ratio and confidence")
    @InputFunction
    public static void input(
            @AggregationState TopElementsState state,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice,
            @SqlType(StandardTypes.DOUBLE) double minPercentShare,
            @SqlType(StandardTypes.DOUBLE) double error,
            @SqlType(StandardTypes.DOUBLE) double confidence)
    {
        input(state, slice, 1, minPercentShare, error, confidence);
    }

    @Description("Get the items with highest sum of increments. Using default error ratio and confidence")
    @InputFunction
    public static void input(
            @AggregationState TopElementsState state,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice,
            @SqlType(StandardTypes.BIGINT) long increment,
            @SqlType(StandardTypes.DOUBLE) double minPercentShare)
    {
        // Default error rate. Refer: http://theory.stanford.edu/~tim/s17/l/l2.pdf
        // ε = 1/2k and minPercentShare = 100/k which is ε = minPercentShare/200  This ensures that no element occurring less than minPercentShare/2 will appear in the result
        input(state, slice, increment, minPercentShare, minPercentShare / 200);
    }

    @Description("Get the items with highest sum of increments. Using given error ratio and default confidence")
    @InputFunction
    public static void input(
            @AggregationState TopElementsState state,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice,
            @SqlType(StandardTypes.BIGINT) long increment,
            @SqlType(StandardTypes.DOUBLE) double minPercentShare,
            @SqlType(StandardTypes.DOUBLE) double error)
    {
        // Use the minimum of the provided error bound and the default error for given minPercentShare
        error = Math.min(minPercentShare / 200, error);
        input(state, slice, increment, minPercentShare, error, DEFAULT_CONFIDENCE);
    }

    @Description("Get the items with highest sum of increments. Using given error ratio and confidence")
    @InputFunction
    public static void input(
            @AggregationState TopElementsState state,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice,
            @SqlType(StandardTypes.BIGINT) long increment,
            @SqlType(StandardTypes.DOUBLE) double minPercentShare,
            @SqlType(StandardTypes.DOUBLE) double error,
            @SqlType(StandardTypes.DOUBLE) double confidence)
    {
        TopElementsHistogram histogram = state.getHistogram();
        if (histogram == null) {
            histogram = new TopElementsHistogram(minPercentShare, error, confidence, 1);
            state.setHistogram(histogram);
        }
        histogram.add(slice, increment);
    }

    @CombineFunction
    public static void combine(@AggregationState TopElementsState state, @AggregationState TopElementsState otherState)
    {
        TopElementsHistogram currHistogram = state.getHistogram();
        TopElementsHistogram otherHistogram = otherState.getHistogram();
        if (currHistogram == null) {
            state.setHistogram(otherHistogram);
        }
        else {
            currHistogram.merge(otherHistogram);
        }
    }

    @SqlNullable
    @OutputFunction("map(varchar,bigint)")
    public static void output(@AggregationState TopElementsState state, BlockBuilder out)
    {
        TopElementsHistogram histogram = state.getHistogram();
        if (histogram == null) {
            out.beginBlockEntry();
            out.closeEntry();
            return;
        }
        Map<Slice, Long> value = histogram.getTopElements();
        if (value == null) {
            out.appendNull();
            return;
        }
        BlockBuilder entryBuilder = out.beginBlockEntry();
        for (Map.Entry<Slice, Long> entry : value.entrySet()) {
            VarbinaryType.VARBINARY.writeSlice(entryBuilder, entry.getKey());
            BigintType.BIGINT.writeLong(entryBuilder, entry.getValue());
        }
        out.closeEntry();
    }
}
