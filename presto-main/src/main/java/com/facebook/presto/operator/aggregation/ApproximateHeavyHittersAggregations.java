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


import com.facebook.presto.operator.aggregation.state.TopElementsHistogram;
import com.facebook.presto.operator.aggregation.state.TopElementsState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.*;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;

import java.util.Map;


@AggregationFunction("approx_heavy_hitters")
public class ApproximateHeavyHittersAggregations
{
    private ApproximateHeavyHittersAggregations() {}

    @InputFunction
    public static void input(@AggregationState TopElementsState state, @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice, @SqlType(StandardTypes.DOUBLE) double min_percent_share)
    {
        input(state, slice, min_percent_share, 0.0001, 0.999);
    }

    //TODO how to add a input function which accepts confidence but uses default value for error
    @InputFunction
    public static void input(@AggregationState TopElementsState state, @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice, @SqlType(StandardTypes.DOUBLE) double min_percent_share, @SqlType(StandardTypes.DOUBLE) double error)
    {
        input(state, slice, min_percent_share, error, 0.999);
    }

    @InputFunction
    public static void input(@AggregationState TopElementsState state, @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice, @SqlType(StandardTypes.DOUBLE) double min_percent_share, @SqlType(StandardTypes.DOUBLE) double error, @SqlType(StandardTypes.DOUBLE) double confidence)
    {
        TopElementsHistogram<String> histogram = state.getHistogram();
        if (histogram == null) {
            histogram = new TopElementsHistogram<>(min_percent_share, error, confidence, 1);  //TODO set the seed to be derived from the column name
        }

        histogram.add(slice.toStringUtf8());
        state.setHistogram(histogram);
        state.addMemoryUsage(histogram.estimatedInMemorySize());
    }

    @CombineFunction
    public static void combine(@AggregationState TopElementsState state, @AggregationState TopElementsState otherState)
    {
        TopElementsHistogram currHistogram = state.getHistogram();
        TopElementsHistogram otherHistogram = otherState.getHistogram();
        if (currHistogram == null) {
            state.setHistogram(otherHistogram);
            state.addMemoryUsage(otherHistogram.estimatedInMemorySize());
        }else{
            currHistogram.merge(otherHistogram);
            state.addMemoryUsage(otherHistogram.estimatedInMemorySize());
        }
    }

    @OutputFunction("map(varchar,bigint)")  //  <== these are presto data types and not java
    public static void output(@AggregationState TopElementsState state, BlockBuilder out)
    {
        TopElementsHistogram histogram = state.getHistogram();
        if (histogram == null) {
            out.appendNull();
        }
        else {
            Map<String, Long> value = histogram.getTopElements();

            BlockBuilder entryBuilder = out.beginBlockEntry();
            for (Map.Entry<String, Long> entry : value.entrySet()) {
                VarcharType.VARCHAR.writeString(entryBuilder, entry.getKey());
                BigintType.BIGINT.writeLong(entryBuilder, entry.getValue());
            }
            out.closeEntry();
        }
    }
}
