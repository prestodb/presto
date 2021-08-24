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
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;

import java.util.Map;

import static com.facebook.presto.common.type.RealType.REAL;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

@AggregationFunction("numeric_histogram")
public class RealHistogramAggregation
{
    private RealHistogramAggregation()
    {
    }

    @InputFunction
    public static void add(@AggregationState DoubleHistogramAggregation.State state, @SqlType(StandardTypes.BIGINT) long buckets, @SqlType(StandardTypes.REAL) long value, @SqlType(StandardTypes.DOUBLE) double weight)
    {
        DoubleHistogramAggregation.add(state, buckets, intBitsToFloat((int) value), weight);
    }

    @InputFunction
    public static void add(@AggregationState DoubleHistogramAggregation.State state, @SqlType(StandardTypes.BIGINT) long buckets, @SqlType(StandardTypes.REAL) long value)
    {
        add(state, buckets, value, 1);
    }

    @CombineFunction
    public static void merge(@AggregationState DoubleHistogramAggregation.State state, @AggregationState DoubleHistogramAggregation.State other)
    {
        DoubleHistogramAggregation.merge(state, other);
    }

    @OutputFunction("map(real,real)")
    public static void output(@AggregationState DoubleHistogramAggregation.State state, BlockBuilder out)
    {
        if (state.get() == null) {
            out.appendNull();
        }
        else {
            Map<Double, Double> value = state.get().getBuckets();
            BlockBuilder entryBuilder = out.beginBlockEntry();
            for (Map.Entry<Double, Double> entry : value.entrySet()) {
                REAL.writeLong(entryBuilder, floatToRawIntBits(entry.getKey().floatValue()));
                REAL.writeLong(entryBuilder, floatToRawIntBits(entry.getValue().floatValue()));
            }
            out.closeEntry();
        }
    }
}
