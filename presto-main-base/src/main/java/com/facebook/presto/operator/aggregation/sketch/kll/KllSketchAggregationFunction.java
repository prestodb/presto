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
package com.facebook.presto.operator.aggregation.sketch.kll;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import io.airlift.slice.Slice;

@AggregationFunction(value = "sketch_kll")
public class KllSketchAggregationFunction
{
    /**
     * In the case the underlying library changes the default value for k, we use our own default
     * here.
     */
    private static final int DEFAULT_K = 200;

    private KllSketchAggregationFunction()
    {
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(@TypeParameter("T") Type type, @AggregationState KllSketchAggregationState state, @SqlType("T") long value)
    {
        KllSketchWithKAggregationFunction.input(type, state, value, DEFAULT_K);
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(@TypeParameter("T") Type type, @AggregationState KllSketchAggregationState state, @SqlType("T") double value)
    {
        KllSketchWithKAggregationFunction.input(type, state, value, DEFAULT_K);
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(@TypeParameter("T") Type type, @AggregationState KllSketchAggregationState state, @SqlType("T") Slice value)
    {
        KllSketchWithKAggregationFunction.input(type, state, value, DEFAULT_K);
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(@TypeParameter("T") Type type, @AggregationState KllSketchAggregationState state, @SqlType("T") boolean value)
    {
        KllSketchWithKAggregationFunction.input(type, state, value, DEFAULT_K);
    }

    @CombineFunction
    public static void combine(@AggregationState KllSketchAggregationState state, @AggregationState KllSketchAggregationState otherState)
    {
        KllSketchWithKAggregationFunction.combine(state, otherState);
    }

    @TypeParameter("T")
    @OutputFunction("kllsketch(T)")
    public static void output(@AggregationState KllSketchAggregationState state, BlockBuilder out)
    {
        KllSketchWithKAggregationFunction.output(state, out);
    }
}
