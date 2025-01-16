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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.datasketches.kll.KllItemsSketch;

import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.operator.aggregation.sketch.kll.KllSketchAggregationState.getEstimatedKllInMemorySize;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static java.lang.String.format;
import static org.apache.datasketches.kll.KllSketch.MAX_K;

@AggregationFunction(value = "sketch_kll_with_k")
public class KllSketchWithKAggregationFunction
{
    private KllSketchWithKAggregationFunction()
    {
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(@TypeParameter("T") Type type, @AggregationState KllSketchAggregationState state, @SqlType("T") long value, @SqlType(BIGINT) long k)
    {
        initializeSketch(state, type, k);
        KllItemsSketch<Long> sketch = state.getSketch();
        state.addMemoryUsage(() -> -getEstimatedKllInMemorySize(sketch, long.class));
        state.update(value);
        state.addMemoryUsage(() -> getEstimatedKllInMemorySize(sketch, long.class));
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(@TypeParameter("T") Type type, @AggregationState KllSketchAggregationState state, @SqlType("T") double value, @SqlType(BIGINT) long k)
    {
        initializeSketch(state, type, k);
        KllItemsSketch<Double> sketch = state.getSketch();
        state.addMemoryUsage(() -> -getEstimatedKllInMemorySize(sketch, double.class));
        state.update(value);
        state.addMemoryUsage(() -> getEstimatedKllInMemorySize(sketch, double.class));
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(@TypeParameter("T") Type type, @AggregationState KllSketchAggregationState state, @SqlType("T") Slice value, @SqlType(BIGINT) long k)
    {
        initializeSketch(state, type, k);
        KllItemsSketch sketch = state.getSketch();
        state.addMemoryUsage(() -> -getEstimatedKllInMemorySize(sketch, Slice.class));
        state.update(value);
        state.addMemoryUsage(() -> getEstimatedKllInMemorySize(sketch, Slice.class));
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(@TypeParameter("T") Type type, @AggregationState KllSketchAggregationState state, @SqlType("T") boolean value, @SqlType(BIGINT) long k)
    {
        initializeSketch(state, type, k);
        KllItemsSketch<Boolean> sketch = state.getSketch();
        state.addMemoryUsage(() -> -getEstimatedKllInMemorySize(sketch, boolean.class));
        state.update(value);
        state.addMemoryUsage(() -> getEstimatedKllInMemorySize(sketch, boolean.class));
    }

    @CombineFunction
    public static void combine(@AggregationState KllSketchAggregationState state, @AggregationState KllSketchAggregationState otherState)
    {
        if (state.getSketch() != null && otherState.getSketch() != null) {
            state.addMemoryUsage(() -> -getEstimatedKllInMemorySize(state.getSketch(), state.getType().getJavaType()));
            state.getSketch().merge(otherState.getSketch());
            state.addMemoryUsage(() -> getEstimatedKllInMemorySize(state.getSketch(), state.getType().getJavaType()));
        }
        else if (state.getSketch() == null) {
            state.setSketch(otherState.getSketch());
            state.addMemoryUsage(() -> getEstimatedKllInMemorySize(otherState.getSketch(), state.getType().getJavaType()));
        }
    }

    @TypeParameter("T")
    @OutputFunction("kllsketch(T)")
    public static void output(@AggregationState KllSketchAggregationState state, BlockBuilder out)
    {
        if (state.getSketch() == null) {
            out.appendNull();
            return;
        }
        VARBINARY.writeSlice(out, Slices.wrappedBuffer(state.getSketch().toByteArray()));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void initializeSketch(KllSketchAggregationState state, Type type, long k)
    {
        if (state.getSketch() != null) {
            return;
        }

        if (k < 8 || k > MAX_K) {
            throw new PrestoException(INVALID_ARGUMENTS, format("k value must satisfy 8 <= k <= %d: %d", MAX_K, k));
        }

        KllSketchAggregationState.SketchParameters parameters = KllSketchAggregationState.getSketchParameters(type);
        KllItemsSketch sketch = KllItemsSketch.newHeapInstance((int) k, parameters.getComparator(), parameters.getSerde());

        state.setSketch(sketch);
        state.setConversion(parameters.getConversion());
        state.addMemoryUsage(() -> getEstimatedKllInMemorySize(sketch, state.getType().getJavaType()));
    }
}
