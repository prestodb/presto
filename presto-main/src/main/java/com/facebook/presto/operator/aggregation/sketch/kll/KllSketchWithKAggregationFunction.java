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
import org.apache.datasketches.common.ArrayOfBooleansSerDe;
import org.apache.datasketches.common.ArrayOfDoublesSerDe;
import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.ArrayOfLongsSerDe;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.kll.KllItemsSketch;

import java.util.Comparator;
import java.util.function.Supplier;

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
    public static void input(@AggregationState KllSketchAggregationState state, @SqlType("T") long value, @SqlType(BIGINT) long k)
    {
        initializeSketch(state, () -> Long::compareTo, ArrayOfLongsSerDe::new, k);
        KllItemsSketch<Long> sketch = state.getSketch();
        state.addMemoryUsage(-getEstimatedKllInMemorySize(sketch, long.class));
        state.getSketch().update(value);
        state.addMemoryUsage(getEstimatedKllInMemorySize(sketch, long.class));
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(@AggregationState KllSketchAggregationState state, @SqlType("T") double value, @SqlType(BIGINT) long k)
    {
        initializeSketch(state, () -> Double::compareTo, ArrayOfDoublesSerDe::new, k);
        KllItemsSketch<Double> sketch = state.getSketch();
        state.addMemoryUsage(-getEstimatedKllInMemorySize(sketch, double.class));
        state.getSketch().update(value);
        state.addMemoryUsage(getEstimatedKllInMemorySize(sketch, double.class));
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(@AggregationState KllSketchAggregationState state, @SqlType("T") Slice value, @SqlType(BIGINT) long k)
    {
        initializeSketch(state, () -> String::compareTo, ArrayOfStringsSerDe::new, k);
        KllItemsSketch sketch = state.getSketch();
        state.addMemoryUsage(-getEstimatedKllInMemorySize(sketch, Slice.class));
        state.getSketch().update(value.toStringUtf8());
        state.addMemoryUsage(getEstimatedKllInMemorySize(sketch, Slice.class));
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(@AggregationState KllSketchAggregationState state, @SqlType("T") boolean value, @SqlType(BIGINT) long k)
    {
        initializeSketch(state, () -> Boolean::compareTo, ArrayOfBooleansSerDe::new, k);
        KllItemsSketch<Boolean> sketch = state.getSketch();
        state.addMemoryUsage(-getEstimatedKllInMemorySize(sketch, boolean.class));
        state.getSketch().update(value);
        state.addMemoryUsage(getEstimatedKllInMemorySize(sketch, boolean.class));
    }

    @CombineFunction
    public static void combine(@AggregationState KllSketchAggregationState state, @AggregationState KllSketchAggregationState otherState)
    {
        if (state.getSketch() != null && otherState.getSketch() != null) {
            state.addMemoryUsage(-getEstimatedKllInMemorySize(state.getSketch(), state.getType().getJavaType()));
            state.getSketch().merge(otherState.getSketch());
            state.addMemoryUsage(getEstimatedKllInMemorySize(state.getSketch(), state.getType().getJavaType()));
        }
        else if (state.getSketch() == null) {
            state.setSketch(otherState.getSketch());
            state.addMemoryUsage(getEstimatedKllInMemorySize(otherState.getSketch(), state.getType().getJavaType()));
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

    private static <T> void initializeSketch(KllSketchAggregationState state, Supplier<Comparator<T>> comparator, Supplier<ArrayOfItemsSerDe<T>> serdeSupplier, long k)
    {
        if (k < 8 || k > MAX_K) {
            throw new PrestoException(INVALID_ARGUMENTS, format("k value must satisfy 8 <= k <= %d: %d", MAX_K, k));
        }
        if (state.getSketch() == null) {
            KllItemsSketch<T> sketch = KllItemsSketch.newHeapInstance((int) k, comparator.get(), serdeSupplier.get());
            state.setSketch(sketch);
            state.addMemoryUsage(getEstimatedKllInMemorySize(sketch, state.getType().getJavaType()));
        }
    }
}
