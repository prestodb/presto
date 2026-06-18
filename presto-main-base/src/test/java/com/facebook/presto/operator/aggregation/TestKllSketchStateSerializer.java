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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.sketch.kll.KllSketchAggregationState;
import com.facebook.presto.operator.aggregation.sketch.kll.KllSketchStateSerializer;
import com.google.common.collect.ImmutableList;
import org.apache.datasketches.common.ArrayOfBooleansSerDe;
import org.apache.datasketches.common.ArrayOfDoublesSerDe;
import org.apache.datasketches.common.ArrayOfLongsSerDe;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.kll.KllItemsSketch;
import org.openjdk.jol.info.GraphLayout;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestKllSketchStateSerializer
{
    @Test
    public void testDouble()
    {
        testSerializer(DOUBLE,
                () -> KllItemsSketch.newHeapInstance(Double::compareTo, new ArrayOfDoublesSerDe()),
                DoubleStream.iterate(0, i -> i + 1).limit(10).boxed().collect(toImmutableList()));
    }

    @Test
    public void testString()
    {
        testSerializer(VARCHAR,
                () -> KllItemsSketch.newHeapInstance(String::compareTo, new ArrayOfStringsSerDe()),
                Arrays.asList("abcdefghijklmnopqrstuvwxyz".split("")));
    }

    @Test
    public void testBigint()
    {
        testSerializer(BIGINT,
                () -> KllItemsSketch.newHeapInstance(Long::compareTo, new ArrayOfLongsSerDe()),
                LongStream.iterate(0, i -> i + 1).limit(10).boxed().collect(toImmutableList()));
    }

    @Test
    public void testBoolean()
    {
        testSerializer(BOOLEAN,
                () -> KllItemsSketch.newHeapInstance(Boolean::compareTo, new ArrayOfBooleansSerDe()),
                LongStream.iterate(0, i -> i + 1).limit(10).mapToObj(i -> i % 2 == 0).collect(toImmutableList()));
    }

    @Test
    public void testEstimatedMemorySizeDouble()
    {
        testEstimatedMemorySize(DOUBLE, i -> (double) i, .05);
    }

    @Test
    public void testEstimatedMemorySizeLong()
    {
        testEstimatedMemorySize(BIGINT, i -> (long) i, .05);
    }

    @Ignore("The memory size of a string-typed sketch is dependent upon the strings used as input, so this test doesn't verify much")
    @Test
    public void testEstimatedMemorySizeString()
    {
        testEstimatedMemorySize(VARCHAR, i -> "abcdefghijklmnopqrstuvwxyz".substring(0, i.hashCode() % 26), .05);
    }

    @Test
    public void testEstimatedMemorySizeBoolean()
    {
        testEstimatedMemorySize(BOOLEAN, i -> i % 2 == 0, 0.5);
    }

    private <T> void testEstimatedMemorySize(Type type, Function<Integer, T> generator, double tolerance)
    {
        KllSketchAggregationState state = new KllSketchAggregationState.Single(type);
        KllSketchAggregationState.SketchParameters parameters = KllSketchAggregationState.getSketchParameters(type);
        KllItemsSketch<T> sketch = KllItemsSketch.newHeapInstance(parameters.getComparator(), parameters.getSerde());
        List<Integer> sizes = ImmutableList.of(512, 2048, 4096, 16384);
        for (int size : sizes) {
            IntStream.range(0, size).boxed().map(generator).forEach(sketch::update);
            long trueSize = GraphLayout.parseInstance(sketch).totalSize();
            state.setSketch(sketch);
            long estimatedSize = state.getEstimatedSize();
            // size should be within margin of the actual size
            double errorPercent = (double) Math.abs(estimatedSize - trueSize) / trueSize;
            assertTrue(errorPercent < tolerance, String.format("estimated memory size error for sketch stream size %d was > 5%%: %.2f", size, errorPercent));
        }
    }

    @SuppressWarnings("unchecked")
    private <T> void testSerializer(Type type, Supplier<KllItemsSketch<?>> sketchSupplier, Collection<T> sketchInputs)
    {
        KllSketchStateSerializer serializer = new KllSketchStateSerializer(type);
        KllSketchAggregationState state = new KllSketchAggregationState.Single(type);

        // serialize empty
        state.setSketch(sketchSupplier.get());
        BlockBuilder blockBuilder = VARBINARY.createBlockBuilder(null, 1);
        serializer.serialize(state, blockBuilder);
        KllSketchAggregationState deserialized = new KllSketchAggregationState.Single(type);
        serializer.deserialize(blockBuilder.build(), 0, deserialized);

        // serialize with data
        KllItemsSketch sketch = state.getSketch();
        KllItemsSketch actual = sketchSupplier.get();
        sketchInputs.stream().forEach(i -> {
            sketch.update(i);
            actual.update(i);
        });
        blockBuilder = VARBINARY.createBlockBuilder(null, 1);
        state.setSketch(sketch);
        serializer.serialize(state, blockBuilder);
        Block serializedBlock = blockBuilder.build();
        KllSketchAggregationState newState = new KllSketchAggregationState.Single(type);
        serializer.deserialize(serializedBlock, 0, newState);
        verifySketches(actual, newState.getSketch());
    }

    private static void verifySketches(KllItemsSketch<?> expected, KllItemsSketch<?> actual)
    {
        assertEquals(actual.getSortedView().getCumulativeWeights(), expected.getSortedView().getCumulativeWeights(), "weights are not equal");
        assertEquals(actual.getSortedView().getQuantiles(), expected.getSortedView().getQuantiles(), "quantiles are not equal");
        assertEquals(actual.toByteArray(), expected.toByteArray());
    }
}
