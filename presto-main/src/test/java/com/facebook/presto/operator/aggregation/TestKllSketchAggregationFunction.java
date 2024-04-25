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
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.JavaAggregationFunctionImplementation;
import org.apache.datasketches.common.ArrayOfBooleansSerDe;
import org.apache.datasketches.common.ArrayOfDoublesSerDe;
import org.apache.datasketches.common.ArrayOfLongsSerDe;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.kll.KllItemsSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.intellij.lang.annotations.Language;
import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestKllSketchAggregationFunction
        extends AbstractTestFunctions
{
    private static final MetadataManager metadata = MetadataManager.createTestMetadataManager();
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = metadata.getFunctionAndTypeManager();
    private static final JavaAggregationFunctionImplementation DOUBLE_FUNCTION = getFunction(DOUBLE);
    private static final JavaAggregationFunctionImplementation DOUBLE_WITH_K_FUNCTION = getFunction("sketch_kll_with_k", DOUBLE, BIGINT);
    private static final JavaAggregationFunctionImplementation BIGINT_FUNCTION = getFunction(BIGINT);
    private static final JavaAggregationFunctionImplementation VARCHAR_FUNCTION = getFunction(VARCHAR);
    private static final JavaAggregationFunctionImplementation BOOLEAN_FUNCTION = getFunction(BOOLEAN);

    @Test
    public void testDouble()
    {
        double[] items = DoubleStream.iterate(0, i -> i + ThreadLocalRandom.current().nextDouble()).limit(100).toArray();
        BlockBuilder out = DOUBLE.createBlockBuilder(null, items.length);
        KllItemsSketch<Double> sketch = KllItemsSketch.newHeapInstance(Double::compareTo, new ArrayOfDoublesSerDe());
        Arrays.stream(items).forEach(item -> {
            DOUBLE.writeDouble(out, item);
            sketch.update(item);
        });
        Block input = out.build();
        SqlVarbinary result = (SqlVarbinary) AggregationTestUtils.executeAggregation(
                DOUBLE_FUNCTION,
                input);
        KllItemsSketch<Double> recreated = KllItemsSketch.wrap(WritableMemory.writableWrap((result.getBytes())), Double::compareTo, new ArrayOfDoublesSerDe());
        checkSketchesEqual(DoubleStream.of(items).boxed().toArray(Double[]::new), sketch, recreated);
    }

    @Test
    public void testDoubleWithK()
    {
        double[] items = DoubleStream.iterate(0, i -> i + ThreadLocalRandom.current().nextDouble()).limit(100).toArray();
        BlockBuilder out = DOUBLE.createBlockBuilder(null, items.length);
        BlockBuilder kBlock = BIGINT.createBlockBuilder(null, items.length);
        int k = 150;
        KllItemsSketch<Double> sketch = KllItemsSketch.newHeapInstance(k, Double::compareTo, new ArrayOfDoublesSerDe());
        Arrays.stream(items).forEach(item -> {
            DOUBLE.writeDouble(out, item);
            sketch.update(item);
            BIGINT.writeLong(kBlock, k);
        });
        Block input = out.build();
        SqlVarbinary result = (SqlVarbinary) AggregationTestUtils.executeAggregation(
                DOUBLE_WITH_K_FUNCTION,
                input,
                kBlock.build());
        KllItemsSketch<Double> recreated = KllItemsSketch.wrap(WritableMemory.writableWrap((result.getBytes())), Double::compareTo, new ArrayOfDoublesSerDe());
        checkSketchesEqual(DoubleStream.of(items).boxed().toArray(Double[]::new), sketch, recreated);
    }

    @Test
    public void testInvalidK()
    {
        double[] items = DoubleStream.iterate(0, i -> i + ThreadLocalRandom.current().nextDouble()).limit(10).toArray();
        BlockBuilder inputBlock = DOUBLE.createBlockBuilder(null, items.length);
        BlockBuilder kBlockLow = BIGINT.createBlockBuilder(null, items.length);
        Arrays.stream(items).forEach(item -> {
            DOUBLE.writeDouble(inputBlock, item);
            BIGINT.writeLong(kBlockLow, 7);
        });
        Block input = inputBlock.build();
        assertThrows(() -> AggregationTestUtils.executeAggregation(
                DOUBLE_WITH_K_FUNCTION,
                inputBlock.build(),
                kBlockLow.build()), PrestoException.class, "k value must satisfy 8 <= k <= 65535: 7");

        BlockBuilder kBlockHigh = BIGINT.createBlockBuilder(null, items.length);
        Arrays.stream(items).forEach(item -> {
            BIGINT.writeLong(kBlockHigh, 65536);
        });
        assertThrows(() -> AggregationTestUtils.executeAggregation(
                DOUBLE_WITH_K_FUNCTION,
                input,
                kBlockHigh.build()), PrestoException.class, "k value must satisfy 8 <= k <= 65535: 65536");
    }

    @Test
    public void testBigint()
    {
        Long[] items = LongStream.iterate(0, i -> i + ThreadLocalRandom.current().nextLong(0, 100)).limit(100).boxed().toArray(Long[]::new);
        BlockBuilder out = BIGINT.createBlockBuilder(null, items.length);
        KllItemsSketch<Long> sketch = KllItemsSketch.newHeapInstance(Long::compareTo, new ArrayOfLongsSerDe());
        Arrays.stream(items).forEach(item -> {
            BIGINT.writeLong(out, item);
            sketch.update(item);
        });
        Block input = out.build();
        SqlVarbinary result = (SqlVarbinary) AggregationTestUtils.executeAggregation(
                BIGINT_FUNCTION,
                input);
        KllItemsSketch<Long> recreated = KllItemsSketch.wrap(WritableMemory.writableWrap((result.getBytes())), Long::compareTo, new ArrayOfLongsSerDe());
        checkSketchesEqual(items, sketch, recreated);
    }

    @Test
    public void testVarchar()
    {
        String[] items = "abcdefghijklmnopqrstuvwxyz".split("");
        BlockBuilder out = VARCHAR.createBlockBuilder(null, items.length);
        KllItemsSketch<String> sketch = KllItemsSketch.newHeapInstance(String::compareTo, new ArrayOfStringsSerDe());
        Arrays.stream(items).forEach(item -> {
            VARCHAR.writeString(out, item);
            sketch.update(item);
        });
        Block input = out.build();
        SqlVarbinary result = (SqlVarbinary) AggregationTestUtils.executeAggregation(
                VARCHAR_FUNCTION,
                input);
        KllItemsSketch<String> recreated = KllItemsSketch.wrap(Memory.wrap(result.getBytes()), String::compareTo, new ArrayOfStringsSerDe());
        checkSketchesEqual(items, sketch, recreated);
    }

    @Test
    public void testBoolean()
    {
        Boolean[] items = IntStream.iterate(0, i -> i + 1).limit(10).mapToObj(i -> i % 2 == 0).toArray(Boolean[]::new);
        BlockBuilder out = BOOLEAN.createBlockBuilder(null, items.length);
        KllItemsSketch<Boolean> sketch = KllItemsSketch.newHeapInstance(Boolean::compareTo, new ArrayOfBooleansSerDe());
        Arrays.stream(items).forEach(item -> {
            sketch.update(item);
            BOOLEAN.writeBoolean(out, item);
        });
        Block input = out.build();
        SqlVarbinary result = (SqlVarbinary) AggregationTestUtils.executeAggregation(
                BOOLEAN_FUNCTION,
                input);
        KllItemsSketch<Boolean> recreated = KllItemsSketch.wrap(Memory.wrap(result.getBytes()), Boolean::compareTo, new ArrayOfBooleansSerDe());
        checkSketchesEqual(items, sketch, recreated);
    }

    @Test
    public void testEmptyInput()
    {
        AggregationTestUtils.assertAggregation(DOUBLE_FUNCTION,
                null,
                DOUBLE.createBlockBuilder(null, 0).build());
    }

    private static void assertThrows(ThrowingRunnable runnable, Class<?> exceptionType, @Language("regexp") String regex)
    {
        try {
            runnable.run();
            throw new AssertionError("no exception was thrown");
        }
        catch (Throwable e) {
            assertEquals(e.getClass(), exceptionType);
            assertTrue(Optional.ofNullable(e.getMessage()).orElse("").matches(regex), format("Error message: '%s' didn't match regex: '%s'", e.getMessage(), regex));
        }
    }

    private static <T> void checkSketchesEqual(T[] items, KllItemsSketch<T> expected, KllItemsSketch<T> actual)
    {
        Arrays.stream(items).forEach(item -> assertEquals(actual.getRank(item), expected.getRank(item), 1E-8));

        assertEquals(actual.getSortedView().getCumulativeWeights(), expected.getSortedView().getCumulativeWeights(), "weights are not equal");
        assertEquals(actual.getSortedView().getQuantiles(), expected.getSortedView().getQuantiles(), "quantiles are not equal");
    }

    private static JavaAggregationFunctionImplementation getFunction(Type... types)
    {
        return getFunction("sketch_kll", types);
    }

    private static JavaAggregationFunctionImplementation getFunction(String name, Type... types)
    {
        return FUNCTION_AND_TYPE_MANAGER.getJavaAggregateFunctionImplementation(
                metadata.getFunctionAndTypeManager()
                        .lookupFunction(name, fromTypes(types)));
    }
}
