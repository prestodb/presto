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
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.aggregation.sketch.kll.KllSketchAggregationState;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.JavaAggregationFunctionImplementation;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.datasketches.common.ArrayOfDoublesSerDe;
import org.apache.datasketches.kll.KllItemsSketch;
import org.apache.datasketches.memory.WritableMemory;
import org.intellij.lang.annotations.Language;
import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestKllSketchAggregationFunction
        extends AbstractTestFunctions
{
    private static final MetadataManager metadata = MetadataManager.createTestMetadataManager();
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = metadata.getFunctionAndTypeManager();
    private static final DecimalType SHORT_DECIMAL = createDecimalType(7, 2);
    private static final DecimalType LONG_DECIMAL = createDecimalType(19, 2);
    private static final JavaAggregationFunctionImplementation DOUBLE_FUNCTION = getFunction(DOUBLE);
    private static final JavaAggregationFunctionImplementation DOUBLE_WITH_K_FUNCTION = getFunction("sketch_kll_with_k", DOUBLE, BIGINT);

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
        checkSketchesEqual(Arrays.stream(items).boxed().collect(toImmutableList()), sketch, recreated);
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
        checkSketchesEqual(DoubleStream.of(items).boxed().collect(Collectors.toList()), sketch, recreated);
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

    @DataProvider(name = "testTypes")
    public Object[][] testTypesProvider()
    {
        return new Object[][] {
                {BIGINT, LongStream.iterate(0, i -> i + ThreadLocalRandom.current().nextLong()).boxed(), (BiConsumer<BlockBuilder, Long>) BIGINT::writeLong},
                {INTEGER, IntStream.iterate(0, i -> i + ThreadLocalRandom.current().nextInt()).mapToLong(x -> (long) x).boxed(),
                        (BiConsumer<BlockBuilder, Long>) INTEGER::writeLong},
                {SMALLINT, IntStream.iterate(0, i -> i + ThreadLocalRandom.current().nextInt(0, Short.MAX_VALUE - i)).mapToLong(x -> (long) x).boxed(),
                        (BiConsumer<BlockBuilder, Long>) SMALLINT::writeLong},
                {TINYINT, IntStream.iterate(0, i -> i + ThreadLocalRandom.current().nextInt(0, Byte.MAX_VALUE - i)).mapToLong(x -> (long) x).boxed(),
                        (BiConsumer<BlockBuilder, Long>) TINYINT::writeLong},
                {INTEGER, IntStream.iterate(0, i -> i + ThreadLocalRandom.current().nextInt()).mapToLong(x -> (long) x).boxed(),
                        (BiConsumer<BlockBuilder, Long>) INTEGER::writeLong},
                {DOUBLE, DoubleStream.iterate(0, i -> i + ThreadLocalRandom.current().nextDouble()).boxed(), (BiConsumer<BlockBuilder, Double>) DOUBLE::writeDouble},
                {REAL, DoubleStream.iterate(0, i -> i + ThreadLocalRandom.current().nextFloat()).boxed().map(x -> (long) Float.floatToIntBits((float) (double) x)),
                        (BiConsumer<BlockBuilder, Long>) REAL::writeLong},
                {SHORT_DECIMAL, LongStream.iterate(0, i -> i + ThreadLocalRandom.current().nextLong()).boxed(), (BiConsumer<BlockBuilder, Long>) SHORT_DECIMAL::writeLong},
                {LONG_DECIMAL, LongStream.iterate(0, i -> i + ThreadLocalRandom.current().nextLong()).boxed().map(Decimals::encodeUnscaledValue),
                        (BiConsumer<BlockBuilder, Slice>) LONG_DECIMAL::writeSlice},
                {VARCHAR, Arrays.stream("abcdefghijklmnopqrstuvwxyz".split("")).map(Slices::utf8Slice), (BiConsumer<BlockBuilder, Slice>) VARCHAR::writeSlice},
                {BOOLEAN, IntStream.iterate(0, i -> i + 1).limit(10).mapToObj(i -> i % 2 == 0), (BiConsumer<BlockBuilder, Boolean>) BOOLEAN::writeBoolean},
                {DATE, LongStream.iterate(0, i -> i + 1).boxed(), (BiConsumer<BlockBuilder, Long>) DATE::writeLong},
                {TIME, LongStream.iterate(0, i -> i + 1).boxed(), (BiConsumer<BlockBuilder, Long>) TIME::writeLong},
                {TIMESTAMP, LongStream.iterate(0, i -> i + 1).boxed(), (BiConsumer<BlockBuilder, Long>) TIMESTAMP::writeLong},
                {TIMESTAMP_WITH_TIME_ZONE, LongStream.iterate(0, i -> i + 1).boxed(), (BiConsumer<BlockBuilder, Long>) TIMESTAMP_WITH_TIME_ZONE::writeLong}
        };
    }

    @Test(dataProvider = "testTypes")
    public void testTypes(Type type, Stream<Object> values, BiConsumer<BlockBuilder, Object> writeBlockValue)
    {
        int length = 100;
        JavaAggregationFunctionImplementation function = getFunction(type);
        BlockBuilder out = type.createBlockBuilder(null, length);
        KllSketchAggregationState.SketchParameters parameters = KllSketchAggregationState.getSketchParameters(type);
        KllItemsSketch sketch = KllItemsSketch.<Object>newHeapInstance(parameters.getComparator(), parameters.getSerde());
        List addedValues = values.limit(length)
                .map(item -> {
                    writeBlockValue.accept(out, item);
                    sketch.update(parameters.getConversion().apply(item));
                    return item;
                })
                .collect(Collectors.toList());
        Block input = out.build();
        SqlVarbinary result = (SqlVarbinary) AggregationTestUtils.executeAggregation(
                function,
                input);
        KllItemsSketch recreated = KllItemsSketch.wrap(WritableMemory.writableWrap((result.getBytes())), parameters.getComparator(), parameters.getSerde());
        List sketchItems = (List<Object>) addedValues.stream().map(parameters.getConversion()::apply).collect(Collectors.toList());
        checkSketchesEqual(sketchItems, sketch, recreated);
    }

    @Test
    public void testEmptyInput()
    {
        assertAggregation(DOUBLE_FUNCTION,
                null,
                DOUBLE.createBlockBuilder(null, 0).build());
    }

    @Test
    public void testNulls()
    {
        // test no exception is thrown
        assertAggregation(DOUBLE_FUNCTION,
                null,
                DOUBLE.createBlockBuilder(null, 2)
                        .appendNull()
                        .appendNull()
                        .build());
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

    private static <T> void checkSketchesEqual(List<T> items, KllItemsSketch<T> expected, KllItemsSketch<T> actual)
    {
        items.forEach(item -> assertEquals(actual.getRank(item), expected.getRank(item), 1E-8));

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
