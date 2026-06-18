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
package com.facebook.presto.operator.aggregation.reservoirsample;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.JavaAggregationFunctionImplementation;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.executeAggregation;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestReservoirSampleAggregation
{
    protected FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();

    @Test
    public void testNoInitialSample()
    {
        assertAggregation(
                getDoubleFunction(),
                // seen count, and sample
                ImmutableList.of(5L, ImmutableList.of(1.0, 1.0)),
                // arguments
                copyBlock(BIGINT, nullBlock(), 5),
                copyBlock(BIGINT, bigintBlock(0), 5),
                doubleBlock(1, 1, 1, 1, 1),
                copyBlock(INTEGER, intBlock(2), 5));
    }

    @Test
    public void testLarge()
    {
        int sampleSize = 5000;
        int inputSize = 15_000;
        assertAggregation(
                getDoubleFunction(),
                // seen count, and sample
                ImmutableList.of((long) inputSize, IntStream.range(0, sampleSize).mapToObj(x -> 1.0).collect(Collectors.toList())),
                // arguments
                copyBlock(BIGINT, nullBlock(), inputSize),
                copyBlock(BIGINT, bigintBlock(0), inputSize),
                doubleBlock(IntStream.range(0, inputSize).mapToDouble(x -> 1.0).toArray()),
                copyBlock(INTEGER, intBlock(sampleSize), inputSize));
    }

    @DataProvider(name = "invalidSampleSize")
    public Object[][] invalidSampleParameters()
    {
        return new Object[][] {{0}, {-1}};
    }

    /**
     * Throws exception when desired sample size is <= 0
     */
    @Test(dataProvider = "invalidSampleSize", expectedExceptions = IllegalArgumentException.class)
    @Parameters("sampleSize")
    public void testInvalidSampleSize(int sampleSize)
    {
        assertAggregation(
                getDoubleFunction(),
                // seen count, and sample
                ImmutableList.of(-1L, ImmutableList.of(1.0, 1.0)),
                // arguments
                copyBlock(BIGINT, nullBlock(), 5),
                copyBlock(BIGINT, bigintBlock(0), 5),
                doubleBlock(1, 1, 1, 1, 1),
                copyBlock(INTEGER, intBlock(sampleSize), 5));
    }

    @Test
    public void testInitialSampleSameSize()
    {
        assertAggregation(
                getDoubleFunction(),
                // seen count, and sample
                ImmutableList.of(15L, ImmutableList.of(1.0, 1.0)),
                // arguments
                // initial sample
                arrayOfBlock(DOUBLE, doubleArrayBlock(1.0, 1.0), 5),
                // initial sample seen count
                copyBlock(BIGINT, bigintBlock(10), 5),
                // actual input values
                doubleBlock(1, 1, 1, 1, 1),
                // sample size
                copyBlock(INTEGER, intBlock(2), 5));
    }

    /**
     * Throws exception because the initial sample size is not equal to the desired sample size
     */
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInitialSampleWrongSize()
    {
        assertAggregation(
                getDoubleFunction(),
                // seen count, and sample
                ImmutableList.of(15L, ImmutableList.of(1.0, 1.0)),
                // arguments
                // initial sample
                arrayOfBlock(DOUBLE, doubleArrayBlock(1.0, 1.0, 2.0), 5),
                // initial sample seen count
                copyBlock(BIGINT, bigintBlock(10), 5),
                // actual input values
                doubleBlock(1, 1, 1, 1, 1),
                // sample size
                copyBlock(INTEGER, intBlock(2), 5));
    }

    /**
     * valid because when the initial sample was created there could have been less records than
     * the desired sample size.
     */
    @Test
    public void testInitialSampleSmallerThanMaxSize()
    {
        assertAggregation(
                getDoubleFunction(),
                // seen count, and sample
                ImmutableList.of(6L, ImmutableList.of(1.0, 1.0)),
                // arguments
                // initial sample
                arrayOfBlock(DOUBLE, doubleArrayBlock(1.0), 5),
                // initial sample seen count
                copyBlock(BIGINT, bigintBlock(1), 5),
                // actual input values
                doubleBlock(1, 1, 1, 1, 1),
                // sample size
                copyBlock(INTEGER, intBlock(2), 5));
    }

    /**
     * Throws exception because the processed count is less than the size of the initial sample
     */
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInitialSampleSeenCountSmallerThanInitialSample()
    {
        assertAggregation(
                getDoubleFunction(),
                // seen count, and sample
                ImmutableList.of(6L, ImmutableList.of(1.0, 1.0)),
                // arguments
                // initial sample
                arrayOfBlock(DOUBLE, doubleArrayBlock(1.0, 1.0), 5),
                // initial sample seen count
                copyBlock(BIGINT, bigintBlock(1), 5),
                // actual input values
                doubleBlock(1, 1, 1, 1, 1),
                // sample size
                copyBlock(INTEGER, intBlock(2), 5));
    }

    @Test
    public void testValidResults()
    {
        Object result = executeAggregation(
                getDoubleFunction(),
                // initial sample
                copyBlock(UNKNOWN, nullBlock(), 10),
                // initial sample seen count
                copyBlock(BIGINT, bigintBlock(0), 10),
                // actual input values
                doubleBlock(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
                // sample size
                copyBlock(INTEGER, intBlock(4), 10));
        Set<Double> items = IntStream.range(0, 10).boxed().map(Integer::doubleValue).collect(Collectors.toSet());
        assertTrue(result instanceof List);
        List<Object> resultItems = (List<Object>) result;
        Long processedCount = (Long) resultItems.get(0);
        assertEquals(processedCount, Long.valueOf(items.size()));
        List<Object> sample = (List<Object>) resultItems.get(1);
        assertTrue(items.containsAll(sample));
    }

    private JavaAggregationFunctionImplementation getFunction(Type... arguments)
    {
        return functionAndTypeManager.getJavaAggregateFunctionImplementation(functionAndTypeManager.lookupFunction("reservoir_sample", fromTypes(arguments)));
    }

    private JavaAggregationFunctionImplementation getDoubleFunction()
    {
        return getFunction(new ArrayType(DOUBLE), BIGINT, DOUBLE, INTEGER);
    }

    private static Block bigintBlock(long value)
    {
        return BIGINT.createBlockBuilder(null, 1).writeLong(value).build();
    }

    private static Block intBlock(int... values)
    {
        BlockBuilder blockBuilder = INTEGER.createBlockBuilder(null, values.length);
        Arrays.stream(values).forEach(value -> {
            INTEGER.writeLong(blockBuilder, value);
        });
        return blockBuilder.build();
    }

    private static Block doubleBlock(double... values)
    {
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(null, values.length);
        Arrays.stream(values).forEach(value -> {
            DOUBLE.writeDouble(blockBuilder, value);
        });
        return blockBuilder.build();
    }

    private static Block doubleArrayBlock(double... values)
    {
        BlockBuilder builder = DOUBLE.createBlockBuilder(null, values.length);
        Arrays.stream(values)
                .forEach(value -> {
                    DOUBLE.writeDouble(builder, value);
                });
        return builder.build();
    }

    private static Block arrayOfBlock(Type innerType, Block value, int count)
    {
        Type arrayType = new ArrayType(innerType);
        BlockBuilder builder = arrayType.createBlockBuilder(null, count);
        for (int i = 0; i < count; i++) {
            builder.appendStructure(value);
        }
        return builder.build();
    }

    private static Block nullBlock()
    {
        return DOUBLE.createBlockBuilder(null, 1).appendNull().build();
    }

    private static Block copyBlock(Type type, Block value, int positionCount)
    {
        BlockBuilder builder = type.createBlockBuilder(null, positionCount);
        for (int i = 0; i < positionCount; i++) {
            type.appendTo(value, 0, builder);
        }
        return builder.build();
    }
}
