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
package com.facebook.presto.type.khyperloglog;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.function.AggregationFunction;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createSlicesBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;

public class TestKHyperLogLogAggregationFunction
{
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = MetadataManager.createTestMetadataManager().getFunctionAndTypeManager();
    private static final String NAME = KHyperLogLogAggregationFunction.class.getAnnotation(AggregationFunction.class).value();

    @Test
    public void testSimpleKHyperLogLog()
    {
        int sampleSize = 100;
        List<Long> longs = generateLongs(sampleSize);
        List<Slice> strings = generateStringSlices(sampleSize);
        List<Double> doubles = generateDoubles(sampleSize);

        testAggregation(BIGINT, longs, BIGINT, longs);

        testAggregation(BIGINT, longs, VARCHAR, strings);

        testAggregation(VARCHAR, strings, BIGINT, longs);

        testAggregation(VARCHAR, strings, VARCHAR, strings);

        testAggregation(DOUBLE, doubles, BIGINT, longs);

        testAggregation(DOUBLE, doubles, VARCHAR, strings);
    }

    @Test
    public void testBigKHyperLogLog()
    {
        int sampleSize = 100000;
        List<Long> longs = generateLongs(sampleSize);
        List<Slice> strings = generateStringSlices(sampleSize);
        List<Double> doubles = generateDoubles(sampleSize);

        testAggregation(BIGINT, longs, BIGINT, longs);

        testAggregation(BIGINT, longs, VARCHAR, strings);

        testAggregation(VARCHAR, strings, BIGINT, longs);

        testAggregation(VARCHAR, strings, VARCHAR, strings);

        testAggregation(DOUBLE, doubles, BIGINT, longs);

        testAggregation(DOUBLE, doubles, VARCHAR, strings);
    }

    @Test
    public void testKHyperLogLogWithSomeNulls()
    {
        int sampleSize = 3;
        List<Long> longs = generateLongs(sampleSize);
        List<Slice> strings = generateStringSlices(sampleSize);
        List<Double> doubles = generateDoubles(sampleSize);

        includeNulls(longs);
        includeNulls(strings);
        includeNulls(doubles);

        testAggregation(BIGINT, longs, BIGINT, longs);

        testAggregation(BIGINT, longs, VARCHAR, strings);

        testAggregation(VARCHAR, strings, BIGINT, longs);

        testAggregation(VARCHAR, strings, VARCHAR, strings);

        testAggregation(DOUBLE, doubles, BIGINT, longs);

        testAggregation(DOUBLE, doubles, VARCHAR, strings);
    }

    @Test
    public void testKHyperLogLogWithNullColumn()
    {
        int sampleSize = 3;
        List<Long> longs = generateLongs(sampleSize);
        List<Slice> strings = generateStringSlices(sampleSize);
        List<Double> doubles = generateDoubles(sampleSize);
        List<Object> nulls = generateNulls(sampleSize);

        testAggregation(BIGINT, nulls, BIGINT, longs);
        testAggregation(BIGINT, longs, BIGINT, nulls);

        testAggregation(BIGINT, nulls, VARCHAR, strings);
        testAggregation(BIGINT, longs, VARCHAR, nulls);

        testAggregation(VARCHAR, nulls, BIGINT, longs);
        testAggregation(VARCHAR, strings, BIGINT, nulls);

        testAggregation(VARCHAR, nulls, VARCHAR, strings);
        testAggregation(VARCHAR, strings, VARCHAR, nulls);

        testAggregation(DOUBLE, nulls, BIGINT, longs);
        testAggregation(DOUBLE, doubles, BIGINT, nulls);

        testAggregation(DOUBLE, nulls, VARCHAR, strings);
        testAggregation(DOUBLE, doubles, VARCHAR, nulls);
    }

    private void testAggregation(Type valueType, List<?> values, Type uiiType, List<?> uiis)
    {
        InternalAggregationFunction aggregationFunction = getAggregation(valueType, uiiType);
        KHyperLogLog khll = null;
        long value;
        long uii;

        for (int i = 0; i < values.size(); i++) {
            if (values.get(i) == null || uiis.get(i) == null) {
                continue;
            }

            if (khll == null) {
                khll = new KHyperLogLog();
            }

            value = toLong(values.get(i), valueType);
            uii = toLong(uiis.get(i), uiiType);
            if (valueType == VARCHAR) {
                khll.add((Slice) values.get(i), uii);
            }
            else {
                khll.add(value, uii);
            }
        }

        assertAggregation(
                aggregationFunction,
                (khll == null) ? null : new SqlVarbinary(khll.serialize().getBytes()),
                buildBlock(values, valueType),
                buildBlock(uiis, uiiType));
    }

    private long toLong(Object value, Type type)
    {
        if (type == DOUBLE) {
            return Double.doubleToLongBits((double) value);
        }
        else if (type == VARCHAR) {
            return XxHash64.hash((Slice) value);
        }
        else {
            return (long) value;
        }
    }

    private Block buildBlock(List<?> values, Type type)
    {
        if (type == DOUBLE) {
            return createDoublesBlock(values.stream().map(o -> (Double) o).collect(Collectors.toList()));
        }
        else if (type == VARCHAR) {
            return createSlicesBlock(values.stream().map(o -> (Slice) o).collect(Collectors.toList()));
        }
        else {
            return createLongsBlock(values.stream().map(o -> (Long) o).collect(Collectors.toList()));
        }
    }

    private List<Slice> buildStringSliceList(List<String> strings)
    {
        return strings.stream().map(this::stringToSlice).collect(Collectors.toList());
    }

    private Slice stringToSlice(String s)
    {
        if (s == null) {
            return null;
        }
        return Slices.utf8Slice(s);
    }

    private static InternalAggregationFunction getAggregation(Type... arguments)
    {
        return FUNCTION_AND_TYPE_MANAGER.getAggregateFunctionImplementation(FUNCTION_AND_TYPE_MANAGER.lookupFunction(NAME, fromTypes(arguments)));
    }

    private List<Long> generateLongs(int size)
    {
        Random generator = new Random(13);
        return generator.longs(size).boxed().collect(Collectors.toList());
    }

    private List<Slice> generateStringSlices(int size)
    {
        Random generator = new Random(123);
        return buildStringSliceList(generator.longs(size).boxed().map(Long::toHexString).collect(Collectors.toList()));
    }

    private List<Double> generateDoubles(int size)
    {
        Random generator = new Random(123);
        return generator.doubles(size).boxed().collect(Collectors.toList());
    }

    private List<Object> generateNulls(int size)
    {
        return Arrays.asList(new Object[size]);
    }

    private <K> List<K> includeNulls(List<K> values)
    {
        Random generator = new Random(123);
        for (int i = 0; i < values.size(); i++) {
            if (generator.nextDouble() < 0.2) {
                values.set(i, null);
            }
        }
        return values;
    }
}
