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
package com.facebook.presto.operator.aggregation.noisyaggregation;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.SfmSketch;
import com.facebook.presto.type.IntegerOperators;
import org.testng.annotations.Test;

import java.util.Objects;
import java.util.function.BiFunction;

import static com.facebook.presto.block.BlockAssertions.createDoubleRepeatBlock;
import static com.facebook.presto.block.BlockAssertions.createLongRepeatBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static java.lang.Math.min;

/**
 * Tests for the noisy_approx_set_sfm function.
 * Overall, these are similar to the tests of noisy_approx_distinct_sfm, but with an extra check
 * to ensure that the size of the returned sketch matches the parameters specified (or defaulted).
 */
public class TestNoisyApproximateSetSfmFromIndexAndZerosAggregation
        extends AbstractTestNoisySfmAggregation
{
    protected String getFunctionName()
    {
        return "noisy_approx_set_sfm_from_index_and_zeros";
    }

    protected long getCardinalityFromResult(Object result)
    {
        return getSketchFromResult(result).cardinality();
    }

    private boolean sketchSizesMatch(Object a, Object b)
    {
        SfmSketch sketchA = getSketchFromResult(a);
        SfmSketch sketchB = getSketchFromResult(b);
        return sketchA.getBitmap().length() == sketchB.getBitmap().length();
    }

    public static Block createLongSequenceIndexBlock(int start, int end, int indexBits)
    {
        int shift = 64 - indexBits;
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            long h = IntegerOperators.xxHash64(i);
            BIGINT.writeLong(builder, h >>> shift);
        }

        return builder.build();
    }

    public static Block createLongSequenceZerosBlock(int start, int end, int indexBits, int precision)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(end - start);
        precision = min(precision - 1, 64 - indexBits);

        for (int i = start; i < end; i++) {
            long h = IntegerOperators.xxHash64(i);
            BIGINT.writeLong(builder, min(Long.numberOfTrailingZeros(h), precision));
        }

        return builder.build();
    }

    /**
     * Run assertion on function with signature F(value1, value2, epsilon, numberOfBuckets, precision)
     */
    protected void assertFunction(Block valuesBlock1, Type valueType1, Block valuesBlock2, Type valueType2, double epsilon, int numberOfBuckets, int precision, BiFunction<Object, Object, Boolean> assertion, Object expected)
    {
        assert (valuesBlock1.getPositionCount() == valuesBlock2.getPositionCount());
        assertAggregation(
                getAggregator(getFunctionName(), valueType1, valueType2, DOUBLE, BIGINT, BIGINT),
                assertion,
                null,
                new Page(
                        valuesBlock1,
                        valuesBlock2,
                        createDoubleRepeatBlock(epsilon, valuesBlock1.getPositionCount()),
                        createLongRepeatBlock(numberOfBuckets, valuesBlock1.getPositionCount()),
                        createLongRepeatBlock(precision, valuesBlock1.getPositionCount())),
                expected);
    }

    /**
     * Run assertion on function with signature F(value1, value2, epsilon, numberOfBuckets)
     */
    protected void assertFunction(Block valuesBlock1, Type valueType1, Block valuesBlock2, Type valueType2, double epsilon, int numberOfBuckets, BiFunction<Object, Object, Boolean> assertion, Object expected)
    {
        assert (valuesBlock1.getPositionCount() == valuesBlock2.getPositionCount());
        assertAggregation(
                getAggregator(getFunctionName(), valueType1, valueType2, DOUBLE, BIGINT),
                assertion,
                null,
                new Page(
                        valuesBlock1,
                        valuesBlock2,
                        createDoubleRepeatBlock(epsilon, valuesBlock1.getPositionCount()),
                        createLongRepeatBlock(numberOfBuckets, valuesBlock1.getPositionCount())),
                expected);
    }

    /**
     * Run assertion on function with signature F(value1, value2, epsilon)
     */
    protected void assertFunction(Block valuesBlock1, Type valueType1, Block valuesBlock2, Type valueType2, double epsilon, BiFunction<Object, Object, Boolean> assertion, Object expected)
    {
        assert (valuesBlock1.getPositionCount() == valuesBlock2.getPositionCount());
        assertAggregation(
                getAggregator(getFunctionName(), valueType1, valueType2, DOUBLE),
                assertion,
                null,
                new Page(
                        valuesBlock1,
                        valuesBlock2,
                        createDoubleRepeatBlock(epsilon, valuesBlock1.getPositionCount())),
                expected);
    }

    protected void assertEquivalence(Block valuesBlock1, Type valueType1, Block valuesBlock2, Type valueType2, double epsilon, int numberOfBuckets, Object expected)
    {
        assertFunction(valuesBlock1, valueType1, valuesBlock2, valueType2, epsilon, numberOfBuckets, Objects::equals, expected);
    }

    /**
     * Assert (approximate) cardinality match on function with signature F(value1, value2, epsilon, numberOfBuckets, precision)
     */
    protected void assertCardinality(Block valuesBlock1, Type valueType1, Block valuesBlock2, Type valueType2, double epsilon, int numberOfBuckets, int precision, Object expected, long delta)
    {
        assertFunction(valuesBlock1, valueType1, valuesBlock2, valueType2, epsilon, numberOfBuckets, precision,
                (actualValue, expectedValue) -> equalCardinalityWithAbsoluteError(actualValue, expectedValue, delta),
                expected);
    }

    /**
     * Assert (approximate) cardinality match on function with signature F(value1, value2, epsilon, numberOfBuckets)
     */
    protected void assertCardinality(Block valuesBlock1, Type valueType1, Block valuesBlock2, Type valueType2, double epsilon, int numberOfBuckets, Object expected, long delta)
    {
        assertFunction(valuesBlock1, valueType1, valuesBlock2, valueType2, epsilon, numberOfBuckets,
                (actualValue, expectedValue) -> equalCardinalityWithAbsoluteError(actualValue, expectedValue, delta),
                expected);
    }

    private void assertSketchSize(Block valuesBlock1, Type valueType1, Block valuesBlock2, Type valueType2, double epsilon, int numberOfBuckets, int precision, SqlVarbinary expected)
    {
        assertFunction(valuesBlock1, valueType1, valuesBlock2, valueType2, epsilon, numberOfBuckets, precision, this::sketchSizesMatch, expected);
    }

    private void assertSketchSize(Block valuesBlock1, Type valueType1, Block valuesBlock2, Type valueType2, double epsilon, int numberOfBuckets, SqlVarbinary expected)
    {
        assertFunction(valuesBlock1, valueType1, valuesBlock2, valueType2, epsilon, numberOfBuckets, this::sketchSizesMatch, expected);
    }

    /**
     * Verify SFMs created using the value or using index and zeros are identical
     */
    @Test
    public void testSfmEquivalence()
    {
        SqlVarbinary refSketch = toSqlVarbinary(createLongSketch(8192, 24, 1, 100_000));

        Block indexValuesBlock = createLongSequenceIndexBlock(1, 100_000, 13);
        Block zerosValuesBlock = createLongSequenceZerosBlock(1, 100_000, 13, 24);

        assertEquivalence(indexValuesBlock, BIGINT, zerosValuesBlock, BIGINT, SfmSketch.NON_PRIVATE_EPSILON, 8192, refSketch);
    }

    @Test
    public void testNonPrivateInteger()
    {
        Block indexValuesBlockBits13 = createLongSequenceIndexBlock(1, 100_000, 13);
        Block zerosValuesBlockBits13 = createLongSequenceZerosBlock(1, 100_000, 13, 24);

        SqlVarbinary refSketch = toSqlVarbinary(createLongSketch(8192, 24, 1, 100_000));
        assertCardinality(indexValuesBlockBits13, BIGINT, zerosValuesBlockBits13, BIGINT, SfmSketch.NON_PRIVATE_EPSILON, 8192, refSketch, 0);
        assertSketchSize(indexValuesBlockBits13, BIGINT, zerosValuesBlockBits13, BIGINT, SfmSketch.NON_PRIVATE_EPSILON, 8192, refSketch);

        Block indexValuesBlockBits11 = createLongSequenceIndexBlock(1, 100_000, 11);
        Block zerosValuesBlockBits11 = createLongSequenceZerosBlock(1, 100_000, 11, 32);

        refSketch = toSqlVarbinary(createLongSketch(2048, 32, 1, 100_000));
        assertCardinality(indexValuesBlockBits11, BIGINT, zerosValuesBlockBits11, BIGINT, SfmSketch.NON_PRIVATE_EPSILON, 2048, 32, refSketch, 0);
        assertSketchSize(indexValuesBlockBits11, BIGINT, zerosValuesBlockBits11, BIGINT, SfmSketch.NON_PRIVATE_EPSILON, 2048, 32, refSketch);
    }

    @Test
    public void testPrivateInteger()
    {
        Block indexValuesBlockBits13 = createLongSequenceIndexBlock(1, 100_000, 13);
        Block zerosValuesBlockBits13 = createLongSequenceZerosBlock(1, 100_000, 13, 24);

        SqlVarbinary refSketch = toSqlVarbinary(createLongSketch(8192, 24, 1, 100_000));
        assertCardinality(indexValuesBlockBits13, BIGINT, zerosValuesBlockBits13, BIGINT, 8, 8192, refSketch, 50_000);
        assertSketchSize(indexValuesBlockBits13, BIGINT, zerosValuesBlockBits13, BIGINT, 8, 8192, refSketch);

        Block indexValuesBlockBits11 = createLongSequenceIndexBlock(1, 100_000, 11);
        Block zerosValuesBlockBits11 = createLongSequenceZerosBlock(1, 100_000, 11, 32);

        refSketch = toSqlVarbinary(createLongSketch(2048, 32, 1, 100_000));
        assertCardinality(indexValuesBlockBits11, BIGINT, zerosValuesBlockBits11, BIGINT, 8, 2048, 32, refSketch, 50_000);
        assertSketchSize(indexValuesBlockBits11, BIGINT, zerosValuesBlockBits11, BIGINT, 8, 2048, 32, refSketch);
    }
}
