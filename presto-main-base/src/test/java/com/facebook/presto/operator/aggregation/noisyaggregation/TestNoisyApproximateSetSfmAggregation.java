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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.SfmSketch;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createDoubleSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createStringSequenceBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;

/**
 * Tests for the noisy_approx_set_sfm function.
 * Overall, these are similar to the tests of noisy_approx_distinct_sfm, but with an extra check
 * to ensure that the size of the returned sketch matches the parameters specified (or defaulted).
 */
public class TestNoisyApproximateSetSfmAggregation
        extends AbstractTestNoisySfmAggregation
{
    protected String getFunctionName()
    {
        return "noisy_approx_set_sfm";
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

    private void assertSketchSize(Block valuesBlock, Type valueType, double epsilon, int numberOfBuckets, int precision, SqlVarbinary expected)
    {
        assertFunction(valuesBlock, valueType, epsilon, numberOfBuckets, precision, this::sketchSizesMatch, expected);
    }

    private void assertSketchSize(Block valuesBlock, Type valueType, double epsilon, int numberOfBuckets, SqlVarbinary expected)
    {
        assertFunction(valuesBlock, valueType, epsilon, numberOfBuckets, this::sketchSizesMatch, expected);
    }

    private void assertSketchSize(Block valuesBlock, Type valueType, double epsilon, SqlVarbinary expected)
    {
        assertFunction(valuesBlock, valueType, epsilon, this::sketchSizesMatch, expected);
    }

    @Test
    public void testNonPrivateInteger()
    {
        Block valuesBlock = createLongSequenceBlock(1, 100_000);

        SqlVarbinary refSketch = toSqlVarbinary(createLongSketch(4096, 24, 1, 100_000));
        assertCardinality(valuesBlock, BIGINT, SfmSketch.NON_PRIVATE_EPSILON, refSketch, 0);
        assertSketchSize(valuesBlock, BIGINT, SfmSketch.NON_PRIVATE_EPSILON, refSketch);

        refSketch = toSqlVarbinary(createLongSketch(8192, 24, 1, 100_000));
        assertCardinality(valuesBlock, BIGINT, SfmSketch.NON_PRIVATE_EPSILON, 8192, refSketch, 0);
        assertSketchSize(valuesBlock, BIGINT, SfmSketch.NON_PRIVATE_EPSILON, 8192, refSketch);

        refSketch = toSqlVarbinary(createLongSketch(2048, 32, 1, 100_000));
        assertCardinality(valuesBlock, BIGINT, SfmSketch.NON_PRIVATE_EPSILON, 2048, 32, refSketch, 0);
        assertSketchSize(valuesBlock, BIGINT, SfmSketch.NON_PRIVATE_EPSILON, 2048, 32, refSketch);
    }

    @Test
    public void testPrivateInteger()
    {
        Block valuesBlock = createLongSequenceBlock(1, 100_000);

        SqlVarbinary refSketch = toSqlVarbinary(createLongSketch(4096, 24, 1, 100_000));
        assertCardinality(valuesBlock, BIGINT, 8, refSketch, 50_000);
        assertSketchSize(valuesBlock, BIGINT, 8, refSketch);

        refSketch = toSqlVarbinary(createLongSketch(8192, 24, 1, 100_000));
        assertCardinality(valuesBlock, BIGINT, 8, 8192, refSketch, 50_000);
        assertSketchSize(valuesBlock, BIGINT, 8, 8192, refSketch);

        refSketch = toSqlVarbinary(createLongSketch(2048, 32, 1, 100_000));
        assertCardinality(valuesBlock, BIGINT, 8, 2048, 32, refSketch, 50_000);
        assertSketchSize(valuesBlock, BIGINT, 8, 2048, 32, refSketch);
    }

    @Test
    public void testNonPrivateDouble()
    {
        Block valuesBlock = createDoubleSequenceBlock(1, 100_000);

        SqlVarbinary refSketch = toSqlVarbinary(createDoubleSketch(4096, 24, 1, 100_000));
        assertCardinality(valuesBlock, DOUBLE, SfmSketch.NON_PRIVATE_EPSILON, refSketch, 0);
        assertSketchSize(valuesBlock, DOUBLE, SfmSketch.NON_PRIVATE_EPSILON, refSketch);

        refSketch = toSqlVarbinary(createDoubleSketch(8192, 24, 1, 100_000));
        assertCardinality(valuesBlock, DOUBLE, SfmSketch.NON_PRIVATE_EPSILON, 8192, refSketch, 0);
        assertSketchSize(valuesBlock, DOUBLE, SfmSketch.NON_PRIVATE_EPSILON, 8192, refSketch);

        refSketch = toSqlVarbinary(createDoubleSketch(2048, 32, 1, 100_000));
        assertCardinality(valuesBlock, DOUBLE, SfmSketch.NON_PRIVATE_EPSILON, 2048, 32, refSketch, 0);
        assertSketchSize(valuesBlock, DOUBLE, SfmSketch.NON_PRIVATE_EPSILON, 2048, 32, refSketch);
    }

    @Test
    public void testPrivateDouble()
    {
        Block valuesBlock = createDoubleSequenceBlock(1, 100_000);

        SqlVarbinary refSketch = toSqlVarbinary(createDoubleSketch(4096, 24, 1, 100_000));
        assertCardinality(valuesBlock, DOUBLE, 8, refSketch, 50_000);
        assertSketchSize(valuesBlock, DOUBLE, 8, refSketch);

        refSketch = toSqlVarbinary(createDoubleSketch(8192, 24, 1, 100_000));
        assertCardinality(valuesBlock, DOUBLE, 8, 8192, refSketch, 50_000);
        assertSketchSize(valuesBlock, DOUBLE, 8, 8192, refSketch);

        refSketch = toSqlVarbinary(createDoubleSketch(2048, 32, 1, 100_000));
        assertCardinality(valuesBlock, DOUBLE, 8, 2048, 32, refSketch, 50_000);
        assertSketchSize(valuesBlock, DOUBLE, 8, 2048, 32, refSketch);
    }

    @Test
    public void testNonPrivateString()
    {
        Block valuesBlock = createStringSequenceBlock(1, 100_000);

        SqlVarbinary refSketch = toSqlVarbinary(createStringSketch(4096, 24, 1, 100_000));
        assertCardinality(valuesBlock, VARCHAR, SfmSketch.NON_PRIVATE_EPSILON, refSketch, 0);
        assertSketchSize(valuesBlock, VARCHAR, SfmSketch.NON_PRIVATE_EPSILON, refSketch);

        refSketch = toSqlVarbinary(createStringSketch(8192, 24, 1, 100_000));
        assertCardinality(valuesBlock, VARCHAR, SfmSketch.NON_PRIVATE_EPSILON, 8192, refSketch, 0);
        assertSketchSize(valuesBlock, VARCHAR, SfmSketch.NON_PRIVATE_EPSILON, 8192, refSketch);

        refSketch = toSqlVarbinary(createStringSketch(2048, 32, 1, 100_000));
        assertCardinality(valuesBlock, VARCHAR, SfmSketch.NON_PRIVATE_EPSILON, 2048, 32, refSketch, 0);
        assertSketchSize(valuesBlock, VARCHAR, SfmSketch.NON_PRIVATE_EPSILON, 2048, 32, refSketch);
    }

    @Test
    public void testPrivateString()
    {
        Block valuesBlock = createStringSequenceBlock(1, 100_000);

        SqlVarbinary refSketch = toSqlVarbinary(createStringSketch(4096, 24, 1, 100_000));
        assertCardinality(valuesBlock, VARCHAR, 8, refSketch, 50_000);
        assertSketchSize(valuesBlock, VARCHAR, 8, refSketch);

        refSketch = toSqlVarbinary(createStringSketch(8192, 24, 1, 100_000));
        assertCardinality(valuesBlock, VARCHAR, 8, 8192, refSketch, 50_000);
        assertSketchSize(valuesBlock, VARCHAR, 8, 8192, refSketch);

        refSketch = toSqlVarbinary(createStringSketch(2048, 32, 1, 100_000));
        assertCardinality(valuesBlock, VARCHAR, 8, 2048, 32, refSketch, 50_000);
        assertSketchSize(valuesBlock, VARCHAR, 8, 2048, 32, refSketch);
    }
}
