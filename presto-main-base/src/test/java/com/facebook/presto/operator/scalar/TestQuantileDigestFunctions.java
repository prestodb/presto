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
package com.facebook.presto.operator.scalar;

import com.facebook.airlift.stats.QuantileDigest;
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeParameter;
import com.facebook.presto.operator.aggregation.FloatingPointBitsConverterUtil;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.QuantileDigestParametricType.QDIGEST;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestQuantileDigestFunctions
        extends AbstractTestFunctions
{
    private static final Type QDIGEST_BIGINT = QDIGEST.createType(ImmutableList.of(TypeParameter.of(BIGINT)));

    @Test
    public void testNullQuantileDigestGetValueAtQuantile()
    {
        functionAssertions.assertFunction("value_at_quantile(CAST(NULL AS qdigest(bigint)), 0.3)", BIGINT, null);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Quantile should be within bounds \\[0, 1\\], was: \\d+\\.\\d+")
    public void testGetValueAtQuantileOverOne()
    {
        QuantileDigest qdigest = new QuantileDigest(1);
        functionAssertions.assertFunction(format("value_at_quantile(CAST(X'%s' AS qdigest(bigint)), 1.5)", toHexString(qdigest)),
                BIGINT,
                null);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Quantile should be within bounds \\[0, 1\\], was: -\\d+\\.\\d+")
    public void testGetValueAtQuantileBelowZero()
    {
        QuantileDigest qdigest = new QuantileDigest(1);
        functionAssertions.assertFunction(format("value_at_quantile(CAST(X'%s' AS qdigest(bigint)), -0.2)", toHexString(qdigest)),
                BIGINT,
                null);
    }

    @Test
    public void testValueAtQuantileBigint()
    {
        QuantileDigest qdigest = new QuantileDigest(1);
        addAll(qdigest, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        functionAssertions.assertFunction(format("value_at_quantile(CAST(X'%s' AS qdigest(bigint)), 0.5)", toHexString(qdigest)),
                BIGINT,
                5L);
    }

    @Test
    public void testQuantileAtValueBigint()
    {
        QuantileDigest qdigest = new QuantileDigest(1);
        addAll(qdigest, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        functionAssertions.assertFunction(format("quantile_at_value(CAST(X'%s' AS qdigest(bigint)), 20)", toHexString(qdigest)),
                DOUBLE,
                null);
        functionAssertions.assertFunction(format("quantile_at_value(CAST(X'%s' AS qdigest(bigint)), 6)", toHexString(qdigest)),
                DOUBLE,
                0.6);
        functionAssertions.assertFunction(format("quantile_at_value(CAST(X'%s' AS qdigest(bigint)), -1)", toHexString(qdigest)),
                DOUBLE,
                null);
    }

    @Test
    public void testQuantileAtValueDouble()
    {
        QuantileDigest qdigest = new QuantileDigest(1);
        ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9).stream()
                .mapToLong(FloatingPointBitsConverterUtil::doubleToSortableLong)
                .forEach(qdigest::add);
        functionAssertions.assertFunction(format("quantile_at_value(CAST(X'%s' AS qdigest(double)), 5.6)", toHexString(qdigest)),
                DOUBLE,
                0.6);
        functionAssertions.assertFunction(format("quantile_at_value(CAST(X'%s' AS qdigest(double)), -1.23)", toHexString(qdigest)),
                DOUBLE,
                null);
        functionAssertions.assertFunction(format("quantile_at_value(CAST(X'%s' AS qdigest(double)), 12.3)", toHexString(qdigest)),
                DOUBLE,
                null);
        functionAssertions.assertFunction(format("quantile_at_value(CAST(X'%s' AS qdigest(double)), nan())", toHexString(qdigest)),
                DOUBLE,
                null);
    }

    @Test
    public void testQuantileAtValueBigintWithEmptyDigest()
    {
        QuantileDigest qdigest = new QuantileDigest(1);
        functionAssertions.assertFunction(format("quantile_at_value(CAST(X'%s' AS qdigest(bigint)), 5)", toHexString(qdigest)),
                DOUBLE,
                null);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Scale factor should be positive\\.")
    public void testScaleNegative()
    {
        QuantileDigest qdigest = new QuantileDigest(1);
        addAll(qdigest, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        functionAssertions.selectSingleValue(format("scale_qdigest(CAST(X'%s' AS qdigest(bigint)), -1)", toHexString(qdigest)), QDIGEST_BIGINT, SqlVarbinary.class);
    }

    @Test
    public void testScale()
    {
        QuantileDigest qdigest = new QuantileDigest(1);
        addAll(qdigest, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        // Before scaling.
        assertEquals(qdigest.getHistogram(asList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L)),
                asList(new QuantileDigest.Bucket(0, Double.NaN),
                        new QuantileDigest.Bucket(1, 0),
                        new QuantileDigest.Bucket(1, 1),
                        new QuantileDigest.Bucket(1, 2),
                        new QuantileDigest.Bucket(1, 3),
                        new QuantileDigest.Bucket(1, 4),
                        new QuantileDigest.Bucket(1, 5),
                        new QuantileDigest.Bucket(1, 6),
                        new QuantileDigest.Bucket(1, 7),
                        new QuantileDigest.Bucket(1, 8),
                        new QuantileDigest.Bucket(1, 9)));

        // Scale up.
        SqlVarbinary sqlVarbinary = functionAssertions.selectSingleValue(format("scale_qdigest(CAST(X'%s' AS qdigest(bigint)), 2)", toHexString(qdigest)),
                QDIGEST_BIGINT, SqlVarbinary.class);

        QuantileDigest scaledQdigest = new QuantileDigest(wrappedBuffer(sqlVarbinary.getBytes()));

        assertEquals(scaledQdigest.getHistogram(asList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L)),
                asList(new QuantileDigest.Bucket(0, Double.NaN),
                        new QuantileDigest.Bucket(2, 0),
                        new QuantileDigest.Bucket(2, 1),
                        new QuantileDigest.Bucket(2, 2),
                        new QuantileDigest.Bucket(2, 3),
                        new QuantileDigest.Bucket(2, 4),
                        new QuantileDigest.Bucket(2, 5),
                        new QuantileDigest.Bucket(2, 6),
                        new QuantileDigest.Bucket(2, 7),
                        new QuantileDigest.Bucket(2, 8),
                        new QuantileDigest.Bucket(2, 9)));

        // Scale down.
        sqlVarbinary = functionAssertions.selectSingleValue(format("scale_qdigest(CAST(X'%s' AS qdigest(bigint)), 0.5)", toHexString(qdigest)), QDIGEST_BIGINT, SqlVarbinary.class);

        scaledQdigest = new QuantileDigest(wrappedBuffer(sqlVarbinary.getBytes()));

        assertEquals(scaledQdigest.getHistogram(asList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L)),
                asList(new QuantileDigest.Bucket(0, Double.NaN),
                        new QuantileDigest.Bucket(0.5, 0),
                        new QuantileDigest.Bucket(0.5, 1),
                        new QuantileDigest.Bucket(0.5, 2),
                        new QuantileDigest.Bucket(0.5, 3),
                        new QuantileDigest.Bucket(0.5, 4),
                        new QuantileDigest.Bucket(0.5, 5),
                        new QuantileDigest.Bucket(0.5, 6),
                        new QuantileDigest.Bucket(0.5, 7),
                        new QuantileDigest.Bucket(0.5, 8),
                        new QuantileDigest.Bucket(0.5, 9)));
    }

    private static void addAll(QuantileDigest digest, long... values)
    {
        requireNonNull(values, "values is null");
        for (long value : values) {
            digest.add(value);
        }
    }

    private static String toHexString(QuantileDigest qdigest)
    {
        return new SqlVarbinary(qdigest.serialize().getBytes()).toString().replaceAll("\\s+", " ");
    }
}
