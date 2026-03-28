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
package com.facebook.presto.tests;

import org.testng.annotations.Test;

public abstract class AbstractTestKllSketchFunctions
        extends AbstractTestQueryFramework
{
    @Test
    public void testKllSketchRankDouble()
    {
        // Test basic rank functionality with double values
        // Using approximate comparisons due to sketch approximation
        assertQuery("SELECT sketch_kll_rank(sketch_kll(CAST(x AS DOUBLE)), CAST(50.0 AS DOUBLE)) BETWEEN 0.45 AND 0.55 " +
                        "FROM (SELECT x FROM UNNEST(sequence(1, 100)) AS t(x))",
                "SELECT true");

        assertQuery("SELECT sketch_kll_rank(sketch_kll(CAST(x AS DOUBLE)), CAST(0.0 AS DOUBLE)) < 0.05 " +
                        "FROM (SELECT x FROM UNNEST(sequence(1, 100)) AS t(x))",
                "SELECT true");

        assertQuery("SELECT sketch_kll_rank(sketch_kll(CAST(x AS DOUBLE)), CAST(100.0 AS DOUBLE)) > 0.95 " +
                        "FROM (SELECT x FROM UNNEST(sequence(1, 100)) AS t(x))",
                "SELECT true");
    }

    @Test
    public void testKllSketchRankBigint()
    {
        // Test rank with bigint values
        assertQuery("SELECT sketch_kll_rank(sketch_kll(CAST(x AS BIGINT)), CAST(50 AS BIGINT)) BETWEEN 0.45 AND 0.55 " +
                        "FROM (SELECT x FROM UNNEST(sequence(1, 100)) AS t(x))",
                "SELECT true");

        assertQuery("SELECT sketch_kll_rank(sketch_kll(CAST(x AS BIGINT)), CAST(1 AS BIGINT)) < 0.05 " +
                        "FROM (SELECT x FROM UNNEST(sequence(1, 100)) AS t(x))",
                "SELECT true");
    }

    @Test
    public void testKllSketchRankVarchar()
    {
        // Test rank with varchar values
        assertQuery("SELECT sketch_kll_rank(sketch_kll(CAST(x AS VARCHAR)), 'm') > 0.4 AND " +
                        "sketch_kll_rank(sketch_kll(CAST(x AS VARCHAR)), 'm') < 0.6 " +
                        "FROM (VALUES 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', " +
                        "'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z') AS t(x)",
                "SELECT true");
    }

    @Test
    public void testKllSketchRankBoolean()
    {
        // Test rank with boolean values
        assertQuery("SELECT sketch_kll_rank(sketch_kll(x), true) " +
                        "FROM (VALUES true, true, true, false, false, false, false, false, false, false) AS t(x)",
                "SELECT 1.0");

        assertQuery("SELECT sketch_kll_rank(sketch_kll(x), false) " +
                        "FROM (VALUES true, true, true, false, false, false, false, false, false, false) AS t(x)",
                "SELECT 0.7");
    }

    @Test
    public void testKllSketchRankWithInclusive()
    {
        // Test rank with explicit inclusive parameter
        assertQuery("SELECT sketch_kll_rank(sketch_kll(CAST(x AS DOUBLE)), CAST(50.0 AS DOUBLE), true) BETWEEN 0.45 AND 0.55 " +
                        "FROM (SELECT x FROM UNNEST(sequence(1, 100)) AS t(x))",
                "SELECT true");

        assertQuery("SELECT sketch_kll_rank(sketch_kll(CAST(x AS DOUBLE)), CAST(50.0 AS DOUBLE), false) BETWEEN 0.44 AND 0.54 " +
                        "FROM (SELECT x FROM UNNEST(sequence(1, 100)) AS t(x))",
                "SELECT true");
    }

    @Test
    public void testKllSketchQuantileDouble()
    {
        // Test basic quantile functionality with double values
        assertQuery("SELECT sketch_kll_quantile(sketch_kll(CAST(x AS DOUBLE)), 0.5) BETWEEN 45.0 AND 55.0 " +
                        "FROM (SELECT x FROM UNNEST(sequence(1, 100)) AS t(x))",
                "SELECT true");

        assertQuery("SELECT sketch_kll_quantile(sketch_kll(CAST(x AS DOUBLE)), 0.0) BETWEEN 0.0 AND 5.0 " +
                        "FROM (SELECT x FROM UNNEST(sequence(1, 100)) AS t(x))",
                "SELECT true");

        assertQuery("SELECT sketch_kll_quantile(sketch_kll(CAST(x AS DOUBLE)), 1.0) BETWEEN 95.0 AND 100.0 " +
                        "FROM (SELECT x FROM UNNEST(sequence(1, 100)) AS t(x))",
                "SELECT true");
    }

    @Test
    public void testKllSketchQuantileBigint()
    {
        // Test quantile with bigint values
        assertQuery("SELECT sketch_kll_quantile(sketch_kll(CAST(x AS BIGINT)), 0.5) BETWEEN CAST(45 AS BIGINT) AND CAST(55 AS BIGINT) " +
                        "FROM (SELECT x FROM UNNEST(sequence(1, 100)) AS t(x))",
                "SELECT true");

        assertQuery("SELECT sketch_kll_quantile(sketch_kll(CAST(x AS BIGINT)), 0.25) BETWEEN CAST(20 AS BIGINT) AND CAST(30 AS BIGINT) " +
                        "FROM (SELECT x FROM UNNEST(sequence(1, 100)) AS t(x))",
                "SELECT true");
    }

    @Test
    public void testKllSketchQuantileVarchar()
    {
        // Test quantile with varchar values
        assertQuery("SELECT sketch_kll_quantile(sketch_kll(CAST(x AS VARCHAR)), 0.0) " +
                        "FROM (VALUES 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', " +
                        "'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z') AS t(x)",
                "SELECT 'a'");

        assertQuery("SELECT sketch_kll_quantile(sketch_kll(CAST(x AS VARCHAR)), 1.0) " +
                        "FROM (VALUES 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', " +
                        "'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z') AS t(x)",
                "SELECT 'z'");
    }

    @Test
    public void testKllSketchQuantileBoolean()
    {
        // Test quantile with boolean values
        assertQuery("SELECT sketch_kll_quantile(sketch_kll(x), 0.0) " +
                        "FROM (VALUES true, true, true, false, false, false, false, false, false, false) AS t(x)",
                "SELECT false");

        assertQuery("SELECT sketch_kll_quantile(sketch_kll(x), 1.0) " +
                        "FROM (VALUES true, true, true, false, false, false, false, false, false, false) AS t(x)",
                "SELECT true");
    }

    @Test
    public void testKllSketchQuantileWithInclusive()
    {
        // Test quantile with explicit inclusive parameter
        assertQuery("SELECT sketch_kll_quantile(sketch_kll(CAST(x AS DOUBLE)), 0.5, true) BETWEEN 45.0 AND 55.0 " +
                        "FROM (SELECT x FROM UNNEST(sequence(1, 100)) AS t(x))",
                "SELECT true");

        assertQuery("SELECT sketch_kll_quantile(sketch_kll(CAST(x AS DOUBLE)), 0.5, false) BETWEEN 45.0 AND 55.0 " +
                        "FROM (SELECT x FROM UNNEST(sequence(1, 100)) AS t(x))",
                "SELECT true");
    }

    @Test
    public void testKllSketchWithCustomK()
    {
        // Test sketch_kll_with_k function with custom k value
        assertQuery("SELECT sketch_kll_rank(sketch_kll_with_k(CAST(x AS DOUBLE), 200), CAST(50.0 AS DOUBLE)) BETWEEN 0.45 AND 0.55 " +
                        "FROM (SELECT x FROM UNNEST(sequence(1, 100)) AS t(x))",
                "SELECT true");

        assertQuery("SELECT sketch_kll_quantile(sketch_kll_with_k(CAST(x AS BIGINT), 200), 0.5) BETWEEN CAST(45 AS BIGINT) AND CAST(55 AS BIGINT) " +
                        "FROM (SELECT x FROM UNNEST(sequence(1, 100)) AS t(x))",
                "SELECT true");
    }

    @Test
    public void testKllSketchWithNulls()
    {
        // Test that nulls are ignored
        assertQuery("SELECT sketch_kll_rank(sketch_kll(x), CAST(5 AS BIGINT)) BETWEEN 0.45 AND 0.55 " +
                        "FROM (VALUES CAST(1 AS BIGINT), null, CAST(2 AS BIGINT), CAST(3 AS BIGINT), " +
                        "CAST(4 AS BIGINT), CAST(5 AS BIGINT), null, CAST(6 AS BIGINT), CAST(7 AS BIGINT), " +
                        "CAST(8 AS BIGINT), CAST(9 AS BIGINT), CAST(10 AS BIGINT)) AS t(x)",
                "SELECT true");
    }

    @Test
    public void testKllSketchInvalidRank()
    {
        // Test invalid rank values
        assertQueryFails("SELECT sketch_kll_quantile(sketch_kll(CAST(x AS DOUBLE)), -0.1) " +
                        "FROM (SELECT x FROM UNNEST(sequence(1, 100)) AS t(x))",
                ".*normalized rank.*");

        assertQueryFails("SELECT sketch_kll_quantile(sketch_kll(CAST(x AS DOUBLE)), 1.5) " +
                        "FROM (SELECT x FROM UNNEST(sequence(1, 100)) AS t(x))",
                ".*normalized rank.*");
    }

    @Test
    public void testKllSketchInvalidK()
    {
        // Test invalid k values
        assertQueryFails("SELECT sketch_kll_with_k(CAST(x AS DOUBLE), 7) " +
                        "FROM (SELECT x FROM UNNEST(sequence(1, 100)) AS t(x))",
                ".*k value must satisfy 8 <= k <= 65535.*");

        // Test invalid k values - maximum k
        assertQueryFails("SELECT sketch_kll_with_k(CAST(x AS DOUBLE), 70000) " +
                        "FROM (SELECT x FROM UNNEST(sequence(1, 100)) AS t(x))",
                ".*k value must satisfy 8 <= k <= 65535.*");
    }

    @Test
    public void testKllSketchEmptyInput()
    {
        // Empty sketch operations return NULL (aggregate returns NULL for empty groups)
        assertQuery("SELECT sketch_kll_rank(sketch_kll(CAST(x AS BIGINT)), CAST(1 AS BIGINT)) " +
                        "FROM (SELECT CAST(x AS BIGINT) as x FROM UNNEST(CAST(ARRAY[] AS ARRAY(BIGINT))) AS t(x) WHERE x IS NOT NULL)",
                "SELECT CAST(NULL AS DOUBLE)");

        assertQuery("SELECT sketch_kll_quantile(sketch_kll(CAST(x AS DOUBLE)), 0.5) " +
                        "FROM (SELECT CAST(x AS DOUBLE) as x FROM UNNEST(CAST(ARRAY[] AS ARRAY(DOUBLE))) AS t(x) WHERE x IS NOT NULL)",
                "SELECT CAST(NULL AS DOUBLE)");
    }

    @Test
    public void testKllSketchSingleValue()
    {
        // Test with single value
        assertQuery("SELECT sketch_kll_rank(sketch_kll(CAST(42 AS BIGINT)), CAST(42 AS BIGINT)) > 0.95",
                "SELECT true");

        assertQuery("SELECT sketch_kll_quantile(sketch_kll(CAST(42 AS BIGINT)), 0.5)",
                "SELECT CAST(42 AS BIGINT)");
    }

    @Test
    public void testKllSketchGroupBy()
    {
        // Test sketch functions with GROUP BY
        assertQuery("SELECT g, sketch_kll_rank(sketch_kll(x), CAST(5 AS BIGINT)) BETWEEN 0.45 AND 0.55 " +
                        "FROM (VALUES (1, CAST(1 AS BIGINT)), (1, CAST(2 AS BIGINT)), (1, CAST(3 AS BIGINT)), " +
                        "(1, CAST(4 AS BIGINT)), (1, CAST(5 AS BIGINT)), (1, CAST(6 AS BIGINT)), " +
                        "(1, CAST(7 AS BIGINT)), (1, CAST(8 AS BIGINT)), (1, CAST(9 AS BIGINT)), " +
                        "(1, CAST(10 AS BIGINT)), " +
                        "(2, CAST(11 AS BIGINT)), (2, CAST(12 AS BIGINT)), (2, CAST(13 AS BIGINT)), " +
                        "(2, CAST(14 AS BIGINT)), (2, CAST(15 AS BIGINT))) AS t(g, x) " +
                        "GROUP BY g ORDER BY g",
                "VALUES (1, true), (2, false)");
    }

    @Test
    public void testKllSketchAccuracy()
    {
        // Test accuracy with larger dataset
        assertQuery("SELECT sketch_kll_quantile(sketch_kll(CAST(x AS DOUBLE)), 0.5) BETWEEN 480.0 AND 520.0 " +
                        "FROM (SELECT x FROM UNNEST(sequence(1, 1000)) AS t(x))",
                "SELECT true");

        assertQuery("SELECT sketch_kll_rank(sketch_kll(CAST(x AS DOUBLE)), CAST(250.0 AS DOUBLE)) BETWEEN 0.20 AND 0.30 " +
                        "FROM (SELECT x FROM UNNEST(sequence(1, 1000)) AS t(x))",
                "SELECT true");
    }
}
