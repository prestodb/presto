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
        // Using BETWEEN for approximate results since KLL sketch is probabilistic
        assertQuery("SELECT sketch_kll_rank(sketch_kll(x), true) BETWEEN 0.95 AND 1.0 " +
                       "FROM (VALUES true, true, true, false, false, false, false, false, false, false) AS t(x)",
                "SELECT true");

        assertQuery("SELECT sketch_kll_rank(sketch_kll(x), false) BETWEEN 0.65 AND 0.75 " +
                        "FROM (VALUES true, true, true, false, false, false, false, false, false, false) AS t(x)",
                "SELECT true");
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

    @Test
    public void testKllSketchWithDuplicates()
    {
        // Test that duplicate values are handled correctly
        assertQuery("SELECT sketch_kll_rank(sketch_kll(x), CAST(5 AS BIGINT)) BETWEEN 0.95 AND 1.0 " +
                        "FROM (VALUES CAST(1 AS BIGINT), CAST(2 AS BIGINT), CAST(3 AS BIGINT), " +
                        "CAST(5 AS BIGINT), CAST(5 AS BIGINT), CAST(5 AS BIGINT), CAST(5 AS BIGINT), " +
                        "CAST(5 AS BIGINT), CAST(5 AS BIGINT), CAST(5 AS BIGINT)) AS t(x)",
                "SELECT true");

        assertQuery("SELECT sketch_kll_quantile(sketch_kll(x), 0.5) " +
                        "FROM (VALUES CAST(1 AS BIGINT), CAST(1 AS BIGINT), CAST(1 AS BIGINT), " +
                        "CAST(1 AS BIGINT), CAST(1 AS BIGINT), CAST(2 AS BIGINT)) AS t(x)",
                "SELECT CAST(1 AS BIGINT)");

        // Test rank with inclusive vs exclusive
        assertQuery("SELECT sketch_kll_rank(sketch_kll(x), CAST(2 AS BIGINT), true) BETWEEN 0.55 AND 0.65 " +
                        "FROM (VALUES CAST(1 AS BIGINT), CAST(2 AS BIGINT), CAST(2 AS BIGINT), " +
                        "CAST(2 AS BIGINT), CAST(3 AS BIGINT), CAST(4 AS BIGINT), CAST(5 AS BIGINT)) AS t(x)",
                "SELECT true");

        assertQuery("SELECT sketch_kll_rank(sketch_kll(x), CAST(2 AS BIGINT), false) BETWEEN 0.10 AND 0.20 " +
                        "FROM (VALUES CAST(1 AS BIGINT), CAST(2 AS BIGINT), CAST(2 AS BIGINT), " +
                        "CAST(2 AS BIGINT), CAST(3 AS BIGINT), CAST(4 AS BIGINT), CAST(5 AS BIGINT)) AS t(x)",
                "SELECT true");

        // Verify that inclusive and exclusive can produce different results
        assertQuery("SELECT " +
                        "sketch_kll_rank(sketch_kll(x), CAST(2 AS BIGINT), true) > " +
                        "sketch_kll_rank(sketch_kll(x), CAST(2 AS BIGINT), false) " +
                        "FROM (VALUES CAST(1 AS BIGINT), CAST(2 AS BIGINT), CAST(2 AS BIGINT), " +
                        "CAST(2 AS BIGINT), CAST(3 AS BIGINT), CAST(4 AS BIGINT), CAST(5 AS BIGINT)) AS t(x)",
                "SELECT true");

        // Test quantile with inclusive vs exclusive
        assertQuery("SELECT sketch_kll_quantile(sketch_kll(x), 0.5, true) " +
                        "FROM (VALUES CAST(1 AS BIGINT), CAST(1 AS BIGINT), CAST(2 AS BIGINT), " +
                        "CAST(2 AS BIGINT), CAST(3 AS BIGINT), CAST(4 AS BIGINT), " +
                        "CAST(5 AS BIGINT), CAST(5 AS BIGINT), CAST(6 AS BIGINT), CAST(7 AS BIGINT)) AS t(x)",
                "SELECT CAST(3 AS BIGINT)");

        assertQuery("SELECT sketch_kll_quantile(sketch_kll(x), 0.5, false) " +
                        "FROM (VALUES CAST(1 AS BIGINT), CAST(1 AS BIGINT), CAST(2 AS BIGINT), " +
                        "CAST(2 AS BIGINT), CAST(3 AS BIGINT), CAST(4 AS BIGINT), " +
                        "CAST(5 AS BIGINT), CAST(5 AS BIGINT), CAST(6 AS BIGINT), CAST(7 AS BIGINT)) AS t(x)",
                "SELECT CAST(4 AS BIGINT)");

        // Verify that inclusive and exclusive can produce different results
        assertQuery("SELECT " +
                        "sketch_kll_quantile(sketch_kll(x), 0.5, true) != " +
                        "sketch_kll_quantile(sketch_kll(x), 0.5, false) " +
                        "FROM (VALUES CAST(1 AS BIGINT), CAST(1 AS BIGINT), CAST(2 AS BIGINT), " +
                        "CAST(2 AS BIGINT), CAST(3 AS BIGINT), CAST(4 AS BIGINT), " +
                        "CAST(5 AS BIGINT), CAST(5 AS BIGINT), CAST(6 AS BIGINT), CAST(7 AS BIGINT)) AS t(x)",
                "SELECT true");
    }

    @Test
    public void testKllSketchNegativeNumbers()
    {
        // Test with negative numbers
        assertQuery("SELECT sketch_kll_quantile(sketch_kll(CAST(x AS DOUBLE)), 0.5) BETWEEN -5.0 AND 5.0 " +
                        "FROM (SELECT x FROM UNNEST(sequence(-100, 100)) AS t(x))",
                "SELECT true");

        assertQuery("SELECT sketch_kll_rank(sketch_kll(CAST(x AS DOUBLE)), CAST(0.0 AS DOUBLE)) BETWEEN 0.45 AND 0.55 " +
                        "FROM (SELECT x FROM UNNEST(sequence(-100, 100)) AS t(x))",
                "SELECT true");

        // Test with all negative numbers
        assertQuery("SELECT sketch_kll_quantile(sketch_kll(CAST(x AS DOUBLE)), 0.5) BETWEEN -55.0 AND -45.0 " +
                        "FROM (SELECT x FROM UNNEST(sequence(-100, -1)) AS t(x))",
                "SELECT true");
    }

    @Test
    public void testKllSketchAllSameValues()
    {
        // Test when all values are identical
        assertQuery("SELECT sketch_kll_rank(sketch_kll(CAST(x AS BIGINT)), CAST(42 AS BIGINT)) " +
                        "FROM (VALUES CAST(42 AS BIGINT), CAST(42 AS BIGINT), CAST(42 AS BIGINT), " +
                        "CAST(42 AS BIGINT), CAST(42 AS BIGINT)) AS t(x)",
                "SELECT 1.0");

        assertQuery("SELECT sketch_kll_quantile(sketch_kll(CAST(x AS BIGINT)), 0.5) " +
                        "FROM (VALUES CAST(42 AS BIGINT), CAST(42 AS BIGINT), CAST(42 AS BIGINT)) AS t(x)",
                "SELECT CAST(42 AS BIGINT)");

        // Any quantile should return the same value
        assertQuery("SELECT sketch_kll_quantile(sketch_kll(CAST(x AS BIGINT)), 0.0) " +
                        "FROM (VALUES CAST(42 AS BIGINT), CAST(42 AS BIGINT)) AS t(x)",
                "SELECT CAST(42 AS BIGINT)");

        assertQuery("SELECT sketch_kll_quantile(sketch_kll(CAST(x AS BIGINT)), 1.0) " +
                        "FROM (VALUES CAST(42 AS BIGINT), CAST(42 AS BIGINT)) AS t(x)",
                "SELECT CAST(42 AS BIGINT)");
    }

    @Test
    public void testKllSketchEmptyStrings()
    {
        // Test with empty strings
        assertQuery("SELECT sketch_kll_quantile(sketch_kll(CAST(x AS VARCHAR)), CAST(0.0 AS DOUBLE)) " +
                        "FROM (VALUES '', 'a', 'b', 'c') AS t(x)",
                "SELECT ''");

        assertQuery("SELECT sketch_kll_rank(sketch_kll(CAST(x AS VARCHAR)), '') < 0.3 " +
                        "FROM (VALUES '', 'a', 'b', 'c') AS t(x)",
                "SELECT true");

        // Test with multiple empty strings
        assertQuery("SELECT sketch_kll_quantile(sketch_kll(CAST(x AS VARCHAR)), 0.5) " +
                        "FROM (VALUES '', '', '', 'a', 'b') AS t(x)",
                "SELECT ''");
    }

    @Test
    public void testKllSketchMultiColumnGroupBy()
    {
        // Test quantile with multi-column GROUP BY
        assertQuery("SELECT region, category, " +
                        "sketch_kll_quantile(sketch_kll(CAST(value AS DOUBLE)), 0.5) BETWEEN 49.0 AND 51.0 " +
                        "FROM (VALUES ('North', 'A', CAST(50.0 AS DOUBLE)), " +
                        "('North', 'A', CAST(51.0 AS DOUBLE)), " +
                        "('North', 'A', CAST(49.0 AS DOUBLE)), " +
                        "('South', 'B', CAST(25.0 AS DOUBLE)), " +
                        "('South', 'B', CAST(26.0 AS DOUBLE)), " +
                        "('North', 'B', CAST(75.0 AS DOUBLE))) AS t(region, category, value) " +
                        "GROUP BY region, category " +
                        "ORDER BY region, category",
                "VALUES ('North', 'A', true), ('North', 'B', false), ('South', 'B', false)");

        // Test rank with multi-column GROUP BY
        assertQuery("SELECT region, category, " +
                        "sketch_kll_rank(sketch_kll(CAST(value AS BIGINT)), CAST(50 AS BIGINT)) BETWEEN 0.55 AND 0.65 " +
                        "FROM (VALUES ('East', 'X', CAST(40 AS BIGINT)), " +
                        "('East', 'X', CAST(45 AS BIGINT)), " +
                        "('East', 'X', CAST(50 AS BIGINT)), " +
                        "('East', 'X', CAST(55 AS BIGINT)), " +
                        "('East', 'X', CAST(60 AS BIGINT)), " +
                        "('West', 'Y', CAST(100 AS BIGINT))) AS t(region, category, value) " +
                        "GROUP BY region, category " +
                        "ORDER BY region, category",
                "VALUES ('East', 'X', true), ('West', 'Y', false)");
    }

    /**
     * Verifies Java-serialized KLL sketch bytes are correctly consumed by the worker (native or Java).
     * When run via presto-native-tests, the CAST is constant-folded by the Java coordinator
     * and the scalar functions execute natively, exercising the Java→native byte boundary.
     */
    @Test
    public void testKllSketchCrossEngineConstantFold()
    {
        // CHECKSTYLE:OFF: LineLength
        // BIGINT, values 0..99, k=200
        String bigintHex = "05010f00c80008006400000000000000c8000100640000000000000000000000630000000000000063000000000000006200000000000000610000000000000060000000000000005f000000000000005e000000000000005d000000000000005c000000000000005b000000000000005a0000000000000059000000000000005800000000000000570000000000000056000000000000005500000000000000540000000000000053000000000000005200000000000000510000000000000050000000000000004f000000000000004e000000000000004d000000000000004c000000000000004b000000000000004a0000000000000049000000000000004800000000000000470000000000000046000000000000004500000000000000440000000000000043000000000000004200000000000000410000000000000040000000000000003f000000000000003e000000000000003d000000000000003c000000000000003b000000000000003a0000000000000039000000000000003800000000000000370000000000000036000000000000003500000000000000340000000000000033000000000000003200000000000000310000000000000030000000000000002f000000000000002e000000000000002d000000000000002c000000000000002b000000000000002a0000000000000029000000000000002800000000000000270000000000000026000000000000002500000000000000240000000000000023000000000000002200000000000000210000000000000020000000000000001f000000000000001e000000000000001d000000000000001c000000000000001b000000000000001a0000000000000019000000000000001800000000000000170000000000000016000000000000001500000000000000140000000000000013000000000000001200000000000000110000000000000010000000000000000f000000000000000e000000000000000d000000000000000c000000000000000b000000000000000a000000000000000900000000000000080000000000000007000000000000000600000000000000050000000000000004000000000000000300000000000000020000000000000001000000000000000000000000000000";
        assertQuery(
                "SELECT sketch_kll_rank(CAST(X'" + bigintHex + "' AS kllsketch(bigint)), CAST(49 AS BIGINT)) BETWEEN 0.45 AND 0.55",
                "SELECT true");
        assertQuery(
                "SELECT sketch_kll_quantile(CAST(X'" + bigintHex + "' AS kllsketch(bigint)), 0.0)",
                "SELECT CAST(0 AS BIGINT)");
        assertQuery(
                "SELECT sketch_kll_quantile(CAST(X'" + bigintHex + "' AS kllsketch(bigint)), 1.0)",
                "SELECT CAST(99 AS BIGINT)");

        // DOUBLE, values 0.0..99.0, k=200
        String doubleHex = "05010f00c80008006400000000000000c80001006400000000000000000000000000000000c058400000000000c058400000000000805840000000000040584000000000000058400000000000c057400000000000805740000000000040574000000000000057400000000000c056400000000000805640000000000040564000000000000056400000000000c055400000000000805540000000000040554000000000000055400000000000c054400000000000805440000000000040544000000000000054400000000000c053400000000000805340000000000040534000000000000053400000000000c052400000000000805240000000000040524000000000000052400000000000c051400000000000805140000000000040514000000000000051400000000000c050400000000000805040000000000040504000000000000050400000000000804f400000000000004f400000000000804e400000000000004e400000000000804d400000000000004d400000000000804c400000000000004c400000000000804b400000000000004b400000000000804a400000000000004a40000000000080494000000000000049400000000000804840000000000000484000000000008047400000000000004740000000000080464000000000000046400000000000804540000000000000454000000000008044400000000000004440000000000080434000000000000043400000000000804240000000000000424000000000008041400000000000004140000000000080404000000000000040400000000000003f400000000000003e400000000000003d400000000000003c400000000000003b400000000000003a4000000000000039400000000000003840000000000000374000000000000036400000000000003540000000000000344000000000000033400000000000003240000000000000314000000000000030400000000000002e400000000000002c400000000000002a40000000000000284000000000000026400000000000002440000000000000224000000000000020400000000000001c4000000000000018400000000000001440000000000000104000000000000008400000000000000040000000000000f03f0000000000000000";
        assertQuery(
                "SELECT sketch_kll_rank(CAST(X'" + doubleHex + "' AS kllsketch(double)), CAST(49.0 AS DOUBLE)) BETWEEN 0.45 AND 0.55",
                "SELECT true");
        assertQuery(
                "SELECT sketch_kll_quantile(CAST(X'" + doubleHex + "' AS kllsketch(double)), 0.0)",
                "SELECT 0.0");
        assertQuery(
                "SELECT sketch_kll_quantile(CAST(X'" + doubleHex + "' AS kllsketch(double)), 1.0)",
                "SELECT 99.0");

        // VARCHAR, 'a'..'z', k=200
        String varcharHex = "05010f00c80008001a00000000000000c8000100ae0000000100000061010000007a010000007a0100000079010000007801000000770100000076010000007501000000740100000073010000007201000000710100000070010000006f010000006e010000006d010000006c010000006b010000006a010000006901000000680100000067010000006601000000650100000064010000006301000000620100000061";
        assertQuery(
                "SELECT sketch_kll_rank(CAST(X'" + varcharHex + "' AS kllsketch(varchar)), 'm') BETWEEN 0.45 AND 0.55",
                "SELECT true");
        assertQuery(
                "SELECT sketch_kll_quantile(CAST(X'" + varcharHex + "' AS kllsketch(varchar)), 0.0)",
                "SELECT 'a'");
        assertQuery(
                "SELECT sketch_kll_quantile(CAST(X'" + varcharHex + "' AS kllsketch(varchar)), 1.0)",
                "SELECT 'z'");

        // BOOLEAN, i%3==0, k=200, bit-packed (ArrayOfBooleansSerDe). ~34 true, ~66 false.
        String booleanHex = "05010f00c80008006400000000000000c800010064000000000149922449922449922449922409";
        assertQuery(
                "SELECT sketch_kll_rank(CAST(X'" + booleanHex + "' AS kllsketch(boolean)), false) BETWEEN 0.61 AND 0.71",
                "SELECT true");
        assertQuery(
                "SELECT sketch_kll_quantile(CAST(X'" + booleanHex + "' AS kllsketch(boolean)), 0.0)",
                "SELECT false");
        assertQuery(
                "SELECT sketch_kll_quantile(CAST(X'" + booleanHex + "' AS kllsketch(boolean)), 1.0)",
                "SELECT true");
        // CHECKSTYLE:ON: LineLength
    }
}
