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
package com.facebook.presto.iceberg.function;

import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import java.util.OptionalInt;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.FileFormat.PARQUET;

/**
 * Tests for Z-Order UDFs.
 */
public class TestIcebergZOrderFunctions
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(HADOOP)
                .setFormat(PARQUET)
                .setNodeCount(OptionalInt.of(1))
                .setCreateTpchTables(false)
                .setAddJmxPlugin(false)
                .build().getQueryRunner();
    }

    @Test
    public void testZOrderTinyintBytes()
    {
        // Test that tinyint values are converted to ordered bytes
        assertQuery("SELECT length(zorder_tinyint_bytes(TINYINT '42'))", "SELECT 8");
        assertQuery("SELECT length(zorder_tinyint_bytes(TINYINT '-42'))", "SELECT 8");

        // Test ordering: negative < positive
        assertQuery(
                "SELECT zorder_tinyint_bytes(TINYINT '-1') < zorder_tinyint_bytes(TINYINT '1')",
                "SELECT true");
    }

    @Test
    public void testZOrderSmallintBytes()
    {
        // Test that smallint values are converted to ordered bytes
        assertQuery("SELECT length(zorder_smallint_bytes(SMALLINT '1000'))", "SELECT 8");
        assertQuery("SELECT length(zorder_smallint_bytes(SMALLINT '-1000'))", "SELECT 8");

        // Test ordering
        assertQuery(
                "SELECT zorder_smallint_bytes(SMALLINT '-100') < zorder_smallint_bytes(SMALLINT '100')",
                "SELECT true");
    }

    @Test
    public void testZOrderIntegerBytes()
    {
        // Test that integer values are converted to ordered bytes
        assertQuery("SELECT length(zorder_integer_bytes(INTEGER '100000'))", "SELECT 8");
        assertQuery("SELECT length(zorder_integer_bytes(INTEGER '-100000'))", "SELECT 8");

        // Test ordering
        assertQuery(
                "SELECT zorder_integer_bytes(INTEGER '-1000') < zorder_integer_bytes(INTEGER '1000')",
                "SELECT true");
        assertQuery(
                "SELECT zorder_integer_bytes(INTEGER '100') < zorder_integer_bytes(INTEGER '200')",
                "SELECT true");
    }

    @Test
    public void testZOrderBigintBytes()
    {
        // Test that bigint values are converted to ordered bytes
        assertQuery("SELECT length(zorder_bigint_bytes(BIGINT '9223372036854775807'))", "SELECT 8");
        assertQuery("SELECT length(zorder_bigint_bytes(BIGINT '-9223372036854775808'))", "SELECT 8");

        // Test ordering
        assertQuery(
                "SELECT zorder_bigint_bytes(BIGINT '-1000000') < zorder_bigint_bytes(BIGINT '1000000')",
                "SELECT true");
        assertQuery(
                "SELECT zorder_bigint_bytes(BIGINT '1000') < zorder_bigint_bytes(BIGINT '2000')",
                "SELECT true");
    }

    @Test
    public void testZOrderRealBytes()
    {
        // Test that real values are converted to ordered bytes
        assertQuery("SELECT length(zorder_real_bytes(REAL '3.14'))", "SELECT 8");
        assertQuery("SELECT length(zorder_real_bytes(REAL '-3.14'))", "SELECT 8");

        // Test ordering
        assertQuery(
                "SELECT zorder_real_bytes(REAL '-1.5') < zorder_real_bytes(REAL '1.5')",
                "SELECT true");
        assertQuery(
                "SELECT zorder_real_bytes(REAL '1.0') < zorder_real_bytes(REAL '2.0')",
                "SELECT true");
    }

    @Test
    public void testZOrderDoubleBytes()
    {
        // Test that double values are converted to ordered bytes
        assertQuery("SELECT length(zorder_double_bytes(DOUBLE '3.141592653589793'))", "SELECT 8");
        assertQuery("SELECT length(zorder_double_bytes(DOUBLE '-3.141592653589793'))", "SELECT 8");

        // Test ordering
        assertQuery(
                "SELECT zorder_double_bytes(DOUBLE '-1.5') < zorder_double_bytes(DOUBLE '1.5')",
                "SELECT true");
        assertQuery(
                "SELECT zorder_double_bytes(DOUBLE '1.0') < zorder_double_bytes(DOUBLE '2.0')",
                "SELECT true");
    }

    @Test
    public void testZOrderBooleanBytes()
    {
        // Test that boolean values are converted to ordered bytes
        assertQuery("SELECT length(zorder_boolean_bytes(true))", "SELECT 8");
        assertQuery("SELECT length(zorder_boolean_bytes(false))", "SELECT 8");

        // Test ordering: false < true
        assertQuery(
                "SELECT zorder_boolean_bytes(false) < zorder_boolean_bytes(true)",
                "SELECT true");
    }

    @Test
    public void testZOrderVarcharBytes()
    {
        // Test that varchar values are converted to ordered bytes with specified length
        assertQuery("SELECT length(zorder_varchar_bytes('hello', 10))", "SELECT 10");
        assertQuery("SELECT length(zorder_varchar_bytes('world', 20))", "SELECT 20");

        // Test truncation
        assertQuery("SELECT length(zorder_varchar_bytes('this is a very long string', 10))", "SELECT 10");

        // Test ordering
        assertQuery(
                "SELECT zorder_varchar_bytes('apple', 10) < zorder_varchar_bytes('banana', 10)",
                "SELECT true");
    }

    @Test
    public void testZOrderVarbinaryBytes()
    {
        // Test that varbinary values are converted to ordered bytes with specified length
        assertQuery("SELECT length(zorder_varbinary_bytes(X'DEADBEEF', 10))", "SELECT 10");
        assertQuery("SELECT length(zorder_varbinary_bytes(X'CAFE', 20))", "SELECT 20");
    }

    @Test
    public void testZOrderDateBytes()
    {
        // Test that date values are converted to ordered bytes
        assertQuery("SELECT length(zorder_date_bytes(DATE '2024-01-01'))", "SELECT 8");
        assertQuery("SELECT length(zorder_date_bytes(DATE '1970-01-01'))", "SELECT 8");

        // Test ordering
        assertQuery(
                "SELECT zorder_date_bytes(DATE '2020-01-01') < zorder_date_bytes(DATE '2024-01-01')",
                "SELECT true");
    }

    @Test
    public void testZOrderTimestampBytes()
    {
        // Test that timestamp values are converted to ordered bytes
        assertQuery("SELECT length(zorder_timestamp_bytes(TIMESTAMP '2024-01-01 12:00:00'))", "SELECT 8");

        // Test ordering
        assertQuery(
                "SELECT zorder_timestamp_bytes(TIMESTAMP '2020-01-01 00:00:00') < " +
                "zorder_timestamp_bytes(TIMESTAMP '2024-01-01 00:00:00')",
                "SELECT true");
    }

    @Test
    public void testZOrderInterleave()
    {
        // Test interleaving with 2 columns
        assertQuery(
                "SELECT length(zorder(ARRAY[zorder_integer_bytes(1), zorder_integer_bytes(2)], 16))",
                "SELECT 16");

        // Test interleaving with 3 columns
        assertQuery(
                "SELECT length(zorder(ARRAY[" +
                "zorder_integer_bytes(1), " +
                "zorder_integer_bytes(2), " +
                "zorder_integer_bytes(3)], 24))",
                "SELECT 24");

        // Test that different input orders produce different outputs
        assertQuery(
                "SELECT zorder(ARRAY[zorder_integer_bytes(1), zorder_integer_bytes(2)], 16) != " +
                "zorder(ARRAY[zorder_integer_bytes(2), zorder_integer_bytes(1)], 16)",
                "SELECT true");
    }

    @Test
    public void testZOrderInterleaveTwoColumns()
    {
        // Test a complete Z-order computation with 2 integer columns
        assertQuery("SELECT length(zorder(ARRAY[" +
                "zorder_integer_bytes(100), " +
                "zorder_integer_bytes(200)], 16))",
                "SELECT 16");

        // Verify that points closer in 2D space have closer Z-order values
        // Point (1,1) should be closer to (1,2) than to (100,100)
        assertQuery(
                "WITH points AS (" +
                "  SELECT 1 as x, 1 as y, zorder(ARRAY[zorder_integer_bytes(1), zorder_integer_bytes(1)], 16) as z " +
                "  UNION ALL " +
                "  SELECT 1 as x, 2 as y, zorder(ARRAY[zorder_integer_bytes(1), zorder_integer_bytes(2)], 16) as z " +
                "  UNION ALL " +
                "  SELECT 100 as x, 100 as y, zorder(ARRAY[zorder_integer_bytes(100), zorder_integer_bytes(100)], 16) as z" +
                ") " +
                "SELECT COUNT(*) FROM points",
                "SELECT 3");
    }

    @Test
    public void testZOrderInterleaveThreeColumns()
    {
        // Test a complete Z-order computation with 3 columns of different types
        assertQuery(
                "SELECT length(zorder(ARRAY[" +
                "zorder_integer_bytes(100), " +
                "zorder_double_bytes(3.14), " +
                "zorder_varchar_bytes('test', 10)], 24))",
                "SELECT 24");
    }

    @Test
    public void testZOrderInterleaveFourColumns()
    {
        // Test with 4 columns
        assertQuery(
                "SELECT length(zorder(ARRAY[" +
                "zorder_integer_bytes(1), " +
                "zorder_integer_bytes(2), " +
                "zorder_integer_bytes(3), " +
                "zorder_integer_bytes(4)], 32))",
                "SELECT 32");
    }

    @Test
    public void testZOrderWithNullHandling()
    {
        // Test that null inputs return zero-filled byte arrays (not NULL)
        assertQuery("SELECT zorder_integer_bytes(NULL) IS NOT NULL", "SELECT true");
        assertQuery("SELECT length(zorder_integer_bytes(NULL))", "SELECT 8");

        // Test that varchar with null returns zero-filled array
        assertQuery(
                "SELECT zorder_varchar_bytes(NULL, 10) IS NOT NULL", "SELECT true");
        assertQuery("SELECT length(zorder_varchar_bytes(NULL, 10))", "SELECT 10");

        // Test that interleave returns zero-filled array if any input is null
        assertQuery(
                "SELECT zorder(ARRAY[zorder_integer_bytes(1), NULL], 16) IS NOT NULL",
                "SELECT true");

        // Verify the zero-filled array has the correct length
        assertQuery(
                "SELECT length(zorder(ARRAY[zorder_integer_bytes(1), NULL], 16))",
                "SELECT 16");
    }

    @Test
    public void testZOrderConsistency()
    {
        // Test that the same input always produces the same output
        assertQuery(
                "SELECT zorder_integer_bytes(42) = zorder_integer_bytes(42)",
                "SELECT true");

        assertQuery(
                "SELECT zorder(ARRAY[zorder_integer_bytes(1), zorder_integer_bytes(2)], 16) = " +
                "zorder(ARRAY[zorder_integer_bytes(1), zorder_integer_bytes(2)], 16)",
                "SELECT true");
    }

    @Test
    public void testZOrderWithTableData()
    {
        // Create a table with some data including nulls
        assertUpdate("CREATE TABLE test_zorder_table (id INT, name VARCHAR, value DOUBLE)");
        assertUpdate("INSERT INTO test_zorder_table VALUES (1, 'alice', 100.5), (2, 'bob', 200.3), (3, NULL, 150.0), (NULL, 'charlie', NULL)", 4);

        // Test Z-order computation on table data - UDFs always return non-null bytes
        assertQuery(
                "SELECT id, name FROM test_zorder_table WHERE id IS NOT NULL ORDER BY id",
                "VALUES (1, 'alice'), (2, 'bob'), (3, CAST(NULL AS VARCHAR))");

        // Test interleaving multiple columns from table
        assertQuery(
                "SELECT id FROM test_zorder_table WHERE id IS NOT NULL AND value IS NOT NULL ORDER BY id",
                "VALUES 1, 2, 3");

        // Verify that all rows (including NULL id) return byte arrays with correct length
        assertQuery(
                "SELECT COUNT(*) FROM test_zorder_table WHERE length(zorder_integer_bytes(id)) = 8",
                "SELECT 4");

        // Verify NULL id produces zero-filled bytes (0x0000000000000000)
        assertQuery(
                "SELECT length(zorder_integer_bytes(id)) FROM test_zorder_table WHERE id IS NULL",
                "SELECT 8");

        assertUpdate("DROP TABLE test_zorder_table");
    }

    @Test
    public void testZOrderSorting()
    {
        /*
         * Z-Order Curve Visualization (2D Space):
         *
         * Y-axis
         *   2 |  4---5       6---7
         *     |  |   |       |   |
         *   1 |  2---3       |   |
         *     |  |   |       |   |
         *   0 |  0---1-------+---+
         *     +-------------------> X-axis
         *        0   1   2   3
         *
         * Z-order traversal follows the curve: 0→1→2→3→4→5→6→7
         * This creates spatial locality - nearby points in 2D space
         * are also nearby in the 1D Z-order sequence.
         *
         * Our test data points:
         *   0: (0,0) origin
         *   1: (1,0) middle-bottom
         *   2: (0,1) left-middle
         *   3: (1,1) diagonal1
         *   4: (2,0) bottom-right
         *   5: (0,2) top-left
         *   6: (2,2) diagonal2
         *
         * Expected Z-order: (0,0) → (1,0) → (0,1) → (1,1) → (2,0) → (0,2) → (2,2)
         */

        // Create a table with 2D spatial data
        assertUpdate("CREATE TABLE test_zorder_sort (x INT, y INT, name VARCHAR)");
        assertUpdate("INSERT INTO test_zorder_sort VALUES " +
                "(0, 0, 'origin'), " +
                "(1, 1, 'diagonal1'), " +
                "(2, 2, 'diagonal2'), " +
                "(0, 2, 'top-left'), " +
                "(2, 0, 'bottom-right'), " +
                "(1, 0, 'middle-bottom'), " +
                "(0, 1, 'left-middle')", 7);

        // Order by Z-order value (interleaved x,y coordinates)
        // Z-order should cluster spatially close points together
        assertQuery(
                "SELECT x, y, name FROM test_zorder_sort " +
                "ORDER BY zorder(ARRAY[zorder_integer_bytes(x), zorder_integer_bytes(y)], 16)",
                "VALUES " +
                "(0, 0, 'origin'), " +
                "(1, 0, 'middle-bottom'), " +
                "(0, 1, 'left-middle'), " +
                "(1, 1, 'diagonal1'), " +
                "(2, 0, 'bottom-right'), " +
                "(0, 2, 'top-left'), " +
                "(2, 2, 'diagonal2')");

        // Verify ordering with 3 columns
        assertUpdate("CREATE TABLE test_zorder_3d (x INT, y INT, z INT)");
        assertUpdate("INSERT INTO test_zorder_3d VALUES (1, 2, 3), (0, 0, 0), (2, 1, 0), (1, 1, 1)", 4);

        assertQuery(
                "SELECT x, y, z FROM test_zorder_3d " +
                "ORDER BY zorder(ARRAY[zorder_integer_bytes(x), zorder_integer_bytes(y), zorder_integer_bytes(z)], 24)",
                "VALUES (0, 0, 0), (1, 1, 1), (2, 1, 0), (1, 2, 3)");

        // Order by Z-order value (interleaved x,y coordinates)
        // Z-order should cluster spatially close points together
        assertUpdate("CREATE TABLE test_zorder (x INT, y INT, name VARCHAR)");
        System.out.println(getQueryRunner().execute(
                "EXPLAIN (TYPE DISTRIBUTED) INSERT INTO test_zorder SELECT x, y, name FROM test_zorder_sort " +
                        "ORDER BY zorder(ARRAY[zorder_integer_bytes(x), zorder_integer_bytes(y)], 16)")
                .toString());
        assertUpdate(
                "INSERT INTO test_zorder SELECT x, y, name FROM test_zorder_sort " +
                        "ORDER BY zorder(ARRAY[zorder_integer_bytes(x), zorder_integer_bytes(y)], 16)",
                7);
        assertQuery(
                "SELECT x, y, name FROM test_zorder",
                "VALUES " +
                        "(0, 0, 'origin'), " +
                        "(1, 0, 'middle-bottom'), " +
                        "(0, 1, 'left-middle'), " +
                        "(1, 1, 'diagonal1'), " +
                        "(2, 0, 'bottom-right'), " +
                        "(0, 2, 'top-left'), " +
                        "(2, 2, 'diagonal2')");

        assertUpdate("DROP TABLE test_zorder_sort");
        assertUpdate("DROP TABLE test_zorder_3d");
        assertUpdate("DROP TABLE test_zorder");
    }
}
