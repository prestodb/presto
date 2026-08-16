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
        // Test that tinyint values are converted to ordered bytes (8 bytes)
        assertQuery("SELECT length(zorder(ROW(TINYINT '42')))", "VALUES 8");
        assertQuery("SELECT length(zorder(ROW(TINYINT '-42')))", "VALUES 8");

        // Verify actual hex values for known inputs
        // TINYINT 0: 0 ^ 0x8000000000000000 = 0x8000000000000000
        assertQuery("SELECT to_hex(zorder(ROW(TINYINT '0')))", "VALUES '8000000000000000'");

        // TINYINT 1: 1 ^ 0x8000000000000000 = 0x8000000000000001
        assertQuery("SELECT to_hex(zorder(ROW(TINYINT '1')))", "VALUES '8000000000000001'");

        // Test ordering: negative < positive
        assertQuery(
                "SELECT zorder(ROW(TINYINT '-1')) < zorder(ROW(TINYINT '1'))",
                "VALUES true");

        // Test that different values produce different outputs
        assertQuery(
                "SELECT zorder(ROW(TINYINT '1')) != zorder(ROW(TINYINT '2'))",
                "VALUES true");
    }

    @Test
    public void testZOrderSmallintBytes()
    {
        // Test that smallint values are converted to ordered bytes (8 bytes)
        assertQuery("SELECT length(zorder(ROW(SMALLINT '1000')))", "VALUES 8");
        assertQuery("SELECT length(zorder(ROW(SMALLINT '-1000')))", "VALUES 8");

        // Verify actual hex values
        assertQuery("SELECT to_hex(zorder(ROW(SMALLINT '0')))", "VALUES '8000000000000000'");
        assertQuery("SELECT to_hex(zorder(ROW(SMALLINT '1')))", "VALUES '8000000000000001'");

        // Test ordering
        assertQuery(
                "SELECT zorder(ROW(SMALLINT '-100')) < zorder(ROW(SMALLINT '100'))",
                "VALUES true");

        assertQuery(
                "SELECT zorder(ROW(SMALLINT '100')) < zorder(ROW(SMALLINT '200'))",
                "VALUES true");
    }

    @Test
    public void testZOrderIntegerBytes()
    {
        // Test that integer values are converted to ordered bytes (8 bytes)
        assertQuery("SELECT length(zorder(ROW(INTEGER '100000')))", "VALUES 8");
        assertQuery("SELECT length(zorder(ROW(INTEGER '-100000')))", "VALUES 8");

        // Verify actual hex values
        assertQuery("SELECT to_hex(zorder(ROW(INTEGER '0')))", "VALUES '8000000000000000'");
        assertQuery("SELECT to_hex(zorder(ROW(INTEGER '1')))", "VALUES '8000000000000001'");
        assertQuery("SELECT to_hex(zorder(ROW(INTEGER '42')))", "VALUES '800000000000002A'");

        // Test ordering
        assertQuery(
                "SELECT zorder(ROW(INTEGER '-1000')) < zorder(ROW(INTEGER '1000'))",
                "VALUES true");

        assertQuery(
                "SELECT zorder(ROW(INTEGER '100')) < zorder(ROW(INTEGER '200'))",
                "VALUES true");
    }

    @Test
    public void testZOrderBigintBytes()
    {
        // Test that bigint values are converted to ordered bytes (8 bytes)
        assertQuery("SELECT length(zorder(ROW(BIGINT '9223372036854775807')))", "VALUES 8");
        assertQuery("SELECT length(zorder(ROW(BIGINT '-9223372036854775808')))", "VALUES 8");

        // Verify actual hex values
        assertQuery("SELECT to_hex(zorder(ROW(BIGINT '0')))", "VALUES '8000000000000000'");
        assertQuery("SELECT to_hex(zorder(ROW(BIGINT '1')))", "VALUES '8000000000000001'");
        assertQuery("SELECT to_hex(zorder(ROW(BIGINT '100')))", "VALUES '8000000000000064'");

        // Test ordering
        assertQuery(
                "SELECT zorder(ROW(BIGINT '-1000000')) < zorder(ROW(BIGINT '1000000'))",
                "VALUES true");

        assertQuery(
                "SELECT zorder(ROW(BIGINT '1000')) < zorder(ROW(BIGINT '2000'))",
                "VALUES true");
    }

    @Test
    public void testZOrderRealBytes()
    {
        // Test that real values are converted to ordered bytes (8 bytes)
        assertQuery("SELECT length(zorder(ROW(REAL '3.14')))", "VALUES 8");
        assertQuery("SELECT length(zorder(ROW(REAL '-3.14')))", "VALUES 8");

        // Test ordering
        assertQuery(
                "SELECT zorder(ROW(REAL '-1.5')) < zorder(ROW(REAL '1.5'))",
                "VALUES true");

        assertQuery(
                "SELECT zorder(ROW(REAL '1.0')) < zorder(ROW(REAL '2.0'))",
                "VALUES true");
    }

    @Test
    public void testZOrderDoubleBytes()
    {
        // Test that double values are converted to ordered bytes (8 bytes)
        assertQuery("SELECT length(zorder(ROW(DOUBLE '3.141592653589793')))", "VALUES 8");
        assertQuery("SELECT length(zorder(ROW(DOUBLE '-3.141592653589793')))", "VALUES 8");

        // Test ordering
        assertQuery(
                "SELECT zorder(ROW(DOUBLE '-1.5')) < zorder(ROW(DOUBLE '1.5'))",
                "VALUES true");

        assertQuery(
                "SELECT zorder(ROW(DOUBLE '1.0')) < zorder(ROW(DOUBLE '2.0'))",
                "VALUES true");
    }

    @Test
    public void testZOrderBooleanBytes()
    {
        // Test that boolean values are converted to ordered bytes (8 bytes)
        assertQuery("SELECT length(zorder(ROW(true)))", "VALUES 8");
        assertQuery("SELECT length(zorder(ROW(false)))", "VALUES 8");

        // Verify actual hex values
        // false: 0x00 followed by 7 zero bytes
        assertQuery("SELECT to_hex(zorder(ROW(false)))", "VALUES '0000000000000000'");
        // true: 0x81 (which is -127 as signed byte) followed by 7 zero bytes
        assertQuery("SELECT to_hex(zorder(ROW(true)))", "VALUES '8100000000000000'");

        // Test ordering: false < true
        assertQuery(
                "SELECT zorder(ROW(false)) < zorder(ROW(true))",
                "VALUES true");

        // Test that different values produce different outputs
        assertQuery(
                "SELECT zorder(ROW(false)) != zorder(ROW(true))",
                "VALUES true");
    }

    @Test
    public void testZOrderVarcharBytes()
    {
        // Test that varchar values are converted to ordered bytes (32 bytes default)
        assertQuery("SELECT length(zorder(ROW('hello')))", "VALUES 32");
        assertQuery("SELECT length(zorder(ROW('world')))", "VALUES 32");

        // Test ordering (lexicographic)
        assertQuery(
                "SELECT zorder(ROW('apple')) < zorder(ROW('banana'))",
                "VALUES true");

        // Test that different strings produce different outputs
        assertQuery(
                "SELECT zorder(ROW('test')) != zorder(ROW('best'))",
                "VALUES true");
    }

    @Test
    public void testZOrderVarbinaryBytes()
    {
        // Test that varbinary values are converted to ordered bytes (32 bytes default)
        assertQuery("SELECT length(zorder(ROW(X'DEADBEEF')))", "VALUES 32");
        assertQuery("SELECT length(zorder(ROW(X'CAFE')))", "VALUES 32");

        // Test that different values produce different outputs
        assertQuery(
                "SELECT zorder(ROW(X'DEAD')) != zorder(ROW(X'BEEF'))",
                "VALUES true");
    }

    @Test
    public void testZOrderDateBytes()
    {
        // Test that date values are converted to ordered bytes (8 bytes)
        assertQuery("SELECT length(zorder(ROW(DATE '2024-01-01')))", "VALUES 8");
        assertQuery("SELECT length(zorder(ROW(DATE '1970-01-01')))", "VALUES 8");

        // Test ordering
        assertQuery(
                "SELECT zorder(ROW(DATE '2020-01-01')) < zorder(ROW(DATE '2024-01-01'))",
                "VALUES true");

        assertQuery(
                "SELECT zorder(ROW(DATE '2024-01-01')) < zorder(ROW(DATE '2024-12-31'))",
                "VALUES true");
    }

    @Test
    public void testZOrderTimestampBytes()
    {
        // Test that timestamp values are converted to ordered bytes (8 bytes)
        assertQuery("SELECT length(zorder(ROW(TIMESTAMP '2024-01-01 12:00:00')))", "VALUES 8");
        assertQuery("SELECT length(zorder(ROW(TIMESTAMP '1970-01-01 00:00:00')))", "VALUES 8");

        // Test ordering
        assertQuery(
                "SELECT zorder(ROW(TIMESTAMP '2020-01-01 00:00:00')) < " +
                "zorder(ROW(TIMESTAMP '2024-01-01 00:00:00'))",
                "VALUES true");

        assertQuery(
                "SELECT zorder(ROW(TIMESTAMP '2024-01-01 12:00:00')) < " +
                "zorder(ROW(TIMESTAMP '2024-01-01 13:00:00'))",
                "VALUES true");
    }

    @Test
    public void testZOrderWithRow()
    {
        // Test with 2 integer columns using ROW syntax - each INT is 8 bytes, so 2 INTs = 16 bytes
        assertQuery(
                "SELECT length(zorder(ROW(1, 2)))",
                "SELECT 16");

        // Verify the actual hex output for simple values
        // For ROW(0, 0): both values convert to 0x8000000000000000 (sign bit flipped)
        // After bit interleaving across 2 columns of 8 bytes each = 16 bytes output
        assertQuery(
                "SELECT to_hex(zorder(ROW(0, 0)))",
                "VALUES 'C0000000000000000000000000000000'");

        // For ROW(1, 1): both values convert to 0x8000000000000001
        // Interleaving produces a different pattern
        assertQuery(
                "SELECT to_hex(zorder(ROW(1, 1)))",
                "VALUES 'C0000000000000000000000000000003'");

        // Test that ROW-based zorder produces consistent results
        assertQuery(
                "SELECT zorder(ROW(42, 100)) = zorder(ROW(42, 100))",
                "SELECT true");

        // Verify different inputs produce different outputs
        assertQuery(
                "SELECT zorder(ROW(1, 2)) != zorder(ROW(2, 1))",
                "SELECT true");

        // Verify different inputs produce different hex outputs
        assertQuery(
                "SELECT to_hex(zorder(ROW(1, 2))) != to_hex(zorder(ROW(2, 1)))",
                "VALUES true");
    }

    @Test
    public void testZOrderRowWithNulls()
    {
        // Test that null values in ROW are handled correctly
        assertUpdate("CREATE TABLE test_zorder_row_nulls (a INT, b INT, c VARCHAR)");
        assertUpdate("INSERT INTO test_zorder_row_nulls VALUES (1, 2, 'test'), (NULL, 3, 'null_a'), (4, NULL, 'null_b')", 3);

        // All rows should produce non-null zorder values (zero-filled for nulls)
        assertQuery(
                "SELECT COUNT(*) FROM test_zorder_row_nulls WHERE zorder(ROW(a, b)) IS NOT NULL",
                "SELECT 3");

        // Verify null handling produces consistent byte patterns
        // NULL values should be treated as zero-filled bytes
        assertQuery(
                "SELECT a, b, length(zorder(ROW(a, b))) FROM test_zorder_row_nulls ORDER BY a NULLS FIRST",
                "VALUES (NULL, 3, 16), (1, 2, 16), (4, NULL, 16)");

        // Verify that NULL in first position produces different result than NULL in second position
        assertQuery(
                "SELECT zorder(ROW(CAST(NULL AS INT), 3)) != zorder(ROW(4, CAST(NULL AS INT)))",
                "SELECT true");

        assertUpdate("DROP TABLE test_zorder_row_nulls");
    }

    @Test
    public void testZOrderRowSorting()
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
         */

        // Create a table and verify ROW-based zorder produces correct ordering
        assertUpdate("CREATE TABLE test_zorder_row_sort (x INT, y INT)");
        assertUpdate("INSERT INTO test_zorder_row_sort VALUES (0, 0), (1, 0), (0, 1), (1, 1), (2, 0), (0, 2), (2, 2)", 7);

        // Verify specific hex values for corner cases
        assertQuery(
                "SELECT to_hex(zorder(ROW(0, 0))) FROM test_zorder_row_sort WHERE x = 0 AND y = 0",
                "VALUES 'C0000000000000000000000000000000'");

        assertQuery(
                "SELECT to_hex(zorder(ROW(1, 1))) FROM test_zorder_row_sort WHERE x = 1 AND y = 1",
                "VALUES 'C0000000000000000000000000000003'");

        // ROW-based zorder should produce the correct Z-order
        assertQuery(
                "SELECT x, y FROM test_zorder_row_sort ORDER BY zorder(ROW(x, y))",
                "VALUES (0, 0), (1, 0), (0, 1), (1, 1), (2, 0), (0, 2), (2, 2)");

        // Verify that zorder values are distinct for each point
        assertQuery(
                "SELECT COUNT(DISTINCT to_hex(zorder(ROW(x, y)))) FROM test_zorder_row_sort",
                "VALUES 7");

        assertUpdate("DROP TABLE test_zorder_row_sort");
    }

    @Test
    public void testZOrderRowMixedTypes()
    {
        // Test with various type combinations
        assertUpdate("CREATE TABLE test_zorder_mixed (i INT, d DOUBLE, b BOOLEAN, dt DATE, ts TIMESTAMP)");
        assertUpdate("INSERT INTO test_zorder_mixed VALUES " +
                "(1, 1.5, true, DATE '2024-01-01', TIMESTAMP '2024-01-01 12:00:00'), " +
                "(2, 2.5, false, DATE '2024-01-02', TIMESTAMP '2024-01-02 12:00:00')", 2);

        // Verify zorder works with mixed types - INT (8) + DOUBLE (8) + BOOLEAN (8) = 24 bytes
        assertQuery(
                "SELECT i, length(zorder(ROW(i, d, b))) FROM test_zorder_mixed ORDER BY i",
                "VALUES (1, 24), (2, 24)");

        // Verify with all supported types - INT (8) + DOUBLE (8) + BOOLEAN (8) + DATE (8) + TIMESTAMP (8) = 40 bytes
        assertQuery(
                "SELECT i, length(zorder(ROW(i, d, b, dt, ts))) FROM test_zorder_mixed ORDER BY i",
                "VALUES (1, 40), (2, 40)");

        // Verify different rows produce different zorder values
        assertQuery(
                "SELECT COUNT(DISTINCT zorder(ROW(i, d, b))) FROM test_zorder_mixed",
                "SELECT 2");

        // Verify ordering: row 1 should come before row 2 when ordered by zorder
        assertQuery(
                "SELECT i FROM test_zorder_mixed ORDER BY zorder(ROW(i, d, b))",
                "VALUES 1, 2");

        assertUpdate("DROP TABLE test_zorder_mixed");
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

        // Verify that all rows (including NULL id) return byte arrays with correct length
        // INT (8 bytes) + DOUBLE (8 bytes) = 16 bytes
        assertQuery(
                "SELECT COUNT(*) FROM test_zorder_table WHERE length(zorder(ROW(id, value))) = 16",
                "SELECT 4");

        // Verify each row produces a unique zorder value (nulls are treated as zeros)
        assertQuery(
                "SELECT COUNT(DISTINCT zorder(ROW(id, value))) FROM test_zorder_table",
                "SELECT 4");

        // Verify ordering by zorder produces consistent results
        assertQuery(
                "SELECT id FROM test_zorder_table WHERE id IS NOT NULL ORDER BY zorder(ROW(id, value))",
                "VALUES 1, 2, 3");

        assertUpdate("DROP TABLE test_zorder_table");
    }

    @Test
    public void testZOrderSorting3D()
    {
        // Verify ordering with 3 columns
        assertUpdate("CREATE TABLE test_zorder_3d (x INT, y INT, z INT)");
        assertUpdate("INSERT INTO test_zorder_3d VALUES (1, 2, 3), (0, 0, 0), (2, 1, 0), (1, 1, 1)", 4);

        // Verify correct byte length: 3 INTs * 8 bytes = 24 bytes
        assertQuery(
                "SELECT COUNT(*) FROM test_zorder_3d WHERE length(zorder(ROW(x, y, z))) = 24",
                "SELECT 4");

        // Verify Z-order produces expected ordering
        assertQuery(
                "SELECT x, y, z FROM test_zorder_3d ORDER BY zorder(ROW(x, y, z))",
                "VALUES (0, 0, 0), (1, 1, 1), (2, 1, 0), (1, 2, 3)");

        // Verify each point has a unique zorder value
        assertQuery(
                "SELECT COUNT(DISTINCT zorder(ROW(x, y, z))) FROM test_zorder_3d",
                "SELECT 4");

        // Verify specific ordering relationships
        assertQuery(
                "SELECT zorder(ROW(0, 0, 0)) < zorder(ROW(1, 1, 1))",
                "SELECT true");

        assertUpdate("DROP TABLE test_zorder_3d");
    }

    @Test
    public void testZOrderWithInsert()
    {
        // Test using zorder in INSERT ... ORDER BY
        assertUpdate("CREATE TABLE test_zorder_source (x INT, y INT, name VARCHAR)");
        assertUpdate("INSERT INTO test_zorder_source VALUES " +
                "(0, 0, 'origin'), " +
                "(1, 1, 'diagonal1'), " +
                "(2, 2, 'diagonal2'), " +
                "(0, 2, 'top-left'), " +
                "(2, 0, 'bottom-right'), " +
                "(1, 0, 'middle-bottom'), " +
                "(0, 1, 'left-middle')", 7);

        assertUpdate("CREATE TABLE test_zorder_target (x INT, y INT, name VARCHAR)");
        assertUpdate(
                "INSERT INTO test_zorder_target SELECT x, y, name FROM test_zorder_source ORDER BY zorder(ROW(x, y))",
                7);

        assertQuery(
                "SELECT x, y, name FROM test_zorder_target",
                "VALUES " +
                        "(0, 0, 'origin'), " +
                        "(1, 0, 'middle-bottom'), " +
                        "(0, 1, 'left-middle'), " +
                        "(1, 1, 'diagonal1'), " +
                        "(2, 0, 'bottom-right'), " +
                        "(0, 2, 'top-left'), " +
                        "(2, 2, 'diagonal2')");

        // Verify hex output for the origin point
        assertQuery(
                "SELECT to_hex(zorder(ROW(x, y))) FROM test_zorder_target WHERE name = 'origin'",
                "VALUES 'C0000000000000000000000000000000'");

        assertUpdate("DROP TABLE test_zorder_source");
        assertUpdate("DROP TABLE test_zorder_target");
    }

    @Test
    public void testZOrderConsistency()
    {
        // Test that the same input always produces the same output
        assertQuery(
                "SELECT zorder(ROW(42, 100)) = zorder(ROW(42, 100))",
                "SELECT true");

        // Test with different values produce different outputs
        assertQuery(
                "SELECT zorder(ROW(1, 2)) != zorder(ROW(2, 1))",
                "SELECT true");

        // Verify hex representation is consistent across multiple calls
        assertQuery(
                "SELECT to_hex(zorder(ROW(42, 100))) = to_hex(zorder(ROW(42, 100)))",
                "SELECT true");

        // Verify byte-by-byte equality
        assertQuery(
                "SELECT length(zorder(ROW(42, 100)))",
                "SELECT 16");

        // Test consistency across multiple evaluations - should always produce same hex
        assertQuery(
                "SELECT COUNT(DISTINCT to_hex(zorder(ROW(5, 10)))) FROM (VALUES 1, 2, 3, 4, 5)",
                "SELECT 1");

        // Verify specific known values produce expected hex output
        assertQuery(
                "SELECT to_hex(zorder(ROW(0, 0)))",
                "VALUES 'C0000000000000000000000000000000'");

        // Verify (1,1) produces a different but consistent hex value
        assertQuery(
                "SELECT to_hex(zorder(ROW(1, 1)))",
                "VALUES 'C0000000000000000000000000000003'");
    }

    @Test
    public void testZOrderWithVarchar()
    {
        assertUpdate("CREATE TABLE test_zorder_varchar (id INT, name VARCHAR)");
        assertUpdate("INSERT INTO test_zorder_varchar VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')", 3);

        // Verify zorder works with VARCHAR - INT (8 bytes) + VARCHAR (32 bytes default) = 40 bytes
        assertQuery(
                "SELECT id, length(zorder(ROW(id, name))) FROM test_zorder_varchar ORDER BY id",
                "VALUES (1, 40), (2, 40), (3, 40)");

        // Verify each row produces a unique zorder value
        assertQuery(
                "SELECT COUNT(DISTINCT zorder(ROW(id, name))) FROM test_zorder_varchar",
                "SELECT 3");

        // Verify ordering by zorder
        assertQuery(
                "SELECT name FROM test_zorder_varchar ORDER BY zorder(ROW(id, name))",
                "VALUES 'alice', 'bob', 'charlie'");

        // Verify different names produce different zorder values even with same id
        assertQuery(
                "SELECT zorder(ROW(1, 'alice')) != zorder(ROW(1, 'bob'))",
                "SELECT true");

        assertUpdate("DROP TABLE test_zorder_varchar");
    }

    @Test
    public void testZOrderWithDecimalFails()
    {
        assertUpdate("CREATE TABLE test_zorder_decimal (id INT, amount DECIMAL(10, 2))");
        assertUpdate("INSERT INTO test_zorder_decimal VALUES (1, 100.50), (2, 200.75), (3, 150.25)", 3);

        // Verify zorder with DECIMAL fails with appropriate error message
        assertQueryFails(
                "SELECT zorder(ROW(id, amount)) FROM test_zorder_decimal",
                "Cannot use column of type decimal in ZOrdering, the type is unsupported.*");

        assertUpdate("DROP TABLE test_zorder_decimal");
    }
}
