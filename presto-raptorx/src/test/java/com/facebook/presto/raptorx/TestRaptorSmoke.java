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
package com.facebook.presto.raptorx;

import com.facebook.presto.Session;
import com.facebook.presto.raptorx.storage.CompressionType;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.raptorx.RaptorQueryRunner.createRaptorQueryRunner;
import static com.facebook.presto.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static com.facebook.presto.spi.transaction.IsolationLevel.REPEATABLE_READ;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;

public class TestRaptorSmoke
        extends AbstractTestIntegrationSmokeTest
{
    @SuppressWarnings("unused")
    public TestRaptorSmoke()
    {
        this(() -> createRaptorQueryRunner(ImmutableMap.of(), true, false));
    }

    protected TestRaptorSmoke(QueryRunnerSupplier supplier)
    {
        super(supplier);
    }

    @AfterMethod
    public void afterMethod()
    {
        assertUpdate("CALL system.force_commit_cleanup()");
    }

    @Test
    public void testSchemaOperations()
    {
        assertUpdate("CREATE SCHEMA new_schema");
        assertQueryFails("CREATE SCHEMA new_schema", ".* Schema 'raptor\\.new_schema' already exists");

        assertQueryFails("ALTER SCHEMA new_schema RENAME TO new_schema", ".* Target schema 'raptor\\.new_schema' already exists");

        assertUpdate("CREATE SCHEMA new_schema2");
        assertQueryFails("ALTER SCHEMA new_schema RENAME TO new_schema2", ".* Target schema 'raptor\\.new_schema2' already exists");
        assertUpdate("DROP SCHEMA new_schema2");

        assertUpdate("ALTER SCHEMA new_schema RENAME TO new_schema2");
        assertUpdate("ALTER SCHEMA new_schema2 RENAME TO new_schema3");

        assertUpdate("DROP SCHEMA new_schema3");
        assertQueryFails("DROP SCHEMA new_schema3", ".* Schema 'raptor\\.new_schema3' does not exist");
    }

    @Test
    public void testSchemaRollback()
    {
        assertThat(queryColumn("SHOW SCHEMAS")).doesNotContain("tx_rollback");
        try (Transaction tx = new Transaction()) {
            tx.update("CREATE SCHEMA tx_rollback");
            tx.rollback();
        }
        assertThat(queryColumn("SHOW SCHEMAS")).doesNotContain("tx_rollback");
    }

    @Test
    public void testSchemaConflict()
    {
        try (Transaction tx1 = new Transaction(); Transaction tx2 = new Transaction()) {
            tx1.update("CREATE SCHEMA tx_conflict");
            tx2.update("CREATE SCHEMA tx_conflict");

            assertThat(tx1.queryColumn("SHOW SCHEMAS")).contains("tx_conflict");
            assertThat(tx2.queryColumn("SHOW SCHEMAS")).contains("tx_conflict");
            assertThat(queryColumn("SHOW SCHEMAS")).doesNotContain("tx_conflict");

            tx1.commit();
            assertConflict(tx2::commit);
        }

        assertThat(queryColumn("SHOW SCHEMAS")).contains("tx_conflict");
        assertUpdate("DROP SCHEMA tx_conflict");
        assertThat(queryColumn("SHOW SCHEMAS")).doesNotContain("tx_conflict");
    }

    @Test
    public void testSchemaListing()
    {
        assertSetPrefix(queryColumn("SHOW SCHEMAS"), "list");

        assertUpdate("CREATE SCHEMA list1");
        assertUpdate("CREATE SCHEMA list2");
        assertUpdate("CREATE SCHEMA list3");

        assertSetPrefix(queryColumn("SHOW SCHEMAS"), "list", "list1", "list2", "list3");

        try (Transaction tx = new Transaction()) {
            tx.update("CREATE SCHEMA list4");
            tx.update("ALTER SCHEMA list2 RENAME TO list5");
            tx.update("DROP SCHEMA list3");
            assertSetPrefix(tx.queryColumn("SHOW SCHEMAS"), "list", "list1", "list4", "list5");

            tx.update("ALTER SCHEMA list5 RENAME TO list6");
            assertSetPrefix(tx.queryColumn("SHOW SCHEMAS"), "list", "list1", "list4", "list6");

            tx.update("CREATE SCHEMA list2");
            tx.update("CREATE SCHEMA list3");
            tx.update("DROP SCHEMA list1");
            tx.update("DROP SCHEMA list4");
            assertSetPrefix(tx.queryColumn("SHOW SCHEMAS"), "list", "list2", "list3", "list6");

            assertSetPrefix(queryColumn("SHOW SCHEMAS"), "list", "list1", "list2", "list3");

            tx.commit();
        }

        assertSetPrefix(queryColumn("SHOW SCHEMAS"), "list", "list2", "list3", "list6");

        assertUpdate("DROP SCHEMA list2");
        assertUpdate("DROP SCHEMA list3");
        assertUpdate("DROP SCHEMA list6");

        assertSetPrefix(queryColumn("SHOW SCHEMAS"), "list");
    }

    @Test
    public void testTableOperations()
    {
        assertUpdate("CREATE TABLE new_table (x bigint)");
        assertQueryFails("CREATE TABLE new_table (x bigint)", ".* Table 'raptor\\.tpch\\.new_table' already exists");

        assertQueryFails("ALTER TABLE new_table RENAME TO new_table", ".* Target table 'raptor\\.tpch\\.new_table' already exists");

        assertUpdate("CREATE TABLE new_table2 (x bigint)");
        assertQueryFails("ALTER TABLE new_table RENAME TO new_table2", ".* Target table 'raptor\\.tpch\\.new_table2' already exists");
        assertUpdate("DROP TABLE new_table2");

        assertUpdate("ALTER TABLE new_table RENAME TO new_table2");
        assertUpdate("ALTER TABLE new_table2 RENAME TO new_table3");

        assertUpdate("DROP TABLE new_table3");
        assertQueryFails("DROP TABLE new_table3", ".* Table 'raptor\\.tpch\\.new_table3' does not exist");
    }

    @Test
    public void testTableRollback()
    {
        assertThat(queryColumn("SHOW TABLES")).doesNotContain("tx_rollback");
        try (Transaction tx = new Transaction()) {
            tx.update("CREATE TABLE tx_rollback (x bigint)");
            tx.rollback();
        }
        assertThat(queryColumn("SHOW TABLES")).doesNotContain("tx_rollback");
    }

    @Test
    public void testTableConflict()
    {
        try (Transaction tx1 = new Transaction(); Transaction tx2 = new Transaction()) {
            tx1.update("CREATE TABLE tx_conflict (x bigint)");
            tx2.update("CREATE TABLE tx_conflict (y bigint)");

            assertThat(tx1.queryColumn("SHOW TABLES")).contains("tx_conflict");
            assertThat(tx2.queryColumn("SHOW TABLES")).contains("tx_conflict");
            assertThat(queryColumn("SHOW TABLES")).doesNotContain("tx_conflict");

            tx1.commit();
            assertConflict(tx2::commit);
        }

        assertThat(queryColumn("SHOW TABLES")).contains("tx_conflict");
        assertQueryReturnsEmptyResult("SELECT x FROM tx_conflict");

        assertUpdate("DROP TABLE tx_conflict");
        assertThat(queryColumn("SHOW TABLES")).doesNotContain("tx_conflict");
    }

    @Test
    public void testTableListing()
    {
        assertSetPrefix(queryColumn("SHOW TABLES"), "list");

        assertUpdate("CREATE TABLE list1 (x bigint)");
        assertUpdate("CREATE TABLE list2 (x bigint)");
        assertUpdate("CREATE TABLE list3 (x bigint)");

        assertSetPrefix(queryColumn("SHOW TABLES"), "list", "list1", "list2", "list3");

        try (Transaction tx = new Transaction()) {
            tx.update("CREATE TABLE list4 (x bigint)");
            tx.update("ALTER TABLE list2 RENAME TO list5");
            tx.update("DROP TABLE list3");
            assertSetPrefix(tx.queryColumn("SHOW TABLES"), "list", "list1", "list4", "list5");

            tx.update("ALTER TABLE list5 RENAME TO list6");
            assertSetPrefix(tx.queryColumn("SHOW TABLES"), "list", "list1", "list4", "list6");

            tx.update("CREATE TABLE list2 (x bigint)");
            tx.update("CREATE TABLE list3 (x bigint)");
            tx.update("DROP TABLE list1");
            tx.update("DROP TABLE list4");
            assertSetPrefix(tx.queryColumn("SHOW TABLES"), "list", "list2", "list3", "list6");

            assertSetPrefix(queryColumn("SHOW TABLES"), "list", "list1", "list2", "list3");

            tx.commit();
        }

        assertSetPrefix(queryColumn("SHOW TABLES"), "list", "list2", "list3", "list6");

        assertUpdate("DROP TABLE list2");
        assertUpdate("DROP TABLE list3");
        assertUpdate("DROP TABLE list6");

        assertSetPrefix(queryColumn("SHOW TABLES"), "list");
    }

    @Test
    public void testDataIsolation()
    {
        assertUpdate("CREATE TABLE tx_isolation (x varchar)");
        assertEquals(queryColumn("SELECT * FROM tx_isolation"), set());

        try (Transaction tx1 = new Transaction(); Transaction tx2 = new Transaction()) {
            assertEquals(tx1.queryColumn("SELECT * FROM tx_isolation"), set());
            assertEquals(tx2.queryColumn("SELECT * FROM tx_isolation"), set());

            tx2.update("INSERT INTO tx_isolation VALUES 'a', 'b', 'c'", 3);

            assertEquals(queryColumn("SELECT * FROM tx_isolation"), set());
            assertEquals(tx1.queryColumn("SELECT * FROM tx_isolation"), set());
            assertEquals(tx2.queryColumn("SELECT * FROM tx_isolation"), set("a", "b", "c"));

            tx1.commit();
            tx2.commit();
        }

        assertEquals(queryColumn("SELECT * FROM tx_isolation"), set("a", "b", "c"));

        try (Transaction tx1 = new Transaction(); Transaction tx2 = new Transaction()) {
            assertEquals(tx1.queryColumn("SELECT * FROM tx_isolation"), set("a", "b", "c"));
            assertEquals(tx2.queryColumn("SELECT * FROM tx_isolation"), set("a", "b", "c"));

            tx2.update("DELETE FROM tx_isolation WHERE x = 'b'", 1);

            assertEquals(queryColumn("SELECT * FROM tx_isolation"), set("a", "b", "c"));
            assertEquals(tx1.queryColumn("SELECT * FROM tx_isolation"), set("a", "b", "c"));
            assertEquals(tx2.queryColumn("SELECT * FROM tx_isolation"), set("a", "c"));

            tx2.update("INSERT INTO tx_isolation VALUES 'd', 'e'", 2);

            assertEquals(queryColumn("SELECT * FROM tx_isolation"), set("a", "b", "c"));
            assertEquals(tx1.queryColumn("SELECT * FROM tx_isolation"), set("a", "b", "c"));
            assertEquals(tx2.queryColumn("SELECT * FROM tx_isolation"), set("a", "c", "d", "e"));

            tx2.update("DELETE FROM tx_isolation WHERE x = 'e'", 1);

            assertEquals(queryColumn("SELECT * FROM tx_isolation"), set("a", "b", "c"));
            assertEquals(tx1.queryColumn("SELECT * FROM tx_isolation"), set("a", "b", "c"));
            assertEquals(tx2.queryColumn("SELECT * FROM tx_isolation"), set("a", "c", "d"));

            tx1.commit();
            tx2.commit();
        }

        assertEquals(queryColumn("SELECT * FROM tx_isolation"), set("a", "c", "d"));
    }

    @Test
    public void testShowCreateTable()
    {
        String catalog = getSession().getCatalog().orElseThrow(AssertionError::new);
        String schema = getSession().getSchema().orElseThrow(AssertionError::new);

        @Language("SQL") String createSql = format("" +
                "CREATE TABLE %s.%s.test_show_create_table (\n" +
                "   c1 bigint,\n" +
                "   c2 double,\n" +
                "   \"c 3\" varchar,\n" +
                "   \"c'4\" array(bigint),\n" +
                "   c5 map(bigint, varchar),\n" +
                "   c6 bigint,\n" +
                "   c7 timestamp\n" +
                ")\n" +
                "COMMENT 'test''s table'\n" +
                "WITH (\n" +
                "   bucket_count = 42,\n" +
                "   bucketed_on = ARRAY['c1','c6'],\n" +
                "   compression_type = 'ZLIB',\n" +
                "   ordering = ARRAY['c6','c1'],\n" +
                "   temporal_column = 'c7'\n" +
                ")", catalog, schema);
        assertUpdate(createSql);

        assertQueryValue("SHOW CREATE TABLE test_show_create_table", createSql);
        assertQueryValue(format("SHOW CREATE TABLE %s.test_show_create_table", schema), createSql);
        assertQueryValue(format("SHOW CREATE TABLE %s.%s.test_show_create_table", catalog, schema), createSql);

        createSql = format("" +
                "CREATE TABLE %s.%s.\"test_show_create_table\"\"2\" (\n" +
                "   \"c\"\"1\" bigint,\n" +
                "   c2 double,\n" +
                "   \"c 3\" varchar,\n" +
                "   \"c'4\" array(bigint),\n" +
                "   c5 map(bigint, varchar)\n" +
                ")\n" +
                "WITH (\n" +
                "   bucket_count = 99,\n" +
                "   compression_type = 'LZ4'\n" +
                ")", catalog, schema);
        assertUpdate(createSql);

        assertQueryValue("SHOW CREATE TABLE \"test_show_create_table\"\"2\"", createSql);
    }

    @Test
    public void testAddColumn()
    {
        assertUpdate("CREATE TABLE test_add_column AS SELECT 123 x", 1);

        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN x date", ".* Column 'x' already exists");
        assertUpdate("ALTER TABLE test_add_column ADD COLUMN y bigint");
        assertQuery("SELECT * FROM test_add_column", "VALUES (123, null)");

        assertUpdate("INSERT INTO test_add_column VALUES (888, 777)", 1);
        assertQuery("SELECT * FROM test_add_column", "VALUES (123, null), (888, 777)");

        assertUpdate("ALTER TABLE test_add_column DROP COLUMN x");
        assertQuery("SELECT * FROM test_add_column", "VALUES (null), (777)");

        assertUpdate("ALTER TABLE test_add_column ADD COLUMN x bigint");
        assertQuery("SELECT * FROM test_add_column", "VALUES (null, null), (777, null)");

        assertUpdate("INSERT INTO test_add_column VALUES (333, 444)", 1);
        assertQuery("SELECT * FROM test_add_column", "VALUES (null, null), (777, null), (333, 444)");
    }

    @Test
    public void testDropColumn()
    {
        assertUpdate("CREATE TABLE test_drop_column AS SELECT 123 x, 111 a", 1);

        assertUpdate("ALTER TABLE test_drop_column DROP COLUMN x");
        assertQueryFails("SELECT x FROM test_drop_column", ".* Column 'x' cannot be resolved");
        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN a", ".* Cannot drop the only column in a table");
        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN \"$chunk_id\"", ".* Cannot drop hidden column");

        assertEquals(queryColumn("SELECT * FROM test_drop_column"), set("111"));

        assertUpdate("INSERT INTO test_drop_column VALUES 222, 333", 2);
        assertEquals(queryColumn("SELECT * FROM test_drop_column"), set("111", "222", "333"));

        assertUpdate("DELETE FROM test_drop_column WHERE a = 222", 1);
        assertEquals(queryColumn("SELECT * FROM test_drop_column"), set("111", "333"));

        assertUpdate("DROP TABLE test_drop_column");
        assertUpdate("CREATE TABLE test_drop_column (\n" +
                "  ds date,\n" +
                "  userid bigint,\n" +
                "  site varchar,\n" +
                "  page varchar,\n" +
                "  clicks bigint\n" +
                ")\n" +
                "WITH (\n" +
                "  temporal_column = 'ds',\n" +
                "  bucket_count = 50,\n" +
                "  bucketed_on = ARRAY['userid'],\n" +
                "  ordering = ARRAY['site', 'page']\n" +
                ")");

        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN ds", "Cannot drop the temporal column");
        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN userid", "Cannot drop bucket columns");
        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN site", "Cannot drop sort columns");
        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN page", "Cannot drop sort columns");
        assertUpdate("ALTER TABLE test_drop_column DROP COLUMN clicks");
    }

    @Test
    public void testRenameColumn()
    {
        assertUpdate("CREATE TABLE test_rename_column AS SELECT 123 x, 456 y", 1);

        assertQueryFails("ALTER TABLE test_rename_column RENAME COLUMN z TO z", ".* Column 'z' does not exist");
        assertQueryFails("ALTER TABLE test_rename_column RENAME COLUMN a TO b", ".* Column 'a' does not exist");
        assertQueryFails("ALTER TABLE test_rename_column RENAME COLUMN a TO x", ".* Column 'a' does not exist");
        assertQueryFails("ALTER TABLE test_rename_column RENAME COLUMN x TO x", ".* Column 'x' already exists");
        assertQueryFails("ALTER TABLE test_rename_column RENAME COLUMN x TO y", ".* Column 'y' already exists");

        assertUpdate("ALTER TABLE test_rename_column RENAME COLUMN x TO z");
        assertQuery("SELECT * FROM test_rename_column", "VALUES (123, 456)");
        assertQuery("SELECT y, z FROM test_rename_column", "VALUES (456, 123)");
    }

    @Test
    public void testRollbackRecovery()
    {
        long rows = computeActual("CREATE TABLE tx_recovery_insert AS SELECT orderkey FROM orders")
                .getUpdateCount().orElseThrow(AssertionError::new);

        assertUpdate("CREATE TABLE tx_recovery_add (x bigint)");

        try (Transaction tx1 = new Transaction(); Transaction tx2 = new Transaction()) {
            tx1.update("CREATE TABLE tx_recovery_create (x bigint)");
            tx1.update("ALTER TABLE tx_recovery_add ADD COLUMN y bigint");
            tx1.update("INSERT INTO tx_recovery_insert SELECT orderkey FROM orders", rows);

            // create a conflict as the last step force a rollback
            tx1.update("CREATE TABLE tx_recovery_conflict (x bigint)");
            tx2.update("CREATE TABLE tx_recovery_conflict (x bigint)");
            tx2.commit();
            assertConflict(tx1::commit);
        }

        assertThat(queryColumn("SHOW TABLES")).doesNotContain("tx_recovery_create");
        assertQuery("SELECT count(*) FROM tx_recovery_insert", "SELECT count(*) FROM orders");

        assertUpdate("CALL system.force_recovery()");
    }

    @Test
    public void testTableSystemTable()
    {
        assertUpdate("CALL system.create_distribution('sys_tables_dist', 50, ARRAY['bigint', 'bigint'])");

        assertUpdate("" +
                "CREATE TABLE sys_tables_t0 (c00 timestamp, c01 varchar, c02 double, c03 bigint, c04 bigint)");
        assertUpdate("" +
                "CREATE TABLE sys_tables_t1 (c10 timestamp, c11 varchar, c12 double, c13 bigint, c14 bigint) " +
                "WITH (temporal_column = 'c10')");
        assertUpdate("" +
                "CREATE TABLE sys_tables_t2 (c20 timestamp, c21 varchar, c22 double, c23 bigint, c24 bigint) " +
                "WITH (temporal_column = 'c20', ordering = ARRAY['c22', 'c21'])");
        assertUpdate("" +
                "CREATE TABLE sys_tables_t3 (c30 timestamp, c31 varchar, c32 double, c33 bigint, c34 bigint) " +
                "WITH (temporal_column = 'c30', bucket_count = 40, bucketed_on = ARRAY['c34', 'c33'])");
        assertUpdate("" +
                "CREATE TABLE sys_tables_t4 (c40 timestamp, c41 varchar, c42 double, c43 bigint, c44 bigint) " +
                "WITH (temporal_column = 'c40', ordering = ARRAY['c41', 'c42'], " +
                "distribution_name = 'sys_tables_dist', bucket_count = 50, bucketed_on = ARRAY['c43', 'c44'])");
        assertUpdate("" +
                "CREATE TABLE sys_tables_t5 (c50 timestamp, c51 varchar, c52 double, c53 bigint, c54 bigint) " +
                "WITH (ordering = ARRAY['c51', 'c52'], " +
                "distribution_name = 'sys_tables_dist', bucket_count = 50, bucketed_on = ARRAY['c53', 'c54'])");

        assertThat(computeActual("SELECT * FROM system.tables")).contains(
                row("tpch", "sys_tables_t0", null, null, null, null, null),
                row("tpch", "sys_tables_t1", "c10", null, null, null, null),
                row("tpch", "sys_tables_t2", "c20", list("c22", "c21"), null, null, null),
                row("tpch", "sys_tables_t3", "c30", null, null, 40L, list("c34", "c33")),
                row("tpch", "sys_tables_t4", "c40", list("c41", "c42"), "sys_tables_dist", 50L, list("c43", "c44")),
                row("tpch", "sys_tables_t5", null, list("c51", "c52"), "sys_tables_dist", 50L, list("c53", "c54")));

        assertThat(queryColumn("SELECT table_name FROM system.tables WHERE table_schema = 'tpch'"))
                .filteredOn(name -> name.startsWith("sys_tables_t")).hasSize(6);

        assertRowCount("SELECT * FROM system.tables WHERE table_name = 'sys_tables_t3'", 1);
        assertRowCount("SELECT * FROM system.tables WHERE table_schema = 'tpch' AND table_name = 'sys_tables_t3'", 1);
        assertRowCount("SELECT * FROM system.tables WHERE table_name LIKE 'sys_tables_t%'", 6);
        assertRowCount("SELECT * FROM system.tables WHERE table_schema = 'foo'", 0);

        assertUpdate("DROP TABLE sys_tables_t0");
        assertUpdate("DROP TABLE sys_tables_t1");
        assertUpdate("DROP TABLE sys_tables_t2");
        assertUpdate("DROP TABLE sys_tables_t3");
        assertUpdate("DROP TABLE sys_tables_t4");
        assertUpdate("DROP TABLE sys_tables_t5");

        assertRowCount("SELECT * FROM system.tables WHERE table_name LIKE 'sys_tables_t%'", 0);
    }

    @Test
    public void testTableSystemTableIsolation()
    {
        @Language("SQL") String listTables = "SELECT table_name FROM system.tables";

        assertUpdate("CREATE TABLE sys_tables_x0 (x bigint)");

        try (Transaction tx = new Transaction()) {
            tx.update("CREATE TABLE sys_tables_x1 (x bigint)");

            assertSetPrefix(queryColumn(listTables), "sys_tables_x", "sys_tables_x0");
            assertSetPrefix(tx.queryColumn(listTables), "sys_tables_x", "sys_tables_x0", "sys_tables_x1");

            tx.update("ALTER TABLE sys_tables_x0 RENAME TO sys_tables_x2");

            assertSetPrefix(queryColumn(listTables), "sys_tables_x", "sys_tables_x0");
            assertSetPrefix(tx.queryColumn(listTables), "sys_tables_x", "sys_tables_x1", "sys_tables_x2");

            tx.commit();
        }

        assertSetPrefix(queryColumn(listTables), "sys_tables_x", "sys_tables_x1", "sys_tables_x2");
    }

    @Test
    public void testChunkSystemTable()
    {
        @Language("SQL") String from = "FROM system.shards WHERE table_schema = 'tpch' AND table_name = 'sys_chunks'";

        assertUpdate("CREATE TABLE sys_chunks (x varchar) WITH (bucket_count = 50, bucketed_on = ARRAY['x'])");
        assertRowCount("SELECT * " + from, 0);

        assertUpdate("INSERT INTO sys_chunks VALUES 'a', 'b', 'c'", 3);
        assertRowCount("SELECT * " + from, 3);
        assertRowCount("SELECT CAST(MIN(min_timestamp) AS DATE) AS min_timestamp " + from, 1);

        Set<String> chunkIds = queryColumn("SELECT chunk_id " + from);

        try (Transaction tx1 = new Transaction(); Transaction tx2 = new Transaction()) {
            assertEquals(tx1.queryColumn("SELECT chunk_id " + from), chunkIds);
            assertEquals(tx2.queryColumn("SELECT chunk_id " + from), chunkIds);

            tx1.update("INSERT INTO sys_chunks VALUES 'd'", 1);

            Set<String> tx1ChunkIds = tx1.queryColumn("SELECT chunk_id " + from);
            assertEquals(tx1ChunkIds.size(), 4);
            assertNotEquals(tx1ChunkIds, chunkIds);
            assertEquals(tx2.queryColumn("SELECT chunk_id " + from), chunkIds);
            assertEquals(queryColumn("SELECT chunk_id " + from), chunkIds);

            tx2.update("INSERT INTO sys_chunks VALUES 'e'", 1);

            assertEquals(tx1.queryColumn("SELECT chunk_id " + from), tx1ChunkIds);
            Set<String> tx2ChunkIds = tx2.queryColumn("SELECT chunk_id " + from);
            assertEquals(tx2ChunkIds.size(), 4);
            assertNotEquals(tx2ChunkIds, chunkIds);
            assertEquals(queryColumn("SELECT chunk_id " + from), chunkIds);

            tx1.update("DELETE FROM sys_chunks WHERE x = 'b'", 1);

            assertNotEquals(tx1.queryColumn("SELECT chunk_id " + from), tx1ChunkIds);
            assertEquals(tx2.queryColumn("SELECT chunk_id " + from), tx2ChunkIds);
            assertEquals(queryColumn("SELECT chunk_id " + from), chunkIds);

            tx1.commit();
            tx2.commit();
        }

        assertEquals(queryColumn("SELECT * FROM sys_chunks"), set("a", "c", "d", "e"));
    }

    @Test
    public void testTableStatsSystemTable()
    {
        assertQuery("" +
                        "SELECT table_schema, table_name, sum(row_count)\n" +
                        "FROM system.table_stats\n" +
                        "WHERE table_schema = 'tpch'\n" +
                        "  AND table_name IN ('orders', 'lineitem')\n" +
                        "GROUP BY 1, 2",
                "" +
                        "SELECT 'tpch', 'orders', (SELECT count(*) FROM orders)\n" +
                        "UNION ALL\n" +
                        "SELECT 'tpch', 'lineitem', (SELECT count(*) FROM lineitem)");

        assertQuery("" +
                        "SELECT\n" +
                        "  bool_and(row_count >= chunk_count),\n" +
                        "  bool_and(update_time >= create_time),\n" +
                        "  bool_and(table_version >= 1)\n" +
                        "FROM system.table_stats\n" +
                        "WHERE row_count > 0",
                "SELECT true, true, true");

        // create empty table
        assertUpdate("CREATE TABLE test_table_stats (x bigint)");

        @Language("SQL") String sql = "" +
                "SELECT create_time, update_time, table_version," +
                "  chunk_count, row_count, uncompressed_size\n" +
                "FROM system.table_stats\n" +
                "WHERE table_schema = 'tpch'\n" +
                "  AND table_name = 'test_table_stats'";
        MaterializedRow row = getOnlyElement(computeActual(sql).getMaterializedRows());

        LocalDateTime createTime = (LocalDateTime) row.getField(0);
        LocalDateTime updateTime = (LocalDateTime) row.getField(1);
        assertEquals(createTime, updateTime);

        long tableVersion = (long) row.getField(2);
        assertEquals(row.getField(3), 0L, "chunk_count");
        assertEquals(row.getField(4), 0L, "row_count");
        long uncompressedSize = (long) row.getField(5);

        // insert
        assertUpdate("INSERT INTO test_table_stats VALUES (1), (2), (3), (4)", 4);
        row = getOnlyElement(computeActual(sql).getMaterializedRows());

        assertEquals(row.getField(0), createTime);
        LocalDateTime updateTime2 = (LocalDateTime) row.getField(1);
        assertThat(updateTime2).isAfterOrEqualTo(updateTime);

        long tableVersion2 = (long) row.getField(2);
        assertThat(tableVersion2).isGreaterThan(tableVersion);

        assertThat((long) row.getField(3)).as("chunk_count").isGreaterThanOrEqualTo(1);
        assertEquals(row.getField(4), 4L, "row_count");
        long uncompressedSize2 = (long) row.getField(5);
        assertThat(uncompressedSize2).isGreaterThan(uncompressedSize);

        // delete
        assertUpdate("DELETE FROM test_table_stats WHERE x IN (2, 4)", 2);
        row = getOnlyElement(computeActual(sql).getMaterializedRows());

        assertEquals(row.getField(0), createTime);
        LocalDateTime updateTime3 = (LocalDateTime) row.getField(1);
        assertThat(updateTime3).isAfterOrEqualTo(updateTime2);

        long tableVersion3 = (long) row.getField(2);
        assertThat(tableVersion3).isGreaterThan(tableVersion2);

        assertThat((long) row.getField(3)).as("chunk_count").isGreaterThanOrEqualTo(1);
        assertEquals(row.getField(4), 2L, "row_count");
        long uncompressedSize3 = (long) row.getField(5);
        assertThat(uncompressedSize3).isLessThan(uncompressedSize2);

        // add column
        assertUpdate("ALTER TABLE test_table_stats ADD COLUMN y bigint");
        row = getOnlyElement(computeActual(sql).getMaterializedRows());

        assertEquals(row.getField(0), createTime);
        LocalDateTime updateTime4 = (LocalDateTime) row.getField(1);
        assertThat(updateTime4).isAfterOrEqualTo(updateTime3);

        long tableVersion4 = (long) row.getField(2);
        assertThat(tableVersion4).isGreaterThan(tableVersion3);

        assertEquals(row.getField(4), 2L, "row_count");
        assertEquals(row.getField(5), uncompressedSize3);

        // cleanup
        assertUpdate("DROP TABLE test_table_stats");
    }

    @Test
    public void testBucketNumberColumn()
    {
        assertUpdate("" +
                "CREATE TABLE test_bucket_number (x bigint)\n" +
                "WITH (bucket_count = 8, bucketed_on = ARRAY['x'])");
        assertUpdate("INSERT INTO test_bucket_number SELECT orderkey FROM orders", "SELECT count(*) FROM orders");
        assertQuery("SELECT count(DISTINCT \"$bucket_number\") FROM test_bucket_number", "VALUES (8)");
        assertUpdate("DROP TABLE test_bucket_number");

        assertUpdate("CREATE TABLE test_bucket_number (x bigint)");
        assertQueryFails("SELECT \"$bucket_number\" from test_bucket_number", "line 1:8: Column '\\$bucket_number' cannot be resolved");
        assertUpdate("DROP TABLE test_bucket_number");
    }

    @Test
    public void testCompressionType()
    {
        for (CompressionType type : CompressionType.values()) {
            assertUpdate(format("CREATE TABLE test_compress (x bigint) WITH (compression_type = '%s')", type.name()));
            assertUpdate("INSERT INTO test_compress SELECT orderkey FROM orders", "SELECT count(*) FROM orders");
            assertQuery("SELECT x FROM test_compress", "SELECT orderkey FROM orders");
            assertUpdate("DELETE FROM test_compress WHERE x % 2 = 0", "SELECT count(*) FROM orders WHERE orderkey % 2 = 0");
            assertQuery("SELECT x FROM test_compress", "SELECT orderkey FROM orders WHERE orderkey % 2 <> 0");
            assertUpdate("DROP TABLE test_compress");
        }
    }

    @Test
    public void testSpecialColumnsNotNull()
    {
        assertUpdate("" +
                "CREATE TABLE test_special_columns (x bigint, b bigint, t date)\n" +
                "WITH (bucket_count = 8, bucketed_on = ARRAY['b'], temporal_column = 't')");
        assertUpdate("INSERT INTO test_special_columns VALUES (null, 123, DATE '2018-02-03')", 1);
        assertQueryFails("INSERT INTO test_special_columns VALUES (null, null, DATE '2018-02-03')", "Bucket column cannot be null");
        assertQueryFails("INSERT INTO test_special_columns VALUES (null, 123, null)", "Temporal column cannot be null");
        assertUpdate("DROP TABLE test_special_columns");
    }

    @Test
    public void testInsertSelectDecimal()
    {
        assertUpdate("CREATE TABLE test_decimal(short_decimal DECIMAL(5,2), long_decimal DECIMAL(25,20))");
        assertUpdate("INSERT INTO test_decimal VALUES(DECIMAL '123.45', DECIMAL '12345.12345678901234567890')", "VALUES(1)");
        assertUpdate("INSERT INTO test_decimal VALUES(NULL, NULL)", "VALUES(1)");
        assertQuery("SELECT * FROM test_decimal", "VALUES (123.45, 12345.12345678901234567890), (NULL, NULL)");
        assertUpdate("DROP TABLE test_decimal");
    }

    @Test
    public void testColumnRangesSystemTable()
    {
        assertQuery("SELECT orderkey_min, orderkey_max, custkey_min, custkey_max, orderdate_min, orderdate_max FROM \"orders$column_ranges\"",
                "SELECT min(orderkey), max(orderkey), min(custkey), max(custkey), min(orderdate), max(orderdate) FROM orders");

        assertQuery("SELECT orderkey_min, orderkey_max FROM \"orders$column_ranges\"",
                "SELECT min(orderkey), max(orderkey) FROM orders");

        // No such table test
        assertQueryFails("SELECT * FROM \"no_table$column_ranges\"", ".*raptor\\.tpch\\.no_table\\$column_ranges does not exist.*");

        // No range column for DOUBLE, INTEGER or VARCHAR
        assertQueryFails("SELECT totalprice_min FROM \"orders$column_ranges\"", ".*Column 'totalprice_min' cannot be resolved.*");
        assertQueryFails("SELECT shippriority_min FROM \"orders$column_ranges\"", ".*Column 'shippriority_min' cannot be resolved.*");
        assertQueryFails("SELECT orderstatus_min FROM \"orders$column_ranges\"", ".*Column 'orderstatus_min' cannot be resolved.*");
        assertQueryFails("SELECT orderpriority_min FROM \"orders$column_ranges\"", ".*Column 'orderpriority_min' cannot be resolved.*");
        assertQueryFails("SELECT clerk_min FROM \"orders$column_ranges\"", ".*Column 'clerk_min' cannot be resolved.*");
        assertQueryFails("SELECT comment_min FROM \"orders$column_ranges\"", ".*Column 'comment_min' cannot be resolved.*");

        // Empty table
        assertUpdate("CREATE TABLE column_ranges_test (a BIGINT, b BIGINT)");
        assertQuery("SELECT a_min, a_max, b_min, b_max FROM \"column_ranges_test$column_ranges\"", "SELECT NULL, NULL, NULL, NULL");

        // Table with NULL values
        assertUpdate("INSERT INTO column_ranges_test VALUES (1, NULL)", 1);
        assertQuery("SELECT a_min, a_max, b_min, b_max FROM \"column_ranges_test$column_ranges\"", "SELECT 1, 1, NULL, NULL");
        assertUpdate("INSERT INTO column_ranges_test VALUES (NULL, 99)", 1);
        assertQuery("SELECT a_min, a_max, b_min, b_max FROM \"column_ranges_test$column_ranges\"", "SELECT 1, 1, 99, 99");
        assertUpdate("INSERT INTO column_ranges_test VALUES (50, 50)", 1);
        assertQuery("SELECT a_min, a_max, b_min, b_max FROM \"column_ranges_test$column_ranges\"", "SELECT 1, 50, 50, 99");

        // Drop table
        assertUpdate("DROP TABLE column_ranges_test");
        assertQueryFails("SELECT a_min, a_max, b_min, b_max FROM \"column_ranges_test$column_ranges\"",
                ".*raptor\\.tpch\\.column_ranges_test\\$column_ranges does not exist.*");
    }

    protected void assertQueryValue(@Language("SQL") String sql, String expected)
    {
        assertEquals(computeActual(sql).getOnlyValue(), expected);
    }

    protected void assertRowCount(@Language("SQL") String sql, int expectedCount)
    {
        assertEquals(computeActual(sql).getRowCount(), expectedCount);
    }

    protected Set<String> queryColumn(@Language("SQL") String sql)
    {
        return computeActual(sql).getOnlyColumn()
                .map(value -> (value == null) ? null : String.valueOf(value))
                .collect(toSet());
    }

    protected static void assertSetPrefix(Set<String> actual, String prefix, String... expected)
    {
        assertThat(actual)
                .filteredOn(value -> value.startsWith(prefix))
                .containsExactlyInAnyOrder(expected);
    }

    protected static void assertConflict(ThrowingRunnable runnable)
    {
        PrestoException e = expectThrows(PrestoException.class, runnable);
        assertEquals(e.getErrorCode(), TRANSACTION_CONFLICT.toErrorCode());
    }

    @SafeVarargs
    protected static <T> MaterializedRow row(T... values)
    {
        return new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, (Object[]) values);
    }

    @SafeVarargs
    protected static <T> List<T> list(T... values)
    {
        return ImmutableList.copyOf(values);
    }

    @SafeVarargs
    protected static <T> Set<T> set(T... values)
    {
        return ImmutableSet.copyOf(values);
    }

    protected final class Transaction
            implements Closeable
    {
        private final Session session;

        public Transaction()
        {
            TransactionId transactionId = transaction().beginTransaction(REPEATABLE_READ, false, false);
            session = getSession().beginTransactionId(transactionId, transaction(), getQueryRunner().getAccessControl());
        }

        public void commit()
        {
            execute(() -> getFutureValue(transaction().asyncCommit(session.getRequiredTransactionId())));
        }

        public void rollback()
        {
            execute(() -> getFutureValue(transaction().asyncAbort(session.getRequiredTransactionId())));
        }

        public MaterializedResult query(@Language("SQL") String sql)
        {
            return computeActual(session, sql);
        }

        public Set<String> queryColumn(@Language("SQL") String sql)
        {
            return query(sql).getOnlyColumn()
                    .map(value -> (value == null) ? null : String.valueOf(value))
                    .collect(toSet());
        }

        public void update(@Language("SQL") String sql)
        {
            assertUpdate(session, sql);
        }

        public void update(@Language("SQL") String sql, long count)
        {
            assertUpdate(session, sql, count);
        }

        private void execute(Runnable runnable)
        {
            try {
                runnable.run();
            }
            catch (Throwable t) {
                throwIfUnchecked(t);
                throw new RuntimeException(t);
            }
        }

        @Override
        public void close()
        {
            if (transaction().transactionExists(session.getRequiredTransactionId())) {
                fail("Test did not commit or rollback");
            }
        }

        private TransactionManager transaction()
        {
            return getQueryRunner().getTransactionManager();
        }
    }
}
