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
package com.facebook.presto.ducklake;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * End-to-end metadata tests against the {@link TestingDuckLakeCatalog} fixture, run through a
 * real {@code ducklake} catalog and {@link com.facebook.presto.tests.DistributedQueryRunner}.
 * The split manager and page source provider are still stubs, so these tests only cover query
 * paths that resolve at analysis time (schema/table listing, {@code $snapshots}, and errors) and
 * never request a split.
 */
public class TestDuckLakeMetadataQueries
        extends AbstractTestQueryFramework
{
    private DuckLakeQueryRunner duckLakeQueryRunner;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        duckLakeQueryRunner = DuckLakeQueryRunner.createQueryRunner();
        return duckLakeQueryRunner.getQueryRunner();
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void close()
            throws Exception
    {
        super.close();
        duckLakeQueryRunner.getTestingCatalog().close();
    }

    @Test
    public void testShowSchemas()
    {
        MaterializedResult result = computeActual("SHOW SCHEMAS");
        List<String> schemaNames = result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0))
                .collect(Collectors.toList());
        assertTrue(schemaNames.containsAll(List.of(
                "del", "evo", "inl", "main", "merge", "part", "tpch", "types", "information_schema")));
    }

    @Test
    public void testShowTablesFromTpch()
    {
        MaterializedResult result = computeActual("SHOW TABLES FROM tpch");
        List<String> tableNames = result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0))
                .collect(Collectors.toList());
        assertEquals(
                tableNames.stream().sorted().collect(Collectors.toList()),
                List.of("customer", "nation", "orders", "region"));
    }

    @Test
    public void testDescribeOrders()
    {
        MaterializedResult result = computeActual("DESCRIBE tpch.orders");
        List<MaterializedRow> rows = result.getMaterializedRows();
        List<String> columnNames = rows.stream()
                .map(row -> (String) row.getField(0))
                .collect(Collectors.toList());
        List<String> columnTypes = rows.stream()
                .map(row -> (String) row.getField(1))
                .collect(Collectors.toList());
        assertEquals(columnNames, List.of(
                "o_orderkey",
                "o_custkey",
                "o_orderstatus",
                "o_totalprice",
                "o_orderdate",
                "o_orderpriority",
                "o_clerk",
                "o_shippriority",
                "o_comment"));
        assertEquals(columnTypes, List.of(
                "bigint",
                "bigint",
                "varchar",
                "double",
                "date",
                "varchar",
                "varchar",
                "integer",
                "varchar"));
    }

    @Test
    public void testInformationSchemaColumnsListsNestedTable()
    {
        MaterializedResult result = computeActual(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = 'types' AND table_name = 'nested'");
        List<String> columnNames = result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0))
                .collect(Collectors.toList());
        assertEquals(columnNames, List.of("id", "list_col", "struct_col", "map_col", "struct_with_list"));
    }

    @Test
    public void testOrdersSnapshotsSystemTable()
    {
        MaterializedResult result = computeActual("SELECT * FROM \"tpch\".\"orders$snapshots\"");
        assertEquals(result.getRowCount(), 46);
    }

    @Test
    public void testOrdersSnapshotsCommitMessages()
    {
        MaterializedResult result = computeActual(
                "SELECT commit_message FROM \"tpch\".\"orders$snapshots\" WHERE commit_message IS NOT NULL");
        List<String> commitMessages = result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0))
                .collect(Collectors.toList());
        assertEquals(
                commitMessages.stream().sorted().collect(Collectors.toList()),
                List.of(
                        "Insert 1000 rows for del.simple fixture",
                        "Load TPC-H tiny fixture (nation, region, customer, orders)"));
    }

    @Test
    public void testOrdersSnapshotsFirstSnapshotIsZero()
    {
        MaterializedResult result = computeActual(
                "SELECT snapshot_id FROM \"tpch\".\"orders$snapshots\" ORDER BY snapshot_id LIMIT 1");
        assertEquals(result.getOnlyValue(), 0L);
    }

    @Test
    public void testUnsupportedTypeQueryFails()
    {
        assertQueryFails(
                "SELECT 1 FROM types.unsupported",
                ".*Unsupported DuckLake type 'interval' for column 'interval_col'.*");
    }

    @Test
    public void testVersionAsOfUnknownSnapshotFails()
    {
        assertQueryFails(
                "SELECT * FROM tpch.orders FOR SYSTEM_VERSION AS OF CAST(999999 AS BIGINT)",
                ".*DuckLake snapshot 999999 does not exist.*");
    }

    @Test
    public void testShowSessionListsDuckLakeAndHiveCommonProperties()
    {
        // DuckLakeSessionProperties contributes minimum_assigned_split_weight/cache_enabled, and
        // InternalDuckLakeConnectorFactory concatenates HiveCommonSessionProperties alongside it --
        // this proves both sets actually reach the "ducklake" catalog's session properties.
        MaterializedResult result = computeActual("SHOW SESSION");
        List<String> propertyNames = result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0))
                .collect(Collectors.toList());
        assertTrue(propertyNames.containsAll(List.of(
                "ducklake.minimum_assigned_split_weight",
                "ducklake.cache_enabled",
                "ducklake.parquet_batch_read_optimization_enabled",
                "ducklake.parquet_max_read_block_size")));
    }

    @Test
    public void testCreateSchemaFails()
    {
        assertQueryFails("CREATE SCHEMA ducklake.new_schema", ".*read-only in this version.*");
    }

    @Test
    public void testCreateTableFails()
    {
        assertQueryFails("CREATE TABLE tpch.t (x bigint)", ".*read-only in this version.*");
    }

    @Test
    public void testCreateTableAsSelectFails()
    {
        assertQueryFails("CREATE TABLE tpch.t AS SELECT 1 x", ".*read-only in this version.*");
    }

    @Test
    public void testInsertFails()
    {
        assertQueryFails("INSERT INTO tpch.nation SELECT * FROM tpch.nation", ".*read-only in this version.*");
    }

    @Test
    public void testDeleteFails()
    {
        assertQueryFails("DELETE FROM tpch.nation WHERE n_nationkey = 1", ".*read-only in this version.*");
    }

    @Test
    public void testDropTableFails()
    {
        assertQueryFails("DROP TABLE tpch.nation", ".*read-only in this version.*");
    }

    @Test
    public void testAddColumnFails()
    {
        assertQueryFails("ALTER TABLE tpch.nation ADD COLUMN c bigint", ".*read-only in this version.*");
    }

    @Test
    public void testRenameTableFails()
    {
        assertQueryFails("ALTER TABLE tpch.nation RENAME TO nation2", ".*read-only in this version.*");
    }

    @Test
    public void testCreateViewFails()
    {
        assertQueryFails("CREATE VIEW tpch.v AS SELECT 1 x", ".*read-only in this version.*");
    }

    @Test
    public void testAnalyzeFails()
    {
        assertQueryFails("ANALYZE tpch.nation", ".*read-only in this version.*");
    }
}
