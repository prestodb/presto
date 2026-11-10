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
package com.facebook.presto.nativetests.iceberg;

import org.testng.annotations.Test;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestNativeIcebergV3Ddl
        extends AbstractNativeIcebergV3Test
{
    protected AbstractTestNativeIcebergV3Ddl(String storageFormat)
    {
        super(storageFormat);
    }

    @Override
    protected void createTables()
    {
        super.createTables();
        createTpchTables("nation", "orders");
    }

    @Test
    public void testCreateInsertReadDrop()
    {
        String table = "ddl_basic";
        createV3Table(table, "id integer, name varchar");
        assertEquals(formatVersion(table), 3);
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);
        assertQuery("SELECT * FROM " + table, "VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        assertDataFilesUseFormat(table);
        assertUpdate("DROP TABLE " + table);
        assertQueryFails("SELECT * FROM " + table, ".*Table .*" + table + " does not exist");
    }

    @Test
    public void testCreateTableAsSelect()
    {
        String table = "ddl_ctas";
        createV3TableAs(table, "SELECT * FROM nation", 25);
        assertEquals(formatVersion(table), 3);
        assertQuery("SELECT * FROM " + table, "SELECT * FROM nation");
        assertQuery("SELECT name FROM " + table + " WHERE nationkey = 1", "VALUES 'ARGENTINA'");
        assertDataFilesUseFormat(table);
        dropTable(table);
    }

    @Test
    public void testCtasWithAggregation()
    {
        String table = "ddl_ctas_agg";
        createV3TableAs(table, "SELECT orderstatus, count(*) AS cnt, sum(totalprice) AS total FROM orders GROUP BY orderstatus", 3);
        assertQuery("SELECT orderstatus, cnt, total FROM " + table, "SELECT orderstatus, count(*), sum(totalprice) FROM orders GROUP BY orderstatus");
        dropTable(table);
    }

    @Test
    public void testPrimitiveTypes()
    {
        String table = "ddl_primitive_types";
        createV3Table(table, "b boolean, i integer, l bigint, r real, d double, dt date, ts timestamp, s varchar, bin varbinary, sd decimal(10, 2)");
        assertUpdate("INSERT INTO " + table + " VALUES "
                + "(true, 1, BIGINT '10000000000', REAL '1.5', 2.25, DATE '2024-01-01', TIMESTAMP '2024-01-01 12:34:56.789', 'abc', X'0102', DECIMAL '12.34'), "
                + "(false, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)", 2);
        assertQuery("SELECT b, i, l, r, d, dt, ts, s, to_hex(bin), sd FROM " + table);
        assertQuery("SELECT count(*) FROM " + table + " WHERE i IS NULL AND s IS NULL AND ts IS NULL AND bin IS NULL AND sd IS NULL", "VALUES 1");
        dropTable(table);
    }

    @Test
    public void testLongDecimal()
    {
        String table = "ddl_long_decimal";
        createV3Table(table, "id integer, d decimal(38, 10)");
        assertUpdate("INSERT INTO " + table + " VALUES (1, DECIMAL '1234567890.0123456789'), (2, NULL)", 2);
        assertQuery("SELECT id, d FROM " + table);
        assertQuery("SELECT sum(d) FROM " + table);
        dropTable(table);
    }

    @Test
    public void testNestedTypes()
    {
        String table = "ddl_nested_types";
        createV3Table(table, "id integer, arr array(integer), m map(varchar, bigint), r row(x integer, y varchar)");
        assertUpdate("INSERT INTO " + table + " VALUES "
                + "(1, ARRAY[1, 2, 3], MAP(ARRAY['a', 'b'], ARRAY[BIGINT '1', BIGINT '2']), ROW(10, 'ten')), "
                + "(2, ARRAY[], MAP(), NULL)", 2);
        assertQuery("SELECT id, cardinality(arr), element_at(m, 'b'), r.x, r.y FROM " + table,
                "VALUES (1, 3, BIGINT '2', 10, 'ten'), (2, 0, NULL, NULL, NULL)");
        assertQuery("SELECT id, x FROM " + table + " CROSS JOIN UNNEST(arr) AS t(x)", "VALUES (1, 1), (1, 2), (1, 3)");
        dropTable(table);
    }

    @Test
    public void testDescribeShowCreateAndNulls()
    {
        String table = "ddl_describe_nulls";
        createV3Table(table, "id integer, name varchar, amount double");
        assertQuery(format("SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'", SCHEMA, table),
                "VALUES 'id', 'name', 'amount'");
        assertTrue(((String) computeScalar("SHOW CREATE TABLE " + table)).contains("\"format-version\" = '3'"), "SHOW CREATE TABLE must report format-version 3");
        assertQuerySucceeds("DESCRIBE " + table);

        assertUpdate("INSERT INTO " + table + " VALUES (1, 'a', 100.0), (2, NULL, NULL)", 2);
        assertQuery("SELECT count(*) FROM " + table + " WHERE name IS NULL", "VALUES 1");
        assertQuery("SELECT id FROM " + table + " WHERE amount IS NULL", "VALUES 2");
        dropTable(table);
    }

    @Test
    public void testIdentityPartitioning()
    {
        String table = "ddl_part_identity";
        createV3Table(table, "id integer, region varchar", "partitioning = ARRAY['region']");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'us'), (2, 'us'), (3, 'eu'), (4, 'eu'), (5, 'apac')", 5);
        assertQuery("SELECT region, count(*) FROM " + table + " GROUP BY region", "VALUES ('apac', 1), ('eu', 2), ('us', 2)");
        assertQuery("SELECT id FROM " + table + " WHERE region = 'eu'", "VALUES 3, 4");
        assertEquals(scalarLong(format("SELECT count(*) FROM \"%s$partitions\"", table)), 3L);
        assertTrue(dataFileCount(table) >= 3, "expected at least one data file per partition");
        assertDataFilesUseFormat(table);
        dropTable(table);
    }

    @Test
    public void testPartitionTransforms()
    {
        assertPartitionTransform("ddl_part_bucket", "bucket(id, 4)", 4);
        assertPartitionTransform("ddl_part_truncate", "truncate(name, 1)", 3);
        assertPartitionTransform("ddl_part_year", "year(dt)", 2);
        assertPartitionTransform("ddl_part_month", "month(dt)", 3);
        assertPartitionTransform("ddl_part_day", "day(dt)", 4);
        assertPartitionTransform("ddl_part_hour", "hour(ts)", 5);
    }

    private void assertPartitionTransform(String table, String transform, long maxPartitions)
    {
        createV3Table(table, "id integer, name varchar, dt date, ts timestamp", format("partitioning = ARRAY['%s']", transform));
        assertUpdate("INSERT INTO " + table + " VALUES "
                + "(1, 'apple', DATE '2023-12-31', TIMESTAMP '2024-01-01 00:15:00'), "
                + "(2, 'avocado', DATE '2024-01-01', TIMESTAMP '2024-01-01 01:15:00'), "
                + "(3, 'banana', DATE '2024-01-02', TIMESTAMP '2024-01-01 02:15:00'), "
                + "(4, 'blueberry', DATE '2024-02-01', TIMESTAMP '2024-01-01 03:15:00'), "
                + "(5, 'cherry', DATE '2024-02-01', TIMESTAMP '2024-01-01 04:15:00')", 5);
        assertQuery("SELECT id, name FROM " + table, "VALUES (1, 'apple'), (2, 'avocado'), (3, 'banana'), (4, 'blueberry'), (5, 'cherry')");
        assertQuery("SELECT id FROM " + table + " WHERE dt >= DATE '2024-01-02'", "VALUES 3, 4, 5");
        assertQuery("SELECT id FROM " + table + " WHERE name LIKE 'b%'", "VALUES 3, 4");
        long partitions = scalarLong(format("SELECT count(*) FROM \"%s$partitions\"", table));
        assertTrue(partitions > 1 && partitions <= maxPartitions, format("%s: unexpected partition count %s", transform, partitions));
        dropTable(table);
    }

    @Test
    public void testPartitionEvolution()
    {
        String table = "ddl_part_evolve";
        createV3Table(table, "id integer, region varchar");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'us'), (2, 'eu')", 2);

        loadTable(table).updateSpec().addField("region").commit();
        assertUpdate("INSERT INTO " + table + " VALUES (3, 'us'), (4, 'apac')", 2);
        assertQuery("SELECT id FROM " + table + " WHERE region = 'us'", "VALUES 1, 3");
        assertQuery("SELECT count(DISTINCT partition_spec_id) FROM \"" + table + "$manifests\"", "VALUES 2");
        dropTable(table);
    }

    @Test
    public void testUpgradeV2ToV3()
    {
        String table = "ddl_upgrade";
        dropTable(table);
        assertUpdate(format("CREATE TABLE %s (id integer, v varchar) WITH (\"format-version\" = '2', format = '%s')", table, storageFormat()));
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);
        assertEquals(formatVersion(table), 2);

        assertQueryFails(format("ALTER TABLE %s SET PROPERTIES (\"format-version\" = '3')", table),
                "Updating property format-version is not supported currently");

        loadTable(table).updateProperties().set("format-version", "3").commit();
        assertEquals(formatVersion(table), 3);
        assertQuery("SELECT * FROM " + table, "VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        assertUpdate("INSERT INTO " + table + " VALUES (4, 'd')", 1);
        assertQuery("SELECT * FROM " + table, "VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')");
        dropTable(table);
    }

    // TODO: Enable once the Velox submodule includes facebookincubator/velox#19706. Before it, a native
    // UPDATE or MERGE that commits reports each updated row twice (once for its delete, once for its
    // re-insert), so the row-count check fails even though the table is correct.
    @Test(enabled = false)
    public void testRowLevelDmlOnV3()
    {
        // Each statement gets its own table, so its outcome cannot affect the others.
        String original = "VALUES (1, 'a'), (2, 'b'), (3, 'c')";
        String[] tables = {"ddl_v3_delete", "ddl_v3_update", "ddl_v3_merge"};
        for (String table : tables) {
            createV3Table(table, "id integer, v varchar");
            assertUpdate("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);
        }

        assertNativeRowLevelDml("DELETE FROM ddl_v3_delete WHERE id = 1", 1, "ddl_v3_delete", "VALUES (2, 'b'), (3, 'c')", original);
        assertNativeRowLevelDml("UPDATE ddl_v3_update SET v = 'z' WHERE id = 2", 1, "ddl_v3_update", "VALUES (1, 'a'), (2, 'z'), (3, 'c')", original);
        assertNativeRowLevelDml("MERGE INTO ddl_v3_merge t USING (VALUES (2, 'B')) s(id, v) ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v",
                1, "ddl_v3_merge", "VALUES (1, 'a'), (2, 'B'), (3, 'c')", original);

        for (String table : tables) {
            assertEquals(formatVersion(table), 3);
            dropTable(table);
        }
    }

    @Test
    public void testDeleteWholeTableAndPartitionOnV3()
    {
        // Deletes that remove whole data files are metadata-only and need no row-level delete files.
        String table = "ddl_v3_metadata_delete";
        createV3Table(table, "id integer, region varchar", "partitioning = ARRAY['region']");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'us'), (2, 'us'), (3, 'eu'), (4, 'apac')", 4);

        assertUpdate("DELETE FROM " + table + " WHERE region = 'us'", 2);
        assertQuery("SELECT id FROM " + table, "VALUES 3, 4");
        assertUpdate("DELETE FROM " + table, 2);
        assertQuery("SELECT count(*) FROM " + table, "VALUES 0");
        assertUpdate("INSERT INTO " + table + " VALUES (5, 'eu')", 1);
        assertQuery("SELECT id FROM " + table, "VALUES 5");
        dropTable(table);
    }

    @Test
    public void testUnsupportedFormatVersion()
    {
        assertQueryFails(format("CREATE TABLE ddl_v4 (id integer) WITH (\"format-version\" = '4', format = '%s')", storageFormat()),
                ".*format version 4 is not supported.*");
    }
}
