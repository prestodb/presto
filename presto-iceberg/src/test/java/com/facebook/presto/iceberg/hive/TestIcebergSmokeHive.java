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
package com.facebook.presto.iceberg.hive;

import com.facebook.presto.FullConnectorSession;
import com.facebook.presto.Session;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.iceberg.IcebergCatalogName;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergDistributedSmokeTestBase;
import com.facebook.presto.iceberg.IcebergHiveTableOperationsConfig;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.iceberg.ManifestFileCache;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types.IntegerType;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;

import static com.facebook.presto.hive.metastore.InMemoryCachingHiveMetastore.memoizeMetastore;
import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestIcebergSmokeHive
        extends IcebergDistributedSmokeTestBase
{
    public TestIcebergSmokeHive()
    {
        super(HIVE);
    }

    @Override
    protected String getLocation(String schema, String table)
    {
        Path dataDirectory = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getDataDirectory();
        File tempLocation = getIcebergDataDirectoryPath(dataDirectory, HIVE.name(), new IcebergConfig().getFileFormat(), false).toFile();
        return format("%s%s/%s", tempLocation.toURI(), schema, table);
    }

    protected ExtendedHiveMetastore getFileHiveMetastore()
    {
        IcebergFileHiveMetastore fileHiveMetastore = new IcebergFileHiveMetastore(getHdfsEnvironment(),
                getCatalogDirectory().toString(),
                "test");
        return memoizeMetastore(fileHiveMetastore, false, 1000, 0);
    }

    @Override
    protected Table getIcebergTable(ConnectorSession session, String schema, String tableName)
    {
        String defaultCatalog = ((FullConnectorSession) session).getSession().getCatalog().get();
        return IcebergUtil.getHiveIcebergTable(getFileHiveMetastore(),
                getHdfsEnvironment(),
                new IcebergHiveTableOperationsConfig(),
                new ManifestFileCache(CacheBuilder.newBuilder().build(), false, 0, 1024),
                session,
                new IcebergCatalogName(defaultCatalog),
                SchemaTableName.valueOf(schema + "." + tableName));
    }

    @Test
    public void testShowCreateSchema()
    {
        String createSchemaSql = "CREATE SCHEMA show_create_iceberg_schema";
        assertUpdate(createSchemaSql);
        String expectedShowCreateSchema = "CREATE SCHEMA show_create_iceberg_schema\n" +
                "WITH (\n" +
                "   location = '.*show_create_iceberg_schema'\n" +
                ")";

        MaterializedResult actualResult = computeActual("SHOW CREATE SCHEMA show_create_iceberg_schema");
        assertThat(getOnlyElement(actualResult.getOnlyColumnAsSet()).toString().matches(expectedShowCreateSchema));

        assertQueryFails(format("SHOW CREATE SCHEMA %s.%s", getSession().getCatalog().get(), ""), ".*mismatched input '.'. Expecting: <EOF>");
        assertQueryFails(format("SHOW CREATE SCHEMA %s.%s.%s", getSession().getCatalog().get(), "show_create_iceberg_schema", "tabletest"), ".*Too many parts in schema name: iceberg.show_create_iceberg_schema.tabletest");
        assertQueryFails(format("SHOW CREATE SCHEMA %s", "schema_not_exist"), ".*Schema 'iceberg.schema_not_exist' does not exist");
        assertUpdate("DROP SCHEMA show_create_iceberg_schema");
    }

    /**
     * {@code ADD COLUMN ... FIRST|AFTER} is resolved in the engine and applied by
     * {@code IcebergAbstractMetadata} through {@code UpdateSchema}, so it behaves identically for every
     * Iceberg catalog. The catalog-independent schema-evolution and complex-type cases therefore live
     * here rather than in {@link IcebergDistributedSmokeTestBase}, which runs against every catalog.
     */
    @Test
    public void testAddColumnWithPositionAndComplexTypes()
    {
        Session session = getSession();
        assertUpdate(session, "CREATE TABLE test_add_column_complex (second INTEGER)");

        assertUpdate(session, "ALTER TABLE test_add_column_complex ADD COLUMN r ROW(x INTEGER, y VARCHAR) FIRST");
        assertUpdate(session, "ALTER TABLE test_add_column_complex ADD COLUMN a ARRAY(INTEGER) AFTER second");
        assertUpdate(session, "ALTER TABLE test_add_column_complex ADD COLUMN m MAP(VARCHAR, INTEGER) AFTER a");

        assertEquals(columnNames("test_add_column_complex"), ImmutableList.of("r", "second", "a", "m"));

        assertUpdate(
                session,
                "INSERT INTO test_add_column_complex VALUES (CAST(ROW(7, 'seven') AS ROW(x INTEGER, y VARCHAR)), 2, ARRAY[8, 9], MAP(ARRAY['ten'], ARRAY[10]))",
                1);

        // Asserted against the materialized row rather than with assertQuery, because the expected side of
        // assertQuery runs on H2, which cannot represent map, array or row types
        MaterializedRow row = getOnlyElement(computeActual(session, "SELECT * FROM test_add_column_complex").getMaterializedRows());
        assertEquals(row.getField(0), ImmutableList.of(7, "seven"));
        assertEquals(row.getField(1), 2);
        assertEquals(row.getField(2), ImmutableList.of(8, 9));
        assertEquals(row.getField(3), ImmutableMap.of("ten", 10));

        // Reading the complex columns by name must agree with the positional read above
        assertQuery(session, "SELECT r.x, r.y, second, a[1], a[2], m['ten'] FROM test_add_column_complex", "VALUES (7, 'seven', 2, 8, 9, 10)");

        dropTable(session, "test_add_column_complex");
    }

    @Test
    public void testAddColumnWithPositionOnPartitionedTable()
    {
        Session session = getSession();
        assertUpdate(session, "CREATE TABLE test_add_column_partitioned (a INTEGER, part VARCHAR) WITH (partitioning = ARRAY['part'])");

        assertUpdate(session, "ALTER TABLE test_add_column_partitioned ADD COLUMN first INTEGER FIRST");
        assertEquals(columnNames("test_add_column_partitioned"), ImmutableList.of("first", "a", "part"));

        // Partitioning still works after the reposition, and each partition holds distinct values
        assertUpdate(session, "INSERT INTO test_add_column_partitioned VALUES (1, 2, 'p1'), (10, 20, 'p2')", 2);
        assertQuery(session, "SELECT * FROM test_add_column_partitioned WHERE part = 'p1'", "VALUES (1, 2, 'p1')");
        assertQuery(session, "SELECT first, a FROM test_add_column_partitioned WHERE part = 'p2'", "VALUES (10, 20)");
        assertQueryOrdered(session, "SELECT * FROM test_add_column_partitioned ORDER BY part", "VALUES (1, 2, 'p1'), (10, 20, 'p2')");

        dropTable(session, "test_add_column_partitioned");
    }

    @Test
    public void testAddColumnWithPositionToTableWithExistingRows()
    {
        Session session = getSession();
        assertUpdate(session, "CREATE TABLE test_add_column_existing_rows AS SELECT 2 AS second, 4 AS fourth", 1);

        assertUpdate(session, "ALTER TABLE test_add_column_existing_rows ADD COLUMN first INTEGER FIRST");
        assertEquals(columnNames("test_add_column_existing_rows"), ImmutableList.of("first", "second", "fourth"));

        // The pre-existing row has no value for the new column; the other two columns keep their own values,
        // asserted by name so the absent value cannot be confused with a shifted column
        assertQuery(session, "SELECT second, fourth FROM test_add_column_existing_rows WHERE first IS NULL", "VALUES (2, 4)");
        assertQuery(session, "SELECT count(*) FROM test_add_column_existing_rows", "VALUES 1");

        // A row written after the reposition round-trips completely
        assertUpdate(session, "INSERT INTO test_add_column_existing_rows VALUES (1, 20, 40)", 1);
        assertQuery(session, "SELECT * FROM test_add_column_existing_rows WHERE first = 1", "VALUES (1, 20, 40)");

        dropTable(session, "test_add_column_existing_rows");
    }

    @Test
    public void testAddColumnInMiddleThenDropIt()
    {
        Session session = getSession();
        assertUpdate(session, "CREATE TABLE test_add_column_then_drop (a INTEGER, c INTEGER)");

        assertUpdate(session, "ALTER TABLE test_add_column_then_drop ADD COLUMN b INTEGER AFTER a");
        assertEquals(columnNames("test_add_column_then_drop"), ImmutableList.of("a", "b", "c"));
        assertUpdate(session, "INSERT INTO test_add_column_then_drop VALUES (1, 2, 3)", 1);

        assertUpdate(session, "ALTER TABLE test_add_column_then_drop DROP COLUMN b");
        assertEquals(columnNames("test_add_column_then_drop"), ImmutableList.of("a", "c"));

        // The row written while the middle column existed is still readable, and the surviving columns
        // did not shift into the hole left by the dropped one
        assertQuery(session, "SELECT * FROM test_add_column_then_drop", "VALUES (1, 3)");
        assertQuery(session, "SELECT a, c FROM test_add_column_then_drop", "VALUES (1, 3)");
        assertQueryFails("SELECT b FROM test_add_column_then_drop", ".*Column 'b' cannot be resolved");

        // And the table still accepts writes with the narrowed schema
        assertUpdate(session, "INSERT INTO test_add_column_then_drop VALUES (10, 30)", 1);
        assertQueryOrdered(session, "SELECT * FROM test_add_column_then_drop ORDER BY a", "VALUES (1, 3), (10, 30)");

        dropTable(session, "test_add_column_then_drop");
    }

    @Test
    public void testAddColumnInMiddleAfterDroppingColumn()
    {
        Session session = getSession();
        assertUpdate(session, "CREATE TABLE test_add_column_after_drop (a INTEGER, b INTEGER, c INTEGER)");
        assertUpdate(session, "INSERT INTO test_add_column_after_drop VALUES (1, 2, 3)", 1);

        assertUpdate(session, "ALTER TABLE test_add_column_after_drop DROP COLUMN b");
        assertEquals(columnNames("test_add_column_after_drop"), ImmutableList.of("a", "c"));

        // The dropped name is no longer a valid position target
        assertQueryFails(
                "ALTER TABLE test_add_column_after_drop ADD COLUMN d INTEGER AFTER b",
                ".*Column 'b' does not exist");

        // Adding a differently named column in the vacated middle position, and writing to it, still works
        assertUpdate(session, "ALTER TABLE test_add_column_after_drop ADD COLUMN b2 INTEGER AFTER a");
        assertEquals(columnNames("test_add_column_after_drop"), ImmutableList.of("a", "b2", "c"));
        assertUpdate(session, "INSERT INTO test_add_column_after_drop VALUES (10, 20, 30)", 1);
        assertQuery(session, "SELECT * FROM test_add_column_after_drop WHERE a = 10", "VALUES (10, 20, 30)");
        assertQuery(session, "SELECT a, c FROM test_add_column_after_drop WHERE b2 IS NULL", "VALUES (1, 3)");

        // Reusing the dropped name is allowed, because Iceberg assigns the new column a fresh field id
        assertUpdate(session, "ALTER TABLE test_add_column_after_drop ADD COLUMN b INTEGER AFTER b2");
        assertEquals(columnNames("test_add_column_after_drop"), ImmutableList.of("a", "b2", "b", "c"));
        assertUpdate(session, "INSERT INTO test_add_column_after_drop VALUES (100, 200, 250, 300)", 1);
        assertQuery(session, "SELECT * FROM test_add_column_after_drop WHERE a = 100", "VALUES (100, 200, 250, 300)");
        // The re-added column reads as absent for the rows written before it existed, rather than
        // picking up the old column's values
        assertQueryOrdered(session, "SELECT a FROM test_add_column_after_drop WHERE b IS NULL ORDER BY a", "VALUES 1, 10");

        dropTable(session, "test_add_column_after_drop");
    }

    @Test
    public void testAddColumnAfterNewlyAddedColumn()
    {
        Session session = getSession();
        assertUpdate(session, "CREATE TABLE test_add_column_chain (a INTEGER, e INTEGER)");

        // Each added column becomes a valid AFTER target for the next statement
        assertUpdate(session, "ALTER TABLE test_add_column_chain ADD COLUMN b INTEGER AFTER a");
        assertUpdate(session, "ALTER TABLE test_add_column_chain ADD COLUMN c INTEGER AFTER b");
        assertUpdate(session, "ALTER TABLE test_add_column_chain ADD COLUMN d INTEGER AFTER c");

        assertEquals(columnNames("test_add_column_chain"), ImmutableList.of("a", "b", "c", "d", "e"));

        assertUpdate(session, "INSERT INTO test_add_column_chain VALUES (1, 2, 3, 4, 5)", 1);
        assertQuery(session, "SELECT * FROM test_add_column_chain", "VALUES (1, 2, 3, 4, 5)");
        assertQuery(session, "SELECT a, b, c, d, e FROM test_add_column_chain", "VALUES (1, 2, 3, 4, 5)");

        dropTable(session, "test_add_column_chain");
    }

    @Test
    public void testAddColumnFirstRepeatedly()
    {
        Session session = getSession();
        assertUpdate(session, "CREATE TABLE test_add_column_repeat_first (c INTEGER)");

        // The second FIRST must land ahead of the column added by the first one
        assertUpdate(session, "ALTER TABLE test_add_column_repeat_first ADD COLUMN b INTEGER FIRST");
        assertEquals(columnNames("test_add_column_repeat_first"), ImmutableList.of("b", "c"));

        assertUpdate(session, "ALTER TABLE test_add_column_repeat_first ADD COLUMN a INTEGER FIRST");
        assertEquals(columnNames("test_add_column_repeat_first"), ImmutableList.of("a", "b", "c"));

        assertUpdate(session, "INSERT INTO test_add_column_repeat_first VALUES (1, 2, 3)", 1);
        assertQuery(session, "SELECT * FROM test_add_column_repeat_first", "VALUES (1, 2, 3)");
        // The by-name projection is what pins the order: it is compared against the same tuple the
        // positional INSERT used, so it fails unless each name holds the value written at its position
        assertQuery(session, "SELECT a, b, c FROM test_add_column_repeat_first", "VALUES (1, 2, 3)");

        dropTable(session, "test_add_column_repeat_first");
    }

    @Test
    public void testAddColumnPositionTargetIsCaseInsensitive()
    {
        Session session = getSession();
        assertUpdate(session, "CREATE TABLE test_add_column_target_case (a INTEGER, c INTEGER)");

        // The AFTER target is normalized the same way any other column reference is
        assertUpdate(session, "ALTER TABLE test_add_column_target_case ADD COLUMN b INTEGER AFTER A");
        assertEquals(columnNames("test_add_column_target_case"), ImmutableList.of("a", "b", "c"));

        assertUpdate(session, "ALTER TABLE test_add_column_target_case ADD COLUMN b2 INTEGER AFTER \"b\"");
        assertEquals(columnNames("test_add_column_target_case"), ImmutableList.of("a", "b", "b2", "c"));

        assertUpdate(session, "INSERT INTO test_add_column_target_case VALUES (1, 2, 3, 4)", 1);
        assertQuery(session, "SELECT * FROM test_add_column_target_case", "VALUES (1, 2, 3, 4)");
        assertQuery(session, "SELECT a, b, b2, c FROM test_add_column_target_case", "VALUES (1, 2, 3, 4)");

        dropTable(session, "test_add_column_target_case");
    }

    /**
     * A table created by another engine can hold a column whose Iceberg name is not lowercase. The engine
     * lowercases the {@code AFTER} target before handing it to the connector, so the connector has to resolve
     * it against the Iceberg schema case-insensitively; a case-sensitive lookup would reject every spelling
     * the user could type and make the clause unusable on such a table.
     */
    @Test
    public void testAddColumnAfterCasePreservedColumn()
    {
        Session session = getSession();
        String tableName = "test_add_column_after_mixed_case";
        assertUpdate(session, "CREATE TABLE " + tableName + " (a INTEGER, c INTEGER)");

        // Renaming through the Iceberg API preserves the case, which CREATE TABLE through Presto cannot do
        getIcebergTable(session, tableName).updateSchema().renameColumn("a", "MixedCase").commit();
        assertEquals(columnNames(tableName), ImmutableList.of("mixedcase", "c"));

        // Every spelling of the target normalizes to the same name, and all of them have to resolve
        assertUpdate(session, "ALTER TABLE " + tableName + " ADD COLUMN b INTEGER AFTER \"MixedCase\"");
        assertEquals(columnNames(tableName), ImmutableList.of("mixedcase", "b", "c"));

        assertUpdate(session, "ALTER TABLE " + tableName + " ADD COLUMN b2 INTEGER AFTER mixedcase");
        assertEquals(columnNames(tableName), ImmutableList.of("mixedcase", "b2", "b", "c"));

        assertUpdate(session, "INSERT INTO " + tableName + " VALUES (1, 2, 3, 4)", 1);
        assertQuery(session, "SELECT * FROM " + tableName, "VALUES (1, 2, 3, 4)");
        assertQuery(session, "SELECT mixedcase, b2, b, c FROM " + tableName, "VALUES (1, 2, 3, 4)");

        dropTable(session, tableName);
    }

    @Test
    public void testAddColumnWithPositionAndPartitioning()
    {
        Session session = getSession();
        String tableName = "test_add_column_position_partitioning";
        assertUpdate(session, "CREATE TABLE " + tableName + " (a INTEGER, c INTEGER)");

        // The connector commits the schema, including the move, before committing the new partition field, so
        // the two have to agree on which column was added
        assertUpdate(session, "ALTER TABLE " + tableName + " ADD COLUMN b INTEGER WITH (partitioning = 'identity') AFTER a");
        assertEquals(columnNames(tableName), ImmutableList.of("a", "b", "c"));

        assertUpdate(session, "INSERT INTO " + tableName + " VALUES (1, 2, 3), (10, 20, 30)", 2);
        assertQuery(session, "SELECT * FROM " + tableName + " WHERE b = 2", "VALUES (1, 2, 3)");
        assertQueryOrdered(session, "SELECT * FROM " + tableName + " ORDER BY a", "VALUES (1, 2, 3), (10, 20, 30)");

        // The new column is the partition field, which confirms the partition spec followed the repositioned column
        assertQueryOrdered(session, "SELECT b FROM \"" + tableName + "$partitions\" ORDER BY b", "VALUES 2, 20");

        dropTable(session, tableName);
    }

    /**
     * A struct can legitimately hold two sibling fields whose names differ only by case, which is not a
     * collision for Iceberg because they have distinct field ids. Resolving the {@code AFTER} target through
     * {@link org.apache.iceberg.Schema#caseInsensitiveFindField} would build a lower-case index over every
     * field in the table and reject such a schema outright, so only the top-level columns are scanned.
     */
    @Test
    public void testAddColumnAfterOnTableWithNestedCaseCollision()
    {
        Session session = getSession();
        String tableName = "test_add_column_nested_case_collision";
        assertUpdate(session, "CREATE TABLE " + tableName + " (a INTEGER, s ROW(x INTEGER), c INTEGER)");

        // Presto lowercases field names, so the colliding sibling has to be added through the Iceberg API
        getIcebergTable(session, tableName).updateSchema().addColumn("s", "X", IntegerType.get()).commit();

        assertUpdate(session, "ALTER TABLE " + tableName + " ADD COLUMN b INTEGER AFTER a");
        assertEquals(columnNames(tableName), ImmutableList.of("a", "b", "s", "c"));

        // A nested field is not a valid target: the engine validates the target against the table's columns,
        // so the connector never sees a dotted name and cannot move the column after a nested leaf
        assertQueryFails(session, "ALTER TABLE " + tableName + " ADD COLUMN d INTEGER AFTER \"s.x\"", ".*Column 's.x' does not exist");
        assertEquals(columnNames(tableName), ImmutableList.of("a", "b", "s", "c"));

        // Writing is not asserted here: an INSERT into a table with such a struct fails with
        // "Duplicate field: x", which was confirmed to happen with no ADD COLUMN involved at all and so is
        // unrelated to this feature
        assertQuery(session, "SELECT count(*) FROM " + tableName, "VALUES 0");

        dropTable(session, tableName);
    }

    /**
     * {@code ALTER COLUMN ... FIRST|AFTER} is resolved in the engine and applied by
     * {@code IcebergAbstractMetadata} through {@code UpdateSchema}, so it behaves identically for every
     * Iceberg catalog. {@link IcebergDistributedSmokeTestBase} therefore keeps only the tests covering the
     * positions themselves and their error paths, and the cases below live here rather than being run once
     * per catalog.
     */
    @Test
    public void testSetColumnPositionWithComplexTypes()
    {
        Session session = getSession();
        String tableName = "test_set_column_position_complex";
        assertUpdate(session, "CREATE TABLE " + tableName + " (r ROW(x INTEGER, y VARCHAR), a ARRAY(INTEGER), m MAP(VARCHAR, INTEGER), i INTEGER)");
        assertUpdate(
                session,
                "INSERT INTO " + tableName + " VALUES (CAST(ROW(7, 'seven') AS ROW(x INTEGER, y VARCHAR)), ARRAY[8, 9], MAP(ARRAY['ten'], ARRAY[10]), 11)",
                1);

        // A column of a complex type moves as a whole, and so does a column moved past one
        assertUpdate(session, "ALTER TABLE " + tableName + " ALTER COLUMN i FIRST");
        assertEquals(columnNames(tableName), ImmutableList.of("i", "r", "a", "m"));

        assertUpdate(session, "ALTER TABLE " + tableName + " ALTER COLUMN m AFTER i");
        assertEquals(columnNames(tableName), ImmutableList.of("i", "m", "r", "a"));

        assertUpdate(session, "ALTER TABLE " + tableName + " ALTER COLUMN r AFTER a");
        assertEquals(columnNames(tableName), ImmutableList.of("i", "m", "a", "r"));

        // Asserted against the materialized row rather than with assertQuery, because the expected side of
        // assertQuery runs on H2, which cannot represent map, array or row types
        MaterializedRow row = getOnlyElement(computeActual(session, "SELECT * FROM " + tableName).getMaterializedRows());
        assertEquals(row.getField(0), 11);
        assertEquals(row.getField(1), ImmutableMap.of("ten", 10));
        assertEquals(row.getField(2), ImmutableList.of(8, 9));
        assertEquals(row.getField(3), ImmutableList.of(7, "seven"));

        // Reading the complex columns by name, including a field of the moved struct, must agree with the
        // positional read above
        assertQuery(session, "SELECT i, m['ten'], a[1], a[2], r.x, r.y FROM " + tableName, "VALUES (11, 10, 8, 9, 7, 'seven')");

        // A nested field is neither movable nor a valid target, because the engine resolves both names
        // against the table's columns and so never passes a dotted name to the connector
        assertQueryFails(session, "ALTER TABLE " + tableName + " ALTER COLUMN \"r.x\" FIRST", ".*Column 'r.x' does not exist");
        assertQueryFails(session, "ALTER TABLE " + tableName + " ALTER COLUMN i AFTER \"r.x\"", ".*Column 'r.x' does not exist");
        assertEquals(columnNames(tableName), ImmutableList.of("i", "m", "a", "r"));

        dropTable(session, tableName);
    }

    @Test
    public void testSetColumnPositionOnPartitionedTable()
    {
        Session session = getSession();
        String tableName = "test_set_column_position_partitioned";
        assertUpdate(session, "CREATE TABLE " + tableName + " (a INTEGER, b INTEGER, part VARCHAR) WITH (partitioning = ARRAY['part'])");
        assertUpdate(session, "INSERT INTO " + tableName + " VALUES (1, 2, 'p1'), (10, 20, 'p2')", 2);

        // The partition column itself can be moved; reordering is metadata-only and leaves the partition spec alone
        assertUpdate(session, "ALTER TABLE " + tableName + " ALTER COLUMN part FIRST");
        assertEquals(columnNames(tableName), ImmutableList.of("part", "a", "b"));
        assertQuery(session, "SELECT count(*) FROM \"" + tableName + "$partitions\"", "VALUES 2");
        assertQuery(session, "SELECT * FROM " + tableName + " WHERE part = 'p1'", "VALUES ('p1', 1, 2)");
        assertQuery(session, "SELECT a, b FROM " + tableName + " WHERE part = 'p2'", "VALUES (10, 20)");

        // Moving a non-partition column of a partitioned table works the same way
        assertUpdate(session, "ALTER TABLE " + tableName + " ALTER COLUMN b AFTER part");
        assertEquals(columnNames(tableName), ImmutableList.of("part", "b", "a"));
        assertQuery(session, "SELECT * FROM " + tableName + " WHERE part = 'p1'", "VALUES ('p1', 2, 1)");

        // Writes after the moves still land in the right partition, and partition pruning still finds them
        assertUpdate(session, "INSERT INTO " + tableName + " VALUES ('p3', 200, 100)", 1);
        assertQuery(session, "SELECT count(*) FROM \"" + tableName + "$partitions\"", "VALUES 3");
        assertQuery(session, "SELECT a, b FROM " + tableName + " WHERE part = 'p3'", "VALUES (100, 200)");
        assertQueryOrdered(session, "SELECT * FROM " + tableName + " ORDER BY part", "VALUES ('p1', 2, 1), ('p2', 20, 10), ('p3', 200, 100)");

        dropTable(session, tableName);
    }

    private Table getIcebergTable(Session session, String tableName)
    {
        CatalogManager catalogManager = getDistributedQueryRunner().getCoordinator().getCatalogManager();
        ConnectorId connectorId = catalogManager.getCatalog(ICEBERG_CATALOG).get().getConnectorId();
        return getIcebergTable(session.toConnectorSession(connectorId), session.getSchema().get(), tableName);
    }
}
