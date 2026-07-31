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
package com.facebook.presto.iceberg.rest;

import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergNativeCatalogFactory;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.FileFormat.ORC;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.restConnectorProperties;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Verifies the default (case-insensitive) behaviour of the Iceberg REST connector, i.e.
 * {@code case-sensitive-name-matching} is NOT set (defaults to {@code false}).
 *
 * <p>In this mode {@code normalizeIdentifier} lowercases every identifier it receives, so:
 * <ul>
 *   <li>Mixed-case table/column names are stored and retrieved in lowercase.</li>
 *   <li>Double-quoted identifiers with mixed case are also lowercased — quoting only helps
 *       with special characters or reserved keywords, not with preserving case.</li>
 * </ul>
 */
@Test(singleThreaded = true)
public class TestIcebergRestCaseInsensitiveNameMatching
        extends IcebergRestNameMatchingTestBase
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // No case-sensitive-name-matching property — defaults to false (case-insensitive).
        return IcebergQueryRunner.builder()
                .setCatalogType(REST)
                .setFormat(ORC)
                .setExtraConnectorProperties(ImmutableMap.copyOf(restConnectorProperties(serverUri)))
                .setDataDirectory(Optional.of(warehouseLocation.toPath()))
                .setCreateTpchTables(false)
                .build()
                .getQueryRunner();
    }

    @Override
    protected IcebergNativeCatalogFactory getCatalogFactory()
    {
        IcebergConfig icebergConfig = new IcebergConfig()
                .setCatalogType(REST)
                .setCatalogWarehouse(warehouseLocation.getAbsolutePath());
        return buildCatalogFactory(icebergConfig);
    }

    @Test
    public void testCreateTableMixedCase()
    {
        // normalizeIdentifier lowercases 'MixedCaseTable' → 'mixedcasetable'.
        // All case variants resolve to the same stored name.
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE MixedCaseTable (id bigint, val varchar)");

            assertQuerySucceeds(testSession(), "SELECT * FROM MixedCaseTable");
            assertQuerySucceeds(testSession(), "SELECT * FROM mixedcasetable");
            assertQuerySucceeds(testSession(), "SELECT * FROM MIXEDCASETABLE");

            // Creating with a different case input refers to the same table — already exists.
            assertQueryFails(testSession(), "CREATE TABLE mixedcasetable (id bigint, val varchar)", ".*Table.*already exists.*");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS MixedCaseTable");
        }
    }

    @Test
    public void testCreateTableAsMixedCase()
    {
        // 'MyTable' is normalised to 'mytable'; all case variants resolve to the same table.
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE MyTable AS SELECT 1 AS id, 'hello' AS name");

            assertQuery(testSession(), "SELECT id, name FROM MyTable", "VALUES (1, 'hello')");
            assertQuery(testSession(), "SELECT id, name FROM mytable", "VALUES (1, 'hello')");
            assertQuery(testSession(), "SELECT id, name FROM MYTABLE", "VALUES (1, 'hello')");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS MyTable");
        }
    }

    @Test
    public void testMixedCaseColumns()
    {
        // normalizeIdentifier lowercases every column name regardless of quoting.

        // Quoted: 'FirstName' → 'firstname'. Any case variant retrieves the same column.
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE coltestquoted (\"FirstName\" varchar, \"lastName\" varchar)");
            assertQuerySucceeds(testSession(), "INSERT INTO coltestquoted VALUES ('Alice', 'Smith')");
            assertQuery(testSession(), "SELECT firstname, lastname FROM coltestquoted", "VALUES ('Alice', 'Smith')");
            assertQuery(testSession(), "SELECT \"FirstName\", \"lastName\" FROM coltestquoted", "VALUES ('Alice', 'Smith')");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS coltestquoted");
        }

        // Unquoted: 'FirstName' → 'firstname'.
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE coltestunquoted (FirstName varchar, lastName varchar)");
            assertQuerySucceeds(testSession(), "INSERT INTO coltestunquoted VALUES ('Bob', 'Jones')");
            assertQuery(testSession(), "SELECT firstname, lastname FROM coltestunquoted", "VALUES ('Bob', 'Jones')");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS coltestunquoted");
        }
    }

    @Test
    public void testMixedCasePartitionColumn()
    {
        // normalizeIdentifier lowercases the resolved column name; column definition and
        // partition array reference both resolve to the same lowercase name.

        // Quoted in array: '"RegionId"' → 'RegionId' → 'regionid'. Column also → 'regionid'. ✓
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE partTestQuoted (\"RegionId\" bigint, name varchar) WITH (partitioning = ARRAY['\"RegionId\"'])");
            assertQuerySucceeds(testSession(), "INSERT INTO partTestQuoted VALUES (1, 'north'), (2, 'south')");
            assertQuery(testSession(), "SELECT regionid, name FROM partTestQuoted ORDER BY regionid", "VALUES (1, 'north'), (2, 'south')");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS partTestQuoted");
        }

        // Unquoted in array: 'RegionId' → 'regionid'. ✓
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE partTestUnquoted (RegionId bigint, name varchar) WITH (partitioning = ARRAY['RegionId'])");
            assertQuerySucceeds(testSession(), "INSERT INTO partTestUnquoted VALUES (1, 'north'), (2, 'south')");
            assertQuery(testSession(), "SELECT regionid, name FROM partTestUnquoted ORDER BY regionid", "VALUES (1, 'north'), (2, 'south')");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS partTestUnquoted");
        }
    }

    @Test
    public void testShowColumnsCaseInsensitive()
    {
        // All column names are lowercased regardless of how they were written.
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE showcols (\"MyCol\" bigint, \"anotherCol\" varchar)");
            String result = getQueryRunner().execute(testSession(), "SHOW COLUMNS FROM showcols").toString();
            assertTrue(result.contains("mycol"), "Expected lowercase 'mycol' in SHOW COLUMNS output");
            assertTrue(result.contains("anothercol"), "Expected lowercase 'anothercol' in SHOW COLUMNS output");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS showcols");
        }
    }

    @Test
    public void testNormalizeIdentifierLowercases()
    {
        String normalized = normalizeIdentifier("MyTable", ICEBERG_CATALOG);
        assertTrue(normalized.equals("mytable"), "Expected normalizeIdentifier to lowercase 'MyTable', got: " + normalized);

        String normalizedLower = normalizeIdentifier("mytable", ICEBERG_CATALOG);
        assertTrue(normalizedLower.equals("mytable"), "Expected normalizeIdentifier to return 'mytable' unchanged, got: " + normalizedLower);

        String normalizedUpper = normalizeIdentifier("MYTABLE", ICEBERG_CATALOG);
        assertTrue(normalizedUpper.equals("mytable"), "Expected normalizeIdentifier to lowercase 'MYTABLE', got: " + normalizedUpper);

        assertFalse(normalized.equals("MyTable"), "normalizeIdentifier must not preserve mixed case in case-insensitive mode");
        assertFalse(normalizedUpper.equals("MYTABLE"), "normalizeIdentifier must not leave uppercase unchanged in case-insensitive mode");
    }

    @Test
    public void testRewriteDataFilesWithSortedByMixedCaseColumn()
    {
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE rewrite_sorted_ci (Id bigint, Name varchar, VAL integer)");
            assertQuerySucceeds(testSession(), "INSERT INTO rewrite_sorted_ci VALUES (3, 'c', 30), (1, 'a', 10)");
            assertQuerySucceeds(testSession(), "INSERT INTO rewrite_sorted_ci VALUES (2, 'b', 20), (4, 'd', 40)");

            // sorted_by 'Id' → normalizeIdentifier → 'id' → matches stored column 'id'
            assertQuerySucceeds(testSession(), format(
                    "CALL iceberg.system.rewrite_data_files(schema => '%s', table_name => 'rewrite_sorted_ci', " +
                            "sorted_by => ARRAY['Id'])",
                    SCHEMA));

            // Columns are stored and retrieved as lowercase
            assertQuery(testSession(), "SELECT id, name, val FROM rewrite_sorted_ci ORDER BY id", "VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40)");

            List<Types.NestedField> columns = getIcebergTable(SCHEMA, "rewrite_sorted_ci").schema().columns();
            assertEquals(columns.get(0).name(), "id", "Column 0 must be stored as lowercase 'id'");
            assertEquals(columns.get(1).name(), "name", "Column 1 must be stored as lowercase 'name'");
            assertEquals(columns.get(2).name(), "val", "Column 2 must be stored as lowercase 'val'");
            assertFalse(columns.get(0).name().equals("Id"), "Column 0 must NOT be stored as mixed-case 'Id'");
            assertFalse(columns.get(1).name().equals("Name"), "Column 1 must NOT be stored as mixed-case 'Name'");
            assertFalse(columns.get(2).name().equals("VAL"), "Column 2 must NOT be stored as mixed-case 'VAL'");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS rewrite_sorted_ci");
        }
    }
}
