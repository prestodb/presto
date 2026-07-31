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
 * Verifies that when {@code case-sensitive-name-matching=true} identifiers (schema names,
 * table names, column names) are preserved as-is by {@code normalizeIdentifier}, enabling
 * case-sensitive access to REST catalogs that preserve identifier case.
 */
@Test(singleThreaded = true)
public class TestIcebergRestCaseSensitiveNameMatching
        extends IcebergRestNameMatchingTestBase
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(REST)
                .setFormat(ORC)
                .setExtraConnectorProperties(ImmutableMap.<String, String>builder()
                        .putAll(restConnectorProperties(serverUri))
                        .put("case-sensitive-name-matching", "true")
                        .build())
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
                .setCaseSensitiveNameMatching(true)
                .setCatalogWarehouse(warehouseLocation.getAbsolutePath());
        return buildCatalogFactory(icebergConfig);
    }

    @Test
    public void testCreateTableMixedCase()
    {
        // Both lower-case and upper-case variants can coexist as distinct tables in a
        // case-sensitive catalog — normalizeIdentifier preserves each name as written.
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE MixedCaseTable (id bigint, val varchar)");
            assertQuerySucceeds(testSession(), "CREATE TABLE mixedcasetable (id bigint, val varchar)");

            assertQuerySucceeds(testSession(), "SELECT * FROM MixedCaseTable");
            assertQuerySucceeds(testSession(), "SELECT * FROM mixedcasetable");

            // MIXEDCASETABLE is a third distinct name — does not exist
            assertQueryFails(testSession(), "SELECT * FROM MIXEDCASETABLE", ".*Table.*does not exist.*");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS MixedCaseTable");
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS mixedcasetable");
        }
    }

    @Test
    public void testCreateTableAsMixedCase()
    {
        // 'MyTable' is stored as-is; 'mytable' is a different (non-existent) table.
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE MyTable AS SELECT 1 AS id, 'hello' AS name");
            assertQuery(testSession(), "SELECT id, name FROM MyTable", "VALUES (1, 'hello')");

            assertQueryFails(testSession(), "SELECT * FROM mytable", ".*Table.*does not exist.*");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS MyTable");
        }
    }

    @Test
    public void testMixedCaseColumns()
    {
        // The Presto parser preserves identifier case exactly as written — it never lowercases.
        // normalizeIdentifier is the only thing that changes case; with case-sensitive-name-matching=true
        // it returns identifiers as-is regardless of whether they were quoted or unquoted in SQL.

        // Quoted: double-quotes tell the parser to treat the token as an identifier (not a keyword).
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE coltestquoted (\"FirstName\" varchar, \"lastName\" varchar)");
            assertQuerySucceeds(testSession(), "INSERT INTO coltestquoted VALUES ('Alice', 'Smith')");
            assertQuery(testSession(), "SELECT \"FirstName\", \"lastName\" FROM coltestquoted", "VALUES ('Alice', 'Smith')");
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS coltestquoted");

            // Unquoted mixed-case: same result — normalizeIdentifier preserves as-is.
            assertQuerySucceeds(testSession(), "CREATE TABLE coltestunquoted (FirstName varchar, lastName varchar)");
            assertQuerySucceeds(testSession(), "INSERT INTO coltestunquoted VALUES ('Bob', 'Jones')");
            assertQuery(testSession(), "SELECT FirstName, lastName FROM coltestunquoted", "VALUES ('Bob', 'Jones')");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS coltestquoted");
        }
    }

    @Test
    public void testMixedCasePartitionColumn()
    {
        // Quoted column in partition array: '"RegionId"' → strips quotes → 'RegionId' preserved.
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE partTestQuoted (\"RegionId\" bigint, name varchar) WITH (partitioning = ARRAY['\"RegionId\"'])");
            assertQuerySucceeds(testSession(), "INSERT INTO partTestQuoted VALUES (1, 'north'), (2, 'south')");
            assertQuery(testSession(), "SELECT \"RegionId\", name FROM partTestQuoted ORDER BY \"RegionId\"", "VALUES (1, 'north'), (2, 'south')");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS partTestQuoted");
        }

        // Unquoted in partition array: 'RegionId' preserved as-is.
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE partTestUnquoted (RegionId bigint, name varchar) WITH (partitioning = ARRAY['RegionId'])");
            assertQuerySucceeds(testSession(), "INSERT INTO partTestUnquoted VALUES (1, 'north'), (2, 'south')");
            assertQuery(testSession(), "SELECT RegionId, name FROM partTestUnquoted ORDER BY RegionId", "VALUES (1, 'north'), (2, 'south')");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS partTestUnquoted");
        }
    }

    @Test
    public void testShowColumnsCaseSensitive()
    {
        // Quoted: normalizeIdentifier preserves mixed case.
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE showcols (\"MyCol\" bigint, \"anotherCol\" varchar)");
            String result = getQueryRunner().execute(testSession(), "SHOW COLUMNS FROM showcols").toString();
            assertTrue(result.contains("MyCol"), "Expected 'MyCol' in SHOW COLUMNS output");
            assertTrue(result.contains("anotherCol"), "Expected 'anotherCol' in SHOW COLUMNS output");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS showcols");
        }

        // Unquoted: same result — normalizeIdentifier preserves as-is.
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE showcolslower (MyCol bigint, anotherCol varchar)");
            String resultLower = getQueryRunner().execute(testSession(), "SHOW COLUMNS FROM showcolslower").toString();
            assertTrue(resultLower.contains("MyCol"), "Expected 'MyCol' in SHOW COLUMNS output");
            assertTrue(resultLower.contains("anotherCol"), "Expected 'anotherCol' in SHOW COLUMNS output");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS showcolslower");
        }
    }

    @Test
    public void testNormalizeIdentifierPreservesCase()
    {
        String normalized = normalizeIdentifier("MyTable", ICEBERG_CATALOG);
        assertTrue(normalized.equals("MyTable"), "Expected normalizeIdentifier to preserve case, got: " + normalized);
        String normalizedLower = normalizeIdentifier("mytable", ICEBERG_CATALOG);
        assertTrue(normalizedLower.equals("mytable"), "Expected normalizeIdentifier to preserve lowercase, got: " + normalizedLower);
        assertFalse(normalized.equals("mytable"), "normalizeIdentifier must not lowercase 'MyTable' in case-sensitive mode");
        assertFalse(normalizedLower.equals("MYTABLE"), "normalizeIdentifier must not uppercase 'mytable' in case-sensitive mode");
    }

    @Test
    public void testRewriteDataFilesWithSortedByMixedCaseColumn()
    {
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE rewrite_sorted_cs (Id bigint, Name varchar, VAL integer)");
            assertQuerySucceeds(testSession(), "INSERT INTO rewrite_sorted_cs VALUES (3, 'c', 30), (1, 'a', 10)");
            assertQuerySucceeds(testSession(), "INSERT INTO rewrite_sorted_cs VALUES (2, 'b', 20), (4, 'd', 40)");

            // sorted_by uses the mixed-case column 'Id' exactly as stored — must resolve correctly
            assertQuerySucceeds(testSession(), format(
                    "CALL iceberg.system.rewrite_data_files(schema => '%s', table_name => 'rewrite_sorted_cs', " +
                            "sorted_by => ARRAY['Id'])",
                    SCHEMA));

            assertQuery(testSession(), "SELECT Id, Name, VAL FROM rewrite_sorted_cs ORDER BY Id", "VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40)");

            List<Types.NestedField> columns = getIcebergTable(SCHEMA, "rewrite_sorted_cs").schema().columns();
            assertEquals(columns.get(0).name(), "Id", "Column 0 must be stored as 'Id', not lower cased");
            assertEquals(columns.get(1).name(), "Name", "Column 1 must be stored as 'Name', not lower cased");
            assertEquals(columns.get(2).name(), "VAL", "Column 2 must be stored as 'VAL', not lower cased");
            assertFalse(columns.get(0).name().equals("id"), "Column 0 must NOT be stored as lowercase 'id'");
            assertFalse(columns.get(1).name().equals("name"), "Column 1 must NOT be stored as lowercase 'name'");
            assertFalse(columns.get(2).name().equals("val"), "Column 2 must NOT be stored as lowercase 'val'");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS rewrite_sorted_cs");
        }
    }
}
