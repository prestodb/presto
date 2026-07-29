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
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.restConnectorProperties;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
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
                .setFormat(fileFormat)
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
            assertQueryFails(testSession(), "SELECT firstname, lastname FROM coltestquoted", ".*Column.*firstname.*cannot be resolved.*");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS coltestquoted");
        }

        // Unquoted mixed-case: normalizeIdentifier preserves as-is; analyser resolution is exact-case too.
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE coltestunquoted (FirstName varchar, lastName varchar)");
            assertQuerySucceeds(testSession(), "INSERT INTO coltestunquoted VALUES ('Bob', 'Jones')");
            assertQuery(testSession(), "SELECT FirstName, lastName FROM coltestunquoted", "VALUES ('Bob', 'Jones')");
            assertQueryFails(testSession(), "SELECT firstname, lastname FROM coltestunquoted", ".*Column.*firstname.*cannot be resolved.*");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS coltestunquoted");
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
            assertQueryFails(testSession(), "SELECT regionid FROM partTestQuoted", ".*Column.*regionid.*cannot be resolved.*");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS partTestQuoted");
        }

        // Unquoted in partition array: 'RegionId' preserved as-is.
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE partTestUnquoted (RegionId bigint, name varchar) WITH (partitioning = ARRAY['RegionId'])");
            assertQuerySucceeds(testSession(), "INSERT INTO partTestUnquoted VALUES (1, 'north'), (2, 'south')");
            assertQuery(testSession(), "SELECT RegionId, name FROM partTestUnquoted ORDER BY RegionId", "VALUES (1, 'north'), (2, 'south')");
            assertQueryFails(testSession(), "SELECT regionid FROM partTestUnquoted", ".*Column.*regionid.*cannot be resolved.*");
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
    public void testAnalyzeMixedCaseColumns()
    {
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE analyze_mixed (Id bigint, Name varchar, VAL integer)");
            assertQuerySucceeds(testSession(), "INSERT INTO analyze_mixed VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'alice', 30)");

            assertQuerySucceeds(testSession(), "ANALYZE analyze_mixed");

            Set<String> colNames = showStatsColumnNames("analyze_mixed");
            assertTrue(colNames.contains("Id"), "SHOW STATS must report stored column name 'Id', got: " + colNames);
            assertTrue(colNames.contains("Name"), "SHOW STATS must report stored column name 'Name', got: " + colNames);
            assertTrue(colNames.contains("VAL"), "SHOW STATS must report stored column name 'VAL', got: " + colNames);
            assertFalse(colNames.contains("id"), "SHOW STATS must not lowercase 'Id' to 'id', got: " + colNames);
            assertFalse(colNames.contains("name"), "SHOW STATS must not lowercase 'Name' to 'name', got: " + colNames);
            assertFalse(colNames.contains("val"), "SHOW STATS must not lowercase 'VAL' to 'val', got: " + colNames);
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS analyze_mixed");
        }
    }

    @Test
    public void testAnalyzeLowerCaseColumns()
    {
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE analyze_lower (id bigint, name varchar, val integer)");
            assertQuerySucceeds(testSession(), "INSERT INTO analyze_lower VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'alice', 30)");

            assertQuerySucceeds(testSession(), "ANALYZE analyze_lower");

            Set<String> colNames = showStatsColumnNames("analyze_lower");
            assertTrue(colNames.contains("id"), "SHOW STATS must report column 'id', got: " + colNames);
            assertTrue(colNames.contains("name"), "SHOW STATS must report column 'name', got: " + colNames);
            assertTrue(colNames.contains("val"), "SHOW STATS must report column 'val', got: " + colNames);
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS analyze_lower");
        }
    }

    @Test
    public void testShowStatsForFilteredMixedCaseColumns()
    {
        // RegionId is both a column and a partition column — predicates on it are pushed down.
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE stats_filter_cs (RegionId bigint, Name varchar) WITH (partitioning = ARRAY['RegionId'])");
            assertQuerySucceeds(testSession(), "INSERT INTO stats_filter_cs VALUES (1, 'north'), (2, 'south'), (1, 'east')");
            assertQuerySucceeds(testSession(), "ANALYZE stats_filter_cs");

            // Exact-case partition filter — pushed down to scan, no FilterNode remains → succeeds.
            assertQuerySucceeds(testSession(), "SHOW STATS FOR (SELECT * FROM stats_filter_cs WHERE RegionId = 1)");
            // Wrong-case partition filter — 'regionid' does not match stored 'RegionId' in case-sensitive mode.
            assertQueryFails(testSession(), "SHOW STATS FOR (SELECT * FROM stats_filter_cs WHERE regionid = 1)", ".*Column.*regionid.*cannot be resolved.*");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS stats_filter_cs");
        }
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

            assertQueryFails(testSession(), "SELECT id FROM rewrite_sorted_cs", ".*Column.*id.*cannot be resolved.*");
            assertQueryFails(testSession(), "SELECT name FROM rewrite_sorted_cs", ".*Column.*name.*cannot be resolved.*");
            assertQueryFails(testSession(), "SELECT val FROM rewrite_sorted_cs", ".*Column.*val.*cannot be resolved.*");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS rewrite_sorted_cs");
        }
    }

    /**
     * Verifies that columns whose names differ only in case — {@code id}, {@code ID}, {@code Id} —
     * plus {@code name} — are all stored and resolved as <em>distinct</em> columns in case-sensitive
     * mode, and that SELECT, WHERE, ORDER BY, and JOIN USING all honour exact-case matching.
     */
    @Test
    public void testColumnsDistinctByCase()
    {
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE casecols (id bigint, ID bigint, Id bigint, name varchar)");
            assertQuerySucceeds(testSession(), "INSERT INTO casecols VALUES (1, 10, 100, 'alpha')");
            assertQuerySucceeds(testSession(), "INSERT INTO casecols VALUES (2, 20, 200, 'beta')");
            assertQuerySucceeds(testSession(), "INSERT INTO casecols VALUES (3, 30, 300, 'gamma')");

            assertQuery(testSession(), "SELECT id, ID, Id, name FROM casecols ORDER BY id",
                    "VALUES (1, 10, 100, 'alpha'), (2, 20, 200, 'beta'), (3, 30, 300, 'gamma')");

            assertQuery(testSession(), "SELECT id, ID, Id, name FROM casecols WHERE id = 2", "VALUES (2, 20, 200, 'beta')");
            assertQuery(testSession(), "SELECT id, ID, Id, name FROM casecols WHERE ID = 20", "VALUES (2, 20, 200, 'beta')");
            assertQuery(testSession(), "SELECT id, ID, Id, name FROM casecols WHERE Id = 200", "VALUES (2, 20, 200, 'beta')");
            assertQuery(testSession(), "SELECT id, ID, Id, name FROM casecols WHERE name = 'gamma'", "VALUES (3, 30, 300, 'gamma')");

            assertQuery(testSession(), "SELECT count(*) FROM casecols WHERE ID = 3", "VALUES (0)");
            assertQuery(testSession(), "SELECT count(*) FROM casecols WHERE id = 100", "VALUES (0)");

            assertQuery(testSession(), "SELECT a.id, a.ID, a.Id, name FROM casecols a JOIN casecols b USING (name) WHERE a.id = 1",
                    "VALUES (1, 10, 100, 'alpha')");
            assertQuery(testSession(), "SELECT id, a.ID, a.Id FROM casecols a JOIN casecols b USING (id) WHERE id = 2",
                    "VALUES (2, 20, 200)");
            assertQueryFails(testSession(), "SELECT a.id FROM casecols a JOIN casecols b USING (iD)", ".*Column.*iD.*missing from left side of join.*");

            assertQueryFails(testSession(), "SELECT iD FROM casecols", ".*Column.*iD.*cannot be resolved.*");
            assertQueryFails(testSession(), "SELECT id FROM casecols WHERE iD = 1", ".*Column.*iD.*cannot be resolved.*");
            assertQueryFails(testSession(), "SELECT id FROM casecols ORDER BY iD", ".*Column.*iD.*cannot be resolved.*");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS casecols");
        }
    }

    /**
     * Verifies that nested ROW field names are stored and resolved exactly as written
     * in case-sensitive mode. Nested fields whose names differ only in case are distinct.
     */
    @Test
    public void testNestedRowFieldsCaseSensitive()
    {
        // Basic nested ROW: field names preserved as-is; wrong-case access fails.
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE nested_basic_cs (id bigint, addr ROW(\"Street\" varchar, \"City\" varchar))");
            assertQuerySucceeds(testSession(), "INSERT INTO nested_basic_cs VALUES (1, row('Main St', 'Springfield'))");

            // Exact-case field access succeeds.
            assertQuery(testSession(), "SELECT id, addr.Street, addr.City FROM nested_basic_cs",
                    "VALUES (1, 'Main St', 'Springfield')");

            // Wrong-case field access fails — 'street' and 'Street' are distinct in case-sensitive mode.
            assertQueryFails(testSession(), "SELECT addr.street FROM nested_basic_cs",
                    "line 1:8: 'addr.street' cannot be resolved");

            // Verify stored field names in raw Iceberg schema.
            Types.NestedField addrField = getIcebergTable(SCHEMA, "nested_basic_cs").schema().findField("addr");
            List<Types.NestedField> subFields = addrField.type().asStructType().fields();
            assertEquals(subFields.get(0).name(), "Street", "Nested field 0 must be stored as 'Street'");
            assertEquals(subFields.get(1).name(), "City", "Nested field 1 must be stored as 'City'");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS nested_basic_cs");
        }
    }

    /**
     * Verifies that nested fields whose names differ only in case are treated as distinct
     * columns in case-sensitive mode — e.g. {@code val}, {@code Val}, and {@code VAL} inside
     * the same ROW are three separate fields.
     */
    @Test
    public void testNestedRowFieldsDistinctByCase()
    {
        // Three sub-fields that differ only in case — all distinct in case-sensitive mode.
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE nested_case_distinct_cs (id bigint, metrics ROW(val bigint, Val bigint, VAL bigint))");
            assertQuerySucceeds(testSession(), "INSERT INTO nested_case_distinct_cs VALUES (1, row(10, 20, 30))");

            assertQuery(testSession(), "SELECT metrics.val, metrics.Val, metrics.VAL FROM nested_case_distinct_cs", "VALUES (10, 20, 30)");

            // Each variant resolves to a different stored field.
            assertQuery(testSession(), "SELECT metrics.val FROM nested_case_distinct_cs", "VALUES (10)");
            assertQuery(testSession(), "SELECT metrics.Val FROM nested_case_distinct_cs", "VALUES (20)");
            assertQuery(testSession(), "SELECT metrics.VAL FROM nested_case_distinct_cs", "VALUES (30)");

            // A name that matches none of the three fails.
            assertQueryFails(testSession(), "SELECT metrics.vAl FROM nested_case_distinct_cs", "line 1:8: 'metrics.vAl' cannot be resolved");

            // Raw schema must carry all three names exactly as written.
            Types.NestedField metricsField = getIcebergTable(SCHEMA, "nested_case_distinct_cs").schema().findField("metrics");
            List<Types.NestedField> subFields = metricsField.type().asStructType().fields();
            assertEquals(subFields.size(), 3, "ROW must have exactly 3 distinct sub-fields");
            assertEquals(subFields.get(0).name(), "val");
            assertEquals(subFields.get(1).name(), "Val");
            assertEquals(subFields.get(2).name(), "VAL");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS nested_case_distinct_cs");
        }
    }

    /**
     * Verifies deeply nested ROW (struct inside struct) with mixed-case field names in
     * case-sensitive mode — both levels of nesting preserve exact case.
     */
    @Test
    public void testDeeplyNestedRowFieldsCaseSensitive()
    {
        // Two levels of nesting: person ROW(Name varchar, address ROW(Street varchar, Zip varchar))
        try {
            assertQuerySucceeds(testSession(),
                    "CREATE TABLE nested_deep_cs (id bigint, person ROW(\"Name\" varchar, address ROW(\"Street\" varchar, \"Zip\" varchar)))");
            assertQuerySucceeds(testSession(),
                    "INSERT INTO nested_deep_cs VALUES (1, row('Alice', row('Baker St', '90210')))");

            // Top-level nested field access.
            assertQuery(testSession(), "SELECT person.Name FROM nested_deep_cs", "VALUES ('Alice')");

            // Second-level nested field access.
            assertQuery(testSession(), "SELECT person.address.Street, person.address.Zip FROM nested_deep_cs",
                    "VALUES ('Baker St', '90210')");

            // Wrong-case at either level fails.
            assertQueryFails(testSession(), "SELECT person.name FROM nested_deep_cs", "line 1:8: 'person.name' cannot be resolved");
            assertQueryFails(testSession(), "SELECT person.address.street FROM nested_deep_cs", "line 1:8: 'person.address.street' cannot be resolved");

            // Verify raw Iceberg schema for both levels.
            Types.NestedField personField = getIcebergTable(SCHEMA, "nested_deep_cs").schema().findField("person");
            List<Types.NestedField> personSubFields = personField.type().asStructType().fields();
            assertEquals(personSubFields.get(0).name(), "Name", "Level-1 field 0 must be 'Name'");
            assertEquals(personSubFields.get(1).name(), "address", "Level-1 field 1 must be 'address'");

            List<Types.NestedField> addrSubFields = personSubFields.get(1).type().asStructType().fields();
            assertEquals(addrSubFields.get(0).name(), "Street", "Level-2 field 0 must be 'Street'");
            assertEquals(addrSubFields.get(1).name(), "Zip", "Level-2 field 1 must be 'Zip'");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS nested_deep_cs");
        }
    }

    /**
     * Verifies that a nested sub-field can share the same name as its parent column,
     * and that case-sensitive resolution keeps the two scopes unambiguous.
     *
     * <p>Schema: {@code id bigint, addr ROW(addr varchar, id bigint, City varchar)}
     * <ul>
     *   <li>{@code addr} — top-level ROW column</li>
     *   <li>{@code addr.addr} — sub-field with the same name as the parent column</li>
     *   <li>{@code addr.id} — sub-field with the same name as the sibling top-level column</li>
     *   <li>{@code addr.City} — mixed-case sub-field distinct from any lowercase variant</li>
     * </ul>
     */
    @Test
    public void testNestedFieldNameSameAsParentColumn()
    {
        try {
            assertQuerySucceeds(testSession(),
                    "CREATE TABLE nested_shadow_cs (id bigint, addr ROW(addr varchar, id bigint, \"City\" varchar))");
            assertQuerySucceeds(testSession(),
                    "INSERT INTO nested_shadow_cs VALUES (1, row('Main St', 42, 'Springfield'))");

            // Top-level 'id' and nested 'addr.id' are independent.
            assertQuery(testSession(), "SELECT id FROM nested_shadow_cs", "VALUES (1)");
            assertQuery(testSession(), "SELECT addr.id FROM nested_shadow_cs", "VALUES (42)");

            // Top-level 'addr' (the ROW) vs nested 'addr.addr' (the varchar sub-field).
            assertQuery(testSession(), "SELECT addr.addr FROM nested_shadow_cs", "VALUES ('Main St')");

            // Mixed-case sub-field preserved as-is; wrong-case fails.
            assertQuery(testSession(), "SELECT addr.City FROM nested_shadow_cs", "VALUES ('Springfield')");
            assertQueryFails(testSession(), "SELECT addr.city FROM nested_shadow_cs", "line 1:8: 'addr.city' cannot be resolved");

            // Raw schema: top-level column is 'addr' (ROW), its sub-fields are 'addr', 'id', 'City'.
            Types.NestedField addrCol = getIcebergTable(SCHEMA, "nested_shadow_cs").schema().findField("addr");
            List<Types.NestedField> subFields = addrCol.type().asStructType().fields();
            assertEquals(subFields.size(), 3);
            assertEquals(subFields.get(0).name(), "addr", "Sub-field 0 must be 'addr' (same as parent)");
            assertEquals(subFields.get(1).name(), "id", "Sub-field 1 must be 'id' (same as sibling top-level)");
            assertEquals(subFields.get(2).name(), "City", "Sub-field 2 must be preserved as 'City'");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS nested_shadow_cs");
        }
    }

    private Set<String> showStatsColumnNames(String tableName)
    {
        MaterializedResult result = computeActual(testSession(), "SHOW STATS FOR " + tableName);
        return result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0))
                .filter(Objects::nonNull)
                .collect(toImmutableSet());
    }
}
