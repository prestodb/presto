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
                .setFormat(fileFormat)
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

    /**
     * Verifies that ANALYZE succeeds when the table has mixed-case column names in case-insensitive mode.
     * All column names are lowercased by {@code normalizeIdentifier}, so SHOW STATS must report
     * the stored lowercase names.
     */
    @Test
    public void testAnalyzeMixedCaseColumns()
    {
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE analyze_mixed_ci (Id bigint, Name varchar, VAL integer)");
            assertQuerySucceeds(testSession(), "INSERT INTO analyze_mixed_ci VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'alice', 30)");

            assertQuerySucceeds(testSession(), "ANALYZE analyze_mixed_ci");

            // All names are lowercased — stored as 'id', 'name', 'val'.
            Set<String> colNames = showStatsColumnNames("analyze_mixed_ci");
            assertTrue(colNames.contains("id"), "SHOW STATS must report stored lowercase column 'id', got: " + colNames);
            assertTrue(colNames.contains("name"), "SHOW STATS must report stored lowercase column 'name', got: " + colNames);
            assertTrue(colNames.contains("val"), "SHOW STATS must report stored lowercase column 'val', got: " + colNames);
            assertFalse(colNames.contains("Id"), "SHOW STATS must not preserve mixed-case 'Id' in case-insensitive mode, got: " + colNames);
            assertFalse(colNames.contains("Name"), "SHOW STATS must not preserve mixed-case 'Name' in case-insensitive mode, got: " + colNames);
            assertFalse(colNames.contains("VAL"), "SHOW STATS must not preserve mixed-case 'VAL' in case-insensitive mode, got: " + colNames);
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS analyze_mixed_ci");
        }
    }

    /**
     * Verifies that ANALYZE and SHOW STATS work correctly with all-lowercase column names in
     * case-insensitive mode — acts as a regression guard that the fix does not break the normal path.
     */
    @Test
    public void testAnalyzeLowerCaseColumns()
    {
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE analyze_lower_ci (id bigint, name varchar, val integer)");
            assertQuerySucceeds(testSession(), "INSERT INTO analyze_lower_ci VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'alice', 30)");

            assertQuerySucceeds(testSession(), "ANALYZE analyze_lower_ci");

            Set<String> colNames = showStatsColumnNames("analyze_lower_ci");
            assertTrue(colNames.contains("id"), "SHOW STATS must report column 'id', got: " + colNames);
            assertTrue(colNames.contains("name"), "SHOW STATS must report column 'name', got: " + colNames);
            assertTrue(colNames.contains("val"), "SHOW STATS must report column 'val', got: " + colNames);
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS analyze_lower_ci");
        }
    }

    /**
     * Verifies that SHOW STATS FOR (subquery) with a partition column filter works in
     * case-insensitive mode. The column is stored as 'regionid' (lowercased), so any
     * case variant of the identifier resolves to the same partition column, ensuring
     * the predicate is pushed down and no FilterNode remains.
     */
    @Test
    public void testShowStatsForFilteredMixedCaseColumns()
    {
        // Column and partition stored as 'regionid' (normalizeIdentifier lowercases).
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE stats_filter_ci (RegionId bigint, Name varchar) WITH (partitioning = ARRAY['RegionId'])");
            assertQuerySucceeds(testSession(), "INSERT INTO stats_filter_ci VALUES (1, 'north'), (2, 'south'), (1, 'east')");
            assertQuerySucceeds(testSession(), "ANALYZE stats_filter_ci");

            // Lowercase filter — matches stored 'regionid' partition column → pushed down → succeeds.
            assertQuerySucceeds(testSession(), "SHOW STATS FOR (SELECT * FROM stats_filter_ci WHERE regionid = 1)");
            // Mixed-case filter — also normalised to 'regionid' → same partition column → succeeds.
            assertQuerySucceeds(testSession(), "SHOW STATS FOR (SELECT * FROM stats_filter_ci WHERE RegionId = 1)");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS stats_filter_ci");
        }
    }

    /**
     * Verifies that in case-insensitive mode all case variants of an identifier collapse to the
     * same lowercase name.
     */
    @Test
    public void testColumnsDistinctByCase()
    {
        assertQueryFails(testSession(), "CREATE TABLE casecols_ci (id bigint, ID bigint, Id bigint, name varchar)",
                ".*Column name 'ID' specified more than once");

        // Table with a single 'id' column — all case variants of the name resolve to it.
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE casecols_ci (id bigint, name varchar)");
            assertQuerySucceeds(testSession(), "INSERT INTO casecols_ci VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')");

            // All case variants of 'id' resolve to the same stored lowercase column.
            assertQuery(testSession(), "SELECT id FROM casecols_ci ORDER BY id", "VALUES (1), (2), (3)");
            assertQuery(testSession(), "SELECT ID FROM casecols_ci ORDER BY ID", "VALUES (1), (2), (3)");
            assertQuery(testSession(), "SELECT Id FROM casecols_ci ORDER BY Id", "VALUES (1), (2), (3)");

            // WHERE and ORDER BY also resolve any case variant to the same column.
            assertQuery(testSession(), "SELECT id FROM casecols_ci WHERE ID = 2", "VALUES (2)");
            assertQuery(testSession(), "SELECT id FROM casecols_ci WHERE Id = 3", "VALUES (3)");
            assertQuery(testSession(), "SELECT id FROM casecols_ci ORDER BY ID", "VALUES (1), (2), (3)");

            // JOIN USING works with any case variant of 'name' — all resolve to the same column.
            assertQuery(testSession(), "SELECT a.id FROM casecols_ci a JOIN casecols_ci b USING (name) WHERE a.id = 1", "VALUES (1)");
            assertQuery(testSession(), "SELECT a.id FROM casecols_ci a JOIN casecols_ci b USING (NAME) WHERE a.id = 2", "VALUES (2)");

            assertQueryFails(testSession(), "SELECT xyz FROM casecols_ci", ".*Column.*xyz.*cannot be resolved.*");
            assertQueryFails(testSession(), "SELECT XYZ FROM casecols_ci", ".*Column.*xyz.*cannot be resolved.*");
            assertQueryFails(testSession(), "SELECT id FROM casecols_ci WHERE XYZ = 1", ".*Column.*xyz.*cannot be resolved.*");
            assertQueryFails(testSession(), "SELECT id FROM casecols_ci ORDER BY XYZ", ".*Column.*xyz.*cannot be resolved.*");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS casecols_ci");
        }
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

    /**
     * Verifies that nested ROW field names are lowercased by {@code normalizeIdentifier}
     * in case-insensitive mode — mixed-case names are stored and resolved as lowercase only.
     */
    @Test
    public void testNestedRowFieldsCaseInsensitive()
    {
        // Basic nested ROW: mixed-case field names are lowercased on write.
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE nested_basic_ci (id bigint, addr ROW(\"Street\" varchar, \"City\" varchar))");
            assertQuerySucceeds(testSession(), "INSERT INTO nested_basic_ci VALUES (1, ROW('Main St', 'Springfield'))");

            // Stored as 'street' and 'city' — any case variant resolves to the same field.
            assertQuery(testSession(), "SELECT addr.street, addr.city FROM nested_basic_ci",
                    "VALUES ('Main St', 'Springfield')");
            assertQuery(testSession(), "SELECT addr.\"Street\", addr.\"City\" FROM nested_basic_ci",
                    "VALUES ('Main St', 'Springfield')");
            assertQuery(testSession(), "SELECT addr.STREET, addr.CITY FROM nested_basic_ci",
                    "VALUES ('Main St', 'Springfield')");

            // Verify that the raw Iceberg schema stores lowercase field names.
            Types.NestedField addrField = getIcebergTable(SCHEMA, "nested_basic_ci").schema().findField("addr");
            List<Types.NestedField> subFields = addrField.type().asStructType().fields();
            assertEquals(subFields.get(0).name(), "street", "Nested field 0 must be lowercased to 'street'");
            assertEquals(subFields.get(1).name(), "city", "Nested field 1 must be lowercased to 'city'");
            assertFalse(subFields.get(0).name().equals("Street"), "Nested field must not preserve 'Street' in case-insensitive mode");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS nested_basic_ci");
        }
    }

    /**
     * Verifies that nested fields whose names differ only in case collapse to the same
     * lowercase field in case-insensitive mode — duplicates are rejected at CREATE time.
     */
    @Test
    public void testNestedRowFieldsDistinctByCaseInsensitive()
    {
        // Attempting to create a ROW with sub-fields that normalise to the same name fails.
        assertQueryFails(testSession(),
                "CREATE TABLE nested_case_dup_ci (id bigint, metrics ROW(val bigint, Val bigint, VAL bigint))",
                ".*Column name 'Val' specified more than once.*");

        // A ROW with already-distinct lowercase sub-field names works, and any-case access resolves correctly.
        try {
            assertQuerySucceeds(testSession(), "CREATE TABLE nested_case_lower_ci (id bigint, metrics ROW(low bigint, high bigint))");
            assertQuerySucceeds(testSession(), "INSERT INTO nested_case_lower_ci VALUES (1, ROW(10, 99))");

            assertQuery(testSession(), "SELECT metrics.low, metrics.high FROM nested_case_lower_ci", "VALUES (10, 99)");
            // Any-case variant resolves to the same stored lowercase field.
            assertQuery(testSession(), "SELECT metrics.LOW, metrics.HIGH FROM nested_case_lower_ci", "VALUES (10, 99)");
            assertQuery(testSession(), "SELECT metrics.Low, metrics.High FROM nested_case_lower_ci", "VALUES (10, 99)");

            Types.NestedField metricsField = getIcebergTable(SCHEMA, "nested_case_lower_ci").schema().findField("metrics");
            List<Types.NestedField> subFields = metricsField.type().asStructType().fields();
            assertEquals(subFields.size(), 2, "ROW must have exactly 2 sub-fields");
            assertEquals(subFields.get(0).name(), "low");
            assertEquals(subFields.get(1).name(), "high");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS nested_case_lower_ci");
        }
    }

    /**
     * Verifies deeply nested ROW (struct inside struct) in case-insensitive mode — both
     * levels of nesting are lowercased, and any-case access resolves to the stored name.
     */
    @Test
    public void testDeeplyNestedRowFieldsCaseInsensitive()
    {
        // Two levels of nesting with mixed-case names.
        try {
            assertQuerySucceeds(testSession(),
                    "CREATE TABLE nested_deep_ci (id bigint, person ROW(\"Name\" varchar, address ROW(\"Street\" varchar, \"Zip\" varchar)))");
            assertQuerySucceeds(testSession(),
                    "INSERT INTO nested_deep_ci VALUES (1, ROW('Alice', ROW('Baker St', '90210')))");

            // All field names are lowercased — any-case variant resolves to the stored lowercase name.
            assertQuery(testSession(), "SELECT person.name FROM nested_deep_ci", "VALUES ('Alice')");
            assertQuery(testSession(), "SELECT person.\"Name\" FROM nested_deep_ci", "VALUES ('Alice')");
            assertQuery(testSession(), "SELECT person.address.street, person.address.zip FROM nested_deep_ci",
                    "VALUES ('Baker St', '90210')");
            assertQuery(testSession(), "SELECT person.ADDRESS.STREET, person.ADDRESS.ZIP FROM nested_deep_ci",
                    "VALUES ('Baker St', '90210')");

            // Verify raw Iceberg schema stores lowercase names at both levels.
            Types.NestedField personField = getIcebergTable(SCHEMA, "nested_deep_ci").schema().findField("person");
            List<Types.NestedField> personSubFields = personField.type().asStructType().fields();
            assertEquals(personSubFields.get(0).name(), "name", "Level-1 field 0 must be lowercased to 'name'");
            assertEquals(personSubFields.get(1).name(), "address", "Level-1 field 1 must stay 'address'");
            assertFalse(personSubFields.get(0).name().equals("Name"), "Level-1 field must not preserve 'Name' in case-insensitive mode");

            List<Types.NestedField> addrSubFields = personSubFields.get(1).type().asStructType().fields();
            assertEquals(addrSubFields.get(0).name(), "street", "Level-2 field 0 must be lowercased to 'street'");
            assertEquals(addrSubFields.get(1).name(), "zip", "Level-2 field 1 must be lowercased to 'zip'");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS nested_deep_ci");
        }
    }

    /**
     * Verifies that a nested sub-field can share the same name as its parent column,
     * and that case-insensitive resolution keeps the two scopes unambiguous.
     *
     * <p>Schema: {@code id bigint, addr ROW(addr varchar, id bigint, City varchar)}
     * <ul>
     *   <li>{@code addr} — top-level ROW column (stored as {@code addr})</li>
     *   <li>{@code addr.addr} — sub-field with the same name as the parent; stored as {@code addr}</li>
     *   <li>{@code addr.id} — sub-field with the same name as the sibling top-level column</li>
     *   <li>{@code addr.City} — lowercased to {@code city} on write</li>
     * </ul>
     */
    @Test
    public void testNestedFieldNameSameAsParentColumn()
    {
        try {
            assertQuerySucceeds(testSession(),
                    "CREATE TABLE nested_shadow_ci (id bigint, addr ROW(addr varchar, id bigint, \"City\" varchar))");
            assertQuerySucceeds(testSession(),
                    "INSERT INTO nested_shadow_ci VALUES (1, ROW('Main St', 42, 'Springfield'))");

            // Top-level 'id' and nested 'addr.id' are independent.
            assertQuery(testSession(), "SELECT id FROM nested_shadow_ci", "VALUES (1)");
            assertQuery(testSession(), "SELECT addr.id FROM nested_shadow_ci", "VALUES (42)");

            // Top-level 'addr' (the ROW) vs nested 'addr.addr' (the varchar sub-field).
            assertQuery(testSession(), "SELECT addr.addr FROM nested_shadow_ci", "VALUES ('Main St')");

            // Mixed-case 'City' is lowercased to 'city'; any-case variant resolves to it.
            assertQuery(testSession(), "SELECT addr.city FROM nested_shadow_ci", "VALUES ('Springfield')");
            assertQuery(testSession(), "SELECT addr.\"City\" FROM nested_shadow_ci", "VALUES ('Springfield')");
            assertQuery(testSession(), "SELECT addr.CITY FROM nested_shadow_ci", "VALUES ('Springfield')");

            // Raw schema: sub-fields are lowercased — 'addr', 'id', 'city'.
            Types.NestedField addrCol = getIcebergTable(SCHEMA, "nested_shadow_ci").schema().findField("addr");
            List<Types.NestedField> subFields = addrCol.type().asStructType().fields();
            assertEquals(subFields.size(), 3);
            assertEquals(subFields.get(0).name(), "addr", "Sub-field 0 must be 'addr' (same as parent)");
            assertEquals(subFields.get(1).name(), "id", "Sub-field 1 must be 'id' (same as sibling top-level)");
            assertEquals(subFields.get(2).name(), "city", "Sub-field 2 must be lowercased to 'city'");
            assertFalse(subFields.get(2).name().equals("City"), "Sub-field 2 must not preserve 'City' in case-insensitive mode");
        }
        finally {
            assertQuerySucceeds(testSession(), "DROP TABLE IF EXISTS nested_shadow_ci");
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
