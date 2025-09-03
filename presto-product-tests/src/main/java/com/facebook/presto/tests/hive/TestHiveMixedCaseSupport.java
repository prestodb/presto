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
package com.facebook.presto.tests.hive;

import io.prestodb.tempto.ProductTest;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.MIXED_CASE;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.query.QueryExecutor.query;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class TestHiveMixedCaseSupport
        extends ProductTest
{
    private static final String SCHEMA_NAME = "hivemixedcaseon";
    private static final String SCHEMA_NAME_UPPER = "HIVEMIXEDCASEON1";
    private static final String SCHEMA_NAME_MIXED = "HiveMixedCase";

    /*
     * Test cases for creating schemas with different naming conventions in Hive.
     *
     * This class includes tests for:
     * 1. Creating a schema with a lowercase name.
     * 2. Creating a schema as the existing schema name with a mixed-case name.
     * 3. Creating a schema with a mixed-case name.
     * 4. Creating a schema with an uppercase name.
     *
     * Each test ensures the schema is created successfully.
     */
    @Test(groups = {MIXED_CASE})
    public void testCreateSchemasWithMixedCaseNames()
    {
        String schemaNameMixedSyllables = "HiveMixedCaseOn";

        query("CREATE SCHEMA IF NOT EXISTS " + SCHEMA_NAME);
        query("CREATE SCHEMA IF NOT EXISTS " + schemaNameMixedSyllables);
        query("CREATE SCHEMA IF NOT EXISTS " + SCHEMA_NAME_MIXED);
        query("CREATE SCHEMA " + SCHEMA_NAME_UPPER);

        assertThat(query("SHOW SCHEMAS"))
                .contains(row(SCHEMA_NAME.toLowerCase(ENGLISH)))
                .contains(row(SCHEMA_NAME_MIXED.toLowerCase(ENGLISH)))
                .contains(row(SCHEMA_NAME_UPPER.toLowerCase(ENGLISH)));
    }

    /*
     * Test cases for creating tables with different naming conventions in Hive.
     *
     * This test verifies table creation, schema behavior, and column definitions
     * when using different case variations for table and column names.
     *
     * Scenarios covered:
     * 1. Creating tables with lowercase, mixed-case, and uppercase names.
     * 2. Creating tables with uppercase column names.
     * 3. Creating tables in schemas with lowercase and uppercase names.
     *
     * The test ensures:
     * - Tables are created successfully.
     * - Table names are stored and retrieved correctly.
     * - Column definitions match expected types.
     */
    @Test(groups = {MIXED_CASE}, dependsOnMethods = "testCreateSchemasWithMixedCaseNames")
    public void testCreateTablesWithMixedCaseNames()
    {
        query("CREATE TABLE " + SCHEMA_NAME + ".testtable0 (name VARCHAR(50), id INT)");
        query("CREATE TABLE " + SCHEMA_NAME + ".testtable (name VARCHAR(50), ID INT)");
        query("CREATE TABLE " + SCHEMA_NAME + ".TestTable1 (Name VARCHAR(50), id INT)");
        query("CREATE TABLE " + SCHEMA_NAME + ".TESTTABLE2 (Name VARCHAR(50), ID INT)");
        query("CREATE TABLE " + SCHEMA_NAME_UPPER + ".TESTTABLE4 (Name VARCHAR(50), ID INT)");
        query("CREATE TABLE " + SCHEMA_NAME_UPPER + ".testtable02 (name VARCHAR(50), id INT, num DOUBLE)");
        query("CREATE TABLE " + SCHEMA_NAME_UPPER + ".TESTTABLE3 (name VARCHAR(50), id INT, num DOUBLE)");
        query("CREATE TABLE " + SCHEMA_NAME_MIXED + ".\"TestTable\" (Name VARCHAR(50), id INT)");

        assertThat(query("SHOW TABLES FROM " + SCHEMA_NAME))
                .containsOnly(row("testtable0"), row("testtable"), row("testtable1"), row("testtable2"));

        assertThat(query("SHOW TABLES FROM " + SCHEMA_NAME_UPPER))
                .containsOnly(row("testtable4"), row("testtable02"), row("testtable3"));

        assertThat(query("DESCRIBE " + SCHEMA_NAME + ".testtable0"))
                .contains(row("name", "varchar(50)", "", "", null, null, 50L), row("id", "integer", "", "", 10L, null, null));

        assertThat(query("DESCRIBE " + SCHEMA_NAME + ".testtable"))
                .contains(row("name", "varchar(50)", "", "", null, null, 50L), row("id", "integer", "", "", 10L, null, null));

        assertThat(query("DESCRIBE " + SCHEMA_NAME_UPPER + ".testtable4"))
                .contains(row("name", "varchar(50)", "", "", null, null, 50L), row("id", "integer", "", "", 10L, null, null));

        assertThat(query("DESCRIBE " + SCHEMA_NAME_UPPER + ".testtable02"))
                .contains(row("name", "varchar(50)", "", "", null, null, 50L), row("id", "integer", "", "", 10L, null, null), row("num", "double", "", "", 53L, null, null));

        assertThat(() -> query("CREATE TABLE " + SCHEMA_NAME + ".TESTTABLE0 (name VARCHAR(50), id INT)"))
                .failsWithMessage(format("line 1:1: Table 'hive.%s.testtable0' already exists", SCHEMA_NAME));
    }

    /*
     * This test validates inserting data into tables with different case variations in schema and table names.
     * It ensures that data is inserted and retrieved correctly regardless of case sensitivity.
     */
    @Test(groups = {MIXED_CASE}, dependsOnMethods = "testCreateTablesWithMixedCaseNames")
    public void testInsertDataWithMixedCaseNames()
    {
        query("INSERT INTO " + SCHEMA_NAME + ".testtable VALUES ('amy', 112), ('mia', 123)");
        query("INSERT INTO " + SCHEMA_NAME + ".TESTTABLE2 VALUES ('ann', 112), ('mary', 123)");
        query("INSERT INTO " + SCHEMA_NAME_UPPER + ".testtable02 VALUES ('emma', 112, 200.002), ('mark', 123, 300.003)");
        query("INSERT INTO " + SCHEMA_NAME_UPPER + ".TESTTABLE3 VALUES ('emma', 112, 200.002), ('mark', 123, 300.003)");

        query("INSERT INTO " + SCHEMA_NAME_MIXED + ".testtable VALUES ('amy1', 112)");
        query("INSERT INTO " + SCHEMA_NAME_MIXED + ".TestTable VALUES ('mary1', 123)");
        query("INSERT INTO \"" + SCHEMA_NAME_MIXED + "\".testtable VALUES ('amy2', 112)");
        query("INSERT INTO \"" + SCHEMA_NAME_MIXED + "\".TestTable VALUES ('mary2', 123)");
        query("INSERT INTO " + SCHEMA_NAME_MIXED + ".\"testtable\" VALUES ('amy3', 112)");
        query("INSERT INTO " + SCHEMA_NAME_MIXED + ".\"TestTable\" VALUES ('mary3', 123)");

        assertThat(query("SELECT * FROM " + SCHEMA_NAME + ".testtable"))
                .containsOnly(row("amy", 112), row("mia", 123));
        assertThat(query("SELECT name, ID FROM " + SCHEMA_NAME + ".testtable"))
                .containsOnly(row("amy", 112), row("mia", 123));
        assertThat(query("SELECT * FROM " + SCHEMA_NAME + ".TESTTABLE2"))
                .containsOnly(row("ann", 112), row("mary", 123));
        assertThat(query("SELECT * FROM " + SCHEMA_NAME_UPPER + ".testtable02"))
                .containsOnly(row("emma", 112, 200.002), row("mark", 123, 300.003));
        assertThat(query("SELECT * FROM " + SCHEMA_NAME_UPPER + ".TESTTABLE3"))
                .containsOnly(row("emma", 112, 200.002), row("mark", 123, 300.003));
        assertThat(query("SELECT name, ID, NUM FROM " + SCHEMA_NAME_UPPER + ".TESTTABLE3"))
                .containsOnly(row("emma", 112, 200.002), row("mark", 123, 300.003));
        assertThat(query("SELECT * FROM \"" + SCHEMA_NAME_MIXED + "\".\"TestTable\"")).containsOnly(
                row("amy1", 112),
                row("mary1", 123),
                row("amy2", 112),
                row("mary2", 123),
                row("amy3", 112),
                row("mary3", 123));
    }

    /*
     * This test verifies altering tables with different case variations in schema and table names.
     * It ensures that columns can be added renamed irrespective of case sensitivity.
     */
    @Test(groups = {MIXED_CASE}, dependsOnMethods = "testInsertDataWithMixedCaseNames")
    public void testTableAlterWithMixedCaseNames()
    {
        query("ALTER TABLE " + SCHEMA_NAME + ".testtable ADD COLUMN num REAL");
        query("ALTER TABLE " + SCHEMA_NAME + ".testtable ADD COLUMN NuM2 REAL");

        query("ALTER TABLE " + SCHEMA_NAME + ".TESTTABLE2 ADD COLUMN num01 REAL");
        query("ALTER TABLE " + SCHEMA_NAME + ".TESTTABLE2 ADD COLUMN NuM2 REAL");

        query("ALTER TABLE " + SCHEMA_NAME_UPPER + ".testtable02 ADD COLUMN num1 REAL");
        query("ALTER TABLE " + SCHEMA_NAME_UPPER + ".TESTTABLE02 ADD COLUMN NuM2 REAL");
        // Negative test: Creating a duplicate column with different case should fail in mysql
        assertThat(() -> query("ALTER TABLE " + SCHEMA_NAME_UPPER + ".TESTTABLE02 ADD COLUMN num2 REAL"))
                .failsWithMessage("line 1:1: Column 'num2' already exists");

        assertThat(query("DESCRIBE " + SCHEMA_NAME + ".testtable")).containsOnly(
                row("name", "varchar(50)", "", "", null, null, 50L),
                row("id", "integer", "", "", 10L, null, null),
                row("num", "real", "", "", 24L, null, null),
                row("num2", "real", "", "", 24L, null, null));

        assertThat(query("DESCRIBE " + SCHEMA_NAME + ".TESTTABLE2")).containsOnly(
                row("name", "varchar(50)", "", "", null, null, 50L),
                row("id", "integer", "", "", 10L, null, null),
                row("num01", "real", "", "", 24L, null, null),
                row("num2", "real", "", "", 24L, null, null));

        assertThat(query("DESCRIBE " + SCHEMA_NAME_UPPER + ".testtable02")).containsOnly(
                row("name", "varchar(50)", "", "", null, null, 50L),
                row("id", "integer", "", "", 10L, null, null),
                row("num", "double", "", "", 53L, null, null),
                row("num1", "real", "", "", 24L, null, null),
                row("num2", "real", "", "", 24L, null, null));

        assertThat(query("DESCRIBE " + SCHEMA_NAME_UPPER + ".TESTTABLE02")).containsOnly(
                row("name", "varchar(50)", "", "", null, null, 50L),
                row("id", "integer", "", "", 10L, null, null),
                row("num", "double", "", "", 53L, null, null),
                row("num1", "real", "", "", 24L, null, null),
                row("num2", "real", "", "", 24L, null, null));

        query("ALTER TABLE " + SCHEMA_NAME + ".testtable RENAME COLUMN num TO numb");
        query("ALTER TABLE " + SCHEMA_NAME + ".testtable RENAME COLUMN NuM2 TO NuM02");
        query("ALTER TABLE " + SCHEMA_NAME_UPPER + ".testtable02 RENAME COLUMN num1 TO numb01");
        query("ALTER TABLE " + SCHEMA_NAME_UPPER + ".testtable02 RENAME COLUMN NuM2 TO NuM02");

        assertThat(query("DESCRIBE " + SCHEMA_NAME + ".testtable")).containsOnly(
                row("name", "varchar(50)", "", "", null, null, 50L),
                row("id", "integer", "", "", 10L, null, null),
                row("numb", "real", "", "", 24L, null, null),
                row("num02", "real", "", "", 24L, null, null));
        assertThat(query("DESCRIBE " + SCHEMA_NAME_UPPER + ".testtable02")).containsOnly(
                row("name", "varchar(50)", "", "", null, null, 50L),
                row("id", "integer", "", "", 10L, null, null),
                row("num", "double", "", "", 53L, null, null),
                row("numb01", "real", "", "", 24L, null, null),
                row("num02", "real", "", "", 24L, null, null));

        // drop column
        query("ALTER TABLE " + SCHEMA_NAME + ".testtable DROP COLUMN NuM02");
        query("ALTER TABLE " + SCHEMA_NAME_UPPER + ".testtable02 DROP COLUMN numb01");

        assertThat(query("DESCRIBE " + SCHEMA_NAME + ".testtable")).containsOnly(
                row("name", "varchar(50)", "", "", null, null, 50L),
                row("id", "integer", "", "", 10L, null, null),
                row("numb", "real", "", "", 24L, null, null));
        assertThat(query("DESCRIBE " + SCHEMA_NAME_UPPER + ".testtable02")).containsOnly(
                row("name", "varchar(50)", "", "", null, null, 50L),
                row("id", "integer", "", "", 10L, null, null),
                row("num", "double", "", "", 53L, null, null),
                row("num02", "real", "", "", 24L, null, null));
    }

    @Test(groups = {MIXED_CASE}, dependsOnMethods = "testTableAlterWithMixedCaseNames")
    public void testDropMixedCaseTablesAndSchemas()
    {
        query("DROP TABLE IF EXISTS " + SCHEMA_NAME + ".testtable");
        query("DROP TABLE IF EXISTS " + SCHEMA_NAME + ".testtable0");
        query("DROP TABLE IF EXISTS " + SCHEMA_NAME + ".TestTable1");
        query("DROP TABLE IF EXISTS " + SCHEMA_NAME + ".TESTTABLE2");
        query("DROP TABLE IF EXISTS " + SCHEMA_NAME_UPPER + ".TESTTABLE4");
        query("DROP TABLE IF EXISTS " + SCHEMA_NAME_UPPER + ".testtable02");
        query("DROP TABLE IF EXISTS " + SCHEMA_NAME_UPPER + ".TESTTABLE3");

        query("DROP SCHEMA IF EXISTS " + SCHEMA_NAME);
        query("DROP SCHEMA IF EXISTS " + SCHEMA_NAME_UPPER);
    }
}
