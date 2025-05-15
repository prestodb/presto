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
import org.testng.annotations.AfterClass;
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
        // Define schema names and their expected stored values in Hive
        String schemaNameMixedSyllables = "HiveMixedCaseOn";
        String schemaNameMixed = "HiveMixedCase";

        // Create schemas
        query("CREATE SCHEMA IF NOT EXISTS " + SCHEMA_NAME);
        query("CREATE SCHEMA IF NOT EXISTS " + schemaNameMixedSyllables);
        query("CREATE SCHEMA IF NOT EXISTS " + schemaNameMixed);
        query("CREATE SCHEMA " + SCHEMA_NAME_UPPER);

        // Verify schema creation
        assertThat(query("SHOW SCHEMAS"))
                .contains(row(SCHEMA_NAME.toLowerCase(ENGLISH)))
                .contains(row(schemaNameMixed.toLowerCase(ENGLISH)))
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
        // Creating tables with different name conventions
        query("CREATE TABLE " + SCHEMA_NAME + ".testtable0 (name VARCHAR(50), id INT)");
        query("CREATE TABLE " + SCHEMA_NAME + ".testtable (name VARCHAR(50), ID INT)");
        query("CREATE TABLE " + SCHEMA_NAME + ".TestTable1 (Name VARCHAR(50), id INT)");
        query("CREATE TABLE " + SCHEMA_NAME + ".TESTTABLE2 (Name VARCHAR(50), ID INT)");
        query("CREATE TABLE " + SCHEMA_NAME_UPPER + ".TESTTABLE4 (Name VARCHAR(50), ID INT)");
        query("CREATE TABLE " + SCHEMA_NAME_UPPER + ".testtable02 (name VARCHAR(50), id INT, num DOUBLE)");
        query("CREATE TABLE " + SCHEMA_NAME_UPPER + ".TESTTABLE3 (name VARCHAR(50), id INT, num DOUBLE)");

        // Verify table existence and column properties
        assertThat(query("SHOW TABLES FROM " + SCHEMA_NAME))
                .containsOnly(row("testtable0"), row("testtable"), row("testtable1"), row("testtable2"));

        assertThat(query("SHOW TABLES FROM " + SCHEMA_NAME_UPPER))
                .containsOnly(row("testtable4"), row("testtable02"), row("testtable3"));

        assertThat(query("DESCRIBE " + SCHEMA_NAME + ".testtable0"))
                .contains(row("name", "varchar(50)", "", ""), row("id", "integer", "", ""));

        assertThat(query("DESCRIBE " + SCHEMA_NAME + ".testtable"))
                .contains(row("name", "varchar(50)", "", ""), row("id", "integer", "", ""));

        assertThat(query("DESCRIBE " + SCHEMA_NAME_UPPER + ".testtable4"))
                .contains(row("name", "varchar(50)", "", ""), row("id", "integer", "", ""));

        assertThat(query("DESCRIBE " + SCHEMA_NAME_UPPER + ".testtable02"))
                .contains(row("name", "varchar(50)", "", ""), row("id", "integer", "", ""), row("num", "double", "", ""));

        // Negative test: Creating a duplicate table with different case should fail
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
        // Insert data into tables with different case variations
        query("INSERT INTO " + SCHEMA_NAME + ".testtable VALUES ('amy', 112), ('mia', 123)");
        query("INSERT INTO " + SCHEMA_NAME + ".TESTTABLE2 VALUES ('ann', 112), ('mary', 123)");
        query("INSERT INTO " + SCHEMA_NAME_UPPER + ".testtable02 VALUES ('emma', 112, 200.002), ('mark', 123, 300.003)");
        query("INSERT INTO " + SCHEMA_NAME_UPPER + ".TESTTABLE3 VALUES ('emma', 112, 200.002), ('mark', 123, 300.003)");

        // Validate inserted data
        assertThat(query("SELECT * FROM " + SCHEMA_NAME + ".testtable"))
                .containsOnly(row("amy", 112), row("mia", 123));
        assertThat(query("SELECT * FROM " + SCHEMA_NAME + ".TESTTABLE2"))
                .containsOnly(row("ann", 112), row("mary", 123));
        assertThat(query("SELECT * FROM " + SCHEMA_NAME_UPPER + ".testtable02"))
                .containsOnly(row("emma", 112, 200.002), row("mark", 123, 300.003));
        assertThat(query("SELECT * FROM " + SCHEMA_NAME_UPPER + ".TESTTABLE3"))
                .containsOnly(row("emma", 112, 200.002), row("mark", 123, 300.003));
    }

    /*
     * This test verifies altering tables with different case variations in schema and table names.
     * It ensures that columns can be added renamed irrespective of case sensitivity.
     */
    @Test(groups = {MIXED_CASE}, dependsOnMethods = "testInsertDataWithMixedCaseNames")
    public void testTableAlterWithMixedCaseNames()
    {
        // Add columns to tables with various schema and table name cases
        query("ALTER TABLE " + SCHEMA_NAME + ".testtable ADD COLUMN num REAL");
        query("ALTER TABLE " + SCHEMA_NAME_UPPER + ".testtable02 ADD COLUMN num1 REAL");
        query("ALTER TABLE " + SCHEMA_NAME + ".TESTTABLE2 ADD COLUMN num01 REAL");
        query("ALTER TABLE " + SCHEMA_NAME_UPPER + ".TESTTABLE02 ADD COLUMN num2 REAL");

        // Verify the added columns
        assertThat(query("DESCRIBE " + SCHEMA_NAME + ".testtable"))
                .contains(row("num", "real", "", ""));
        assertThat(query("DESCRIBE " + SCHEMA_NAME_UPPER + ".testtable02"))
                .contains(row("num1", "real", "", ""));
        assertThat(query("DESCRIBE " + SCHEMA_NAME + ".TESTTABLE2"))
                .contains(row("num01", "real", "", ""));
        assertThat(query("DESCRIBE " + SCHEMA_NAME_UPPER + ".TESTTABLE02"))
                .contains(row("num2", "real", "", ""));

        // Rename columns
        query("ALTER TABLE " + SCHEMA_NAME + ".testtable RENAME COLUMN num TO numb");
        query("ALTER TABLE " + SCHEMA_NAME_UPPER + ".testtable02 RENAME COLUMN num1 TO numb01");

        // Verify column renaming
        assertThat(query("DESCRIBE " + SCHEMA_NAME + ".testtable"))
                .contains(row("numb", "real", "", ""));
        assertThat(query("DESCRIBE " + SCHEMA_NAME_UPPER + ".testtable02"))
                .contains(row("numb01", "real", "", ""));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        // Drop tables if they exist
        query("DROP TABLE IF EXISTS " + SCHEMA_NAME + ".testtable");
        query("DROP TABLE IF EXISTS " + SCHEMA_NAME + ".testtable0");
        query("DROP TABLE IF EXISTS " + SCHEMA_NAME + ".TestTable1");
        query("DROP TABLE IF EXISTS " + SCHEMA_NAME + ".TESTTABLE2");
        query("DROP TABLE IF EXISTS " + SCHEMA_NAME_UPPER + ".TESTTABLE4");
        query("DROP TABLE IF EXISTS " + SCHEMA_NAME_UPPER + ".testtable02");
        query("DROP TABLE IF EXISTS " + SCHEMA_NAME_UPPER + ".TESTTABLE3");

        // Drop schemas if they exist
        query("DROP SCHEMA IF EXISTS " + SCHEMA_NAME);
        query("DROP SCHEMA IF EXISTS " + SCHEMA_NAME_UPPER);
    }
}
