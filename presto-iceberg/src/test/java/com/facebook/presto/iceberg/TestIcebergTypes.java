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
package com.facebook.presto.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.hive.HiveCommonSessionProperties.PARQUET_BATCH_READ_OPTIMIZATION_ENABLED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestIcebergTypes
        extends AbstractTestQueryFramework
{
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder().build().getQueryRunner();
    }

    @DataProvider(name = "testTimestampWithTimezone")
    public Object[][] createTestTimestampWithTimezoneData()
    {
        return new Object[][] {
                {Session.builder(getSession())
                        .setCatalogSessionProperty("iceberg", PARQUET_BATCH_READ_OPTIMIZATION_ENABLED, "true")
                        .build()},
                {Session.builder(getSession())
                        .setCatalogSessionProperty("iceberg", PARQUET_BATCH_READ_OPTIMIZATION_ENABLED, "false")
                        .build()}
        };
    }

    @Test(dataProvider = "testTimestampWithTimezone")
    public void testTimestampWithTimezone(Session session)
    {
        QueryRunner runner = getQueryRunner();
        String timestamptz = "TIMESTAMP '1984-12-08 00:10:00 America/Los_Angeles'";
        String timestamp = "TIMESTAMP '1984-12-08 00:10:00'";

        dropTableIfExists(runner, session.getCatalog().get(), session.getSchema().get(), "test_timestamptz");
        assertQuerySucceeds(session, "CREATE TABLE test_timestamptz(a TIMESTAMP WITH TIME ZONE, b TIMESTAMP, c TIMESTAMP WITH TIME ZONE)");

        String row = "(" + timestamptz + ", " + timestamp + ", " + timestamptz + ")";
        for (int i = 0; i < 10; i++) {
            assertUpdate(session, "INSERT INTO test_timestamptz values " + row, 1);
        }

        MaterializedResult initialRows = runner.execute(session, "SELECT * FROM test_timestamptz");

        List<Type> types = initialRows.getTypes();
        assertTrue(types.get(0) instanceof TimestampWithTimeZoneType);
        assertTrue(types.get(1) instanceof TimestampType);

        List<MaterializedRow> rows = initialRows.getMaterializedRows();
        for (int i = 0; i < 10; i++) {
            assertEquals("[1984-12-08T08:10Z[UTC], 1984-12-08T00:10, 1984-12-08T08:10Z[UTC]]", rows.get(i).toString());
        }

        dropTableIfExists(runner, session.getCatalog().get(), session.getSchema().get(), "test_timestamptz_partition");
        assertQuerySucceeds(session, "CREATE TABLE test_timestamptz_partition(a TIMESTAMP WITH TIME ZONE, b TIMESTAMP, c TIMESTAMP WITH TIME ZONE) " +
                "WITH (PARTITIONING = ARRAY['b'])");
        assertUpdate(session, "INSERT INTO test_timestamptz_partition (a, b, c) SELECT a, b, c FROM test_timestamptz", 10);

        MaterializedResult partitionRows = runner.execute(session, "SELECT * FROM test_timestamptz");

        List<Type> partitionTypes = partitionRows.getTypes();
        assertTrue(partitionTypes.get(0) instanceof TimestampWithTimeZoneType);
        assertTrue(partitionTypes.get(1) instanceof TimestampType);

        rows = partitionRows.getMaterializedRows();
        for (int i = 0; i < 10; i++) {
            assertEquals("[1984-12-08T08:10Z[UTC], 1984-12-08T00:10, 1984-12-08T08:10Z[UTC]]", rows.get(i).toString());
        }

        String earlyTimestamptz = "TIMESTAMP '1980-12-08 00:10:00 America/Los_Angeles'";
        dropTableIfExists(runner, session.getCatalog().get(), session.getSchema().get(), "test_timestamptz_filter");
        assertQuerySucceeds(session, "CREATE TABLE test_timestamptz_filter(a TIMESTAMP WITH TIME ZONE)");

        for (int i = 0; i < 5; i++) {
            assertUpdate(session, "INSERT INTO test_timestamptz_filter VALUES (" + earlyTimestamptz + ")", 1);
        }
        for (int i = 0; i < 5; i++) {
            assertUpdate(session, "INSERT INTO test_timestamptz_filter VALUES (" + timestamptz + ")", 1);
        }

        MaterializedResult lateRows = runner.execute(session, "SELECT a FROM test_timestamptz_filter WHERE a > " + earlyTimestamptz);
        assertEquals(lateRows.getMaterializedRows().size(), 5);

        MaterializedResult lateRowsFromEquals = runner.execute(session, "SELECT a FROM test_timestamptz_filter WHERE a = " + timestamptz);
        com.facebook.presto.testing.assertions.Assert.assertEquals(lateRows, lateRowsFromEquals);

        MaterializedResult earlyRows = runner.execute(session, "SELECT a FROM test_timestamptz_filter WHERE a < " + timestamptz);
        assertEquals(earlyRows.getMaterializedRows().size(), 5);

        MaterializedResult earlyRowsFromEquals = runner.execute(session, "SELECT a FROM test_timestamptz_filter WHERE a = " + earlyTimestamptz);
        com.facebook.presto.testing.assertions.Assert.assertEquals(earlyRows, earlyRowsFromEquals);
    }

    /**
     * Test for struct with hyphenated field names.
     * Before the fix, INSERT was failing because PrimitiveTypeMapBuilder
     * was not calling makeCompatibleName for nested struct fields.
     * SELECT was returning NULL because ColumnIOConverter.constructField
     * was using raw field names for name-based lookup but parquet had hex-encoded names.
     */
    @Test
    public void testStructWithHyphenatedFieldNames()
    {
        String tableName = "test_hyphenated_struct";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (" +
                    "id INT, " +
                    "location ROW(\"aws-region\" VARCHAR, \"data-center\" VARCHAR, \"zone-id\" INT)" +
                    ")");

            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(1, ROW('us-west-2', 'dc-01', 100)), " +
                    "(2, ROW('eu-central-1', 'dc-02', 200))", 2);

            // Test SYNTHESIZED path - SELECT specific subfield
            assertQuery(
                    "SELECT id, location.\"aws-region\", location.\"data-center\", location.\"zone-id\" FROM " + tableName,
                    "VALUES (1, 'us-west-2', 'dc-01', 100), (2, 'eu-central-1', 'dc-02', 200)");

            // Test regular path - SELECT full struct (constructField path)
            assertQuery(
                    "SELECT location.\"aws-region\", location.\"data-center\", location.\"zone-id\" FROM " + tableName,
                    "VALUES ('us-west-2', 'dc-01', 100), ('eu-central-1', 'dc-02', 200)");

            // Test SELECT * - verify individual fields
            assertQuery(
                    "SELECT id, location.\"aws-region\", location.\"data-center\", location.\"zone-id\" FROM " + tableName,
                    "VALUES (1, 'us-west-2', 'dc-01', 100), (2, 'eu-central-1', 'dc-02', 200)");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    /**
     * Test for nested struct with hyphenated field names.
     * Tests deeper nesting levels with special characters.
     * Validates fix in both PrimitiveTypeMapBuilder (write) and
     * ColumnIOConverter.constructField (read) for multi-level nesting.
     */
    @Test
    public void testNestedStructWithHyphenatedFieldNames()
    {
        String tableName = "test_nested_hyphenated";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (" +
                    "id INT, " +
                    "metadata ROW(" +
                    "  \"user-info\" ROW(\"user-id\" INT, \"user-name\" VARCHAR), " +
                    "  \"request-time\" TIMESTAMP" +
                    ")" +
                    ")");

            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(1, ROW(ROW(101, 'Alice'), TIMESTAMP '2024-01-01 10:00:00')), " +
                    "(2, ROW(ROW(102, 'Bob'), TIMESTAMP '2024-01-02 11:00:00'))", 2);

            // Test SYNTHESIZED path - deeply nested subfield access
            assertQuery(
                    "SELECT id, metadata.\"user-info\".\"user-id\", metadata.\"user-info\".\"user-name\" FROM " + tableName,
                    "VALUES (1, 101, 'Alice'), (2, 102, 'Bob')");

            // Test regular path - full struct read via subfields
            assertQuery(
                    "SELECT metadata.\"user-info\".\"user-id\", metadata.\"user-info\".\"user-name\", metadata.\"request-time\" FROM " + tableName,
                    "VALUES (101, 'Alice', TIMESTAMP '2024-01-01 10:00:00'), (102, 'Bob', TIMESTAMP '2024-01-02 11:00:00')");

            // Test SELECT * - verify all fields
            assertQuery(
                    "SELECT id, metadata.\"user-info\".\"user-id\", metadata.\"user-info\".\"user-name\", metadata.\"request-time\" FROM " + tableName,
                    "VALUES (1, 101, 'Alice', TIMESTAMP '2024-01-01 10:00:00'), (2, 102, 'Bob', TIMESTAMP '2024-01-02 11:00:00')");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    /**
     * Test for column pushdown with hyphenated struct fields.
     * Validates the IcebergPageSourceProvider.getColumnType fix
     * where requestedSchema was being built empty for SYNTHESIZED columns
     * with special character field names, causing messageColumnIO to have 0 children.
     */
    @Test
    public void testColumnPushdownWithHyphenatedFields()
    {
        String tableName = "test_pushdown_hyphenated";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (" +
                    "id INT, " +
                    "data ROW(\"field-one\" VARCHAR, \"field-two\" INT, \"field-three\" DOUBLE)" +
                    ")");

            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(1, ROW('a', 10, 1.1)), " +
                    "(2, ROW('b', 20, 2.2)), " +
                    "(3, ROW('c', 30, 3.3))", 3);

            // Test SYNTHESIZED path - selective subfield read
            assertQuery(
                    "SELECT data.\"field-two\" FROM " + tableName + " WHERE id = 2",
                    "VALUES 20");

            // Test with filter on hyphenated field
            assertQuery(
                    "SELECT id FROM " + tableName + " WHERE data.\"field-one\" = 'b'",
                    "VALUES 2");

            // Test regular path - full struct read via subfields
            assertQuery(
                    "SELECT data.\"field-one\", data.\"field-two\", data.\"field-three\" FROM " + tableName,
                    "VALUES ('a', 10, 1.1), ('b', 20, 2.2), ('c', 30, 3.3)");

            // Test SELECT * - verify all fields
            assertQuery(
                    "SELECT id, data.\"field-one\", data.\"field-two\", data.\"field-three\" FROM " + tableName,
                    "VALUES (1, 'a', 10, 1.1), (2, 'b', 20, 2.2), (3, 'c', 30, 3.3)");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    /**
     * Test backward compatibility - verify non-hyphenated fields still work
     * and mixed structs with both normal and hyphenated fields work correctly.
     */
    @Test
    public void testMixedHyphenatedAndNormalFieldNames()
    {
        String tableName = "test_mixed_fields";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (" +
                    "id INT, " +
                    "mixed ROW(normal_field VARCHAR, \"hyphenated-field\" VARCHAR, another_normal INT)" +
                    ")");

            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(1, ROW('normal1', 'hyphenated1', 123)), " +
                    "(2, ROW('normal2', 'hyphenated2', 456))", 2);

            // Test SYNTHESIZED path for both normal and hyphenated fields
            assertQuery(
                    "SELECT mixed.normal_field, mixed.\"hyphenated-field\", mixed.another_normal FROM " + tableName,
                    "VALUES ('normal1', 'hyphenated1', 123), ('normal2', 'hyphenated2', 456)");

            // Test regular path - full struct via subfields
            assertQuery(
                    "SELECT mixed.normal_field, mixed.\"hyphenated-field\", mixed.another_normal FROM " + tableName,
                    "VALUES ('normal1', 'hyphenated1', 123), ('normal2', 'hyphenated2', 456)");

            // Test SELECT * - verify all fields
            assertQuery(
                    "SELECT id, mixed.normal_field, mixed.\"hyphenated-field\", mixed.another_normal FROM " + tableName,
                    "VALUES (1, 'normal1', 'hyphenated1', 123), (2, 'normal2', 'hyphenated2', 456)");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    /**
     * End-to-end test that writes to an Iceberg table with hyphenated struct fields
     * and reads it back, verifying the complete write→read cycle works correctly.
     * This test validates the full integration of:
     * 1. PrimitiveTypeMapBuilder.makeCompatibleName() for writing hex-encoded field names
     * 2. ColumnIOConverter.constructField() for reading with makeCompatibleName()
     * 3. IcebergPageSourceProvider.getColumnType() for SYNTHESIZED column handling
     */
    @Test
    public void testEndToEndWriteReadWithHyphenatedStructFields()
    {
        String tableName = "test_e2e_hyphenated_struct";
        try {
            // Create table with multiple hyphenated fields in struct
            assertUpdate("CREATE TABLE " + tableName + " (" +
                    "id INT, " +
                    "application ROW(" +
                    "  \"aws-region\" VARCHAR, " +
                    "  \"user-id\" INT, " +
                    "  \"data-center\" VARCHAR, " +
                    "  \"zone-id\" INT, " +
                    "  normal_field VARCHAR" +
                    ")" +
                    ")");

            // Write data - tests PrimitiveTypeMapBuilder.makeCompatibleName()
            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(1, ROW('us-west-2', 1001, 'dc-west-01', 100, 'normal1')), " +
                    "(2, ROW('eu-central-1', 1002, 'dc-eu-01', 200, 'normal2')), " +
                    "(3, ROW('ap-south-1', 1003, 'dc-ap-01', 300, 'normal3'))", 3);

            // Read back - tests ColumnIOConverter.constructField() with makeCompatibleName()
            // Test individual field access (SYNTHESIZED path)
            assertQuery(
                    "SELECT id, application.\"aws-region\", application.\"user-id\", application.\"data-center\" FROM " + tableName,
                    "VALUES (1, 'us-west-2', 1001, 'dc-west-01'), " +
                    "(2, 'eu-central-1', 1002, 'dc-eu-01'), " +
                    "(3, 'ap-south-1', 1003, 'dc-ap-01')");

            // Test all fields including normal field
            assertQuery(
                    "SELECT application.\"aws-region\", application.\"user-id\", application.\"data-center\", " +
                    "application.\"zone-id\", application.normal_field FROM " + tableName,
                    "VALUES ('us-west-2', 1001, 'dc-west-01', 100, 'normal1'), " +
                    "('eu-central-1', 1002, 'dc-eu-01', 200, 'normal2'), " +
                    "('ap-south-1', 1003, 'dc-ap-01', 300, 'normal3')");

            // Test with WHERE clause on hyphenated field
            assertQuery(
                    "SELECT id, application.\"aws-region\" FROM " + tableName + " WHERE application.\"user-id\" = 1002",
                    "VALUES (2, 'eu-central-1')");

            // Test with ORDER BY on hyphenated field
            assertQuery(
                    "SELECT id, application.\"zone-id\" FROM " + tableName + " ORDER BY application.\"zone-id\" DESC",
                    "VALUES (3, 300), (2, 200), (1, 100)");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    /**
     * Test top level hyphenated column names still work correctly.
     * Top level columns use field ID based lookup so they were never broken.
     * This test ensures our fix doesn't regress this behavior.
     */
    @Test
    public void testTopLevelHyphenatedColumnName()
    {
        String tableName = "test_toplevel_hyphenated";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (" +
                    "id INT, " +
                    "\"aws-region\" VARCHAR, " +
                    "\"data-center\" VARCHAR" +
                    ")");

            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(1, 'us-east-1', 'dc-01'), " +
                    "(2, 'eu-west-1', 'dc-02')", 2);

            // Top level hyphenated columns use field ID lookup - should always work
            assertQuery(
                    "SELECT id, \"aws-region\", \"data-center\" FROM " + tableName,
                    "VALUES (1, 'us-east-1', 'dc-01'), (2, 'eu-west-1', 'dc-02')");

            // SELECT *
            assertQuery(
                    "SELECT * FROM " + tableName,
                    "VALUES (1, 'us-east-1', 'dc-01'), (2, 'eu-west-1', 'dc-02')");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }
}
