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
package com.facebook.presto.plugin.mysql;

import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcRecordSet;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test
public class TestJdbcCaseSensitive
{
    private TestingMysqlDatabase testingMysqlDatabase;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        this.testingMysqlDatabase = new TestingMysqlDatabase("testuser", "testpass");
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        testingMysqlDatabase.close();
    }

    @Test
    public void testMetadata()
    {
        JdbcClient jdbcClient = testingMysqlDatabase.getJdbcClient();

        assertTrue(jdbcClient.getSchemaNames().containsAll(ImmutableSet.of("example", "examplecamelcase")));
        assertEquals(jdbcClient.getTableNames("example"), ImmutableList.of(
                new SchemaTableName("example", "numbers")));
        assertEquals(jdbcClient.getTableNames("examplecamelcase"), ImmutableList.of(
                new SchemaTableName("examplecamelcase", "camelcasenumbers")));

        SchemaTableName schemaTableName = new SchemaTableName("example", "numbers");
        JdbcTableHandle table = jdbcClient.getTableHandle(schemaTableName);
        assertNotNull(table, "table is null");
        assertEquals(table.getCatalogName(), "example");
        assertEquals(table.getTableName(), "numbers");
        assertEquals(table.getSchemaTableName(), schemaTableName);
        assertEquals(jdbcClient.getColumns(table), ImmutableList.of(
                new JdbcColumnHandle(TestingMysqlDatabase.CONNECTOR_ID, "text", createVarcharType(255)),
                new JdbcColumnHandle(TestingMysqlDatabase.CONNECTOR_ID, "text_short", createVarcharType(32)),
                new JdbcColumnHandle(TestingMysqlDatabase.CONNECTOR_ID, "value", BIGINT)));

        schemaTableName = new SchemaTableName("examplecamelcase", "camelcasenumbers");
        table = jdbcClient.getTableHandle(schemaTableName);
        assertNotNull(table, "table is null");
        assertEquals(table.getCatalogName(), "exampleCamelCase");
        assertEquals(table.getTableName(), "camelCaseNumbers");
        assertEquals(table.getSchemaTableName(), schemaTableName);
        assertEquals(jdbcClient.getColumns(table), ImmutableList.of(
                new JdbcColumnHandle(TestingMysqlDatabase.CONNECTOR_ID, "text", createVarcharType(255)),
                new JdbcColumnHandle(TestingMysqlDatabase.CONNECTOR_ID, "text_short", createVarcharType(32)),
                new JdbcColumnHandle(TestingMysqlDatabase.CONNECTOR_ID, "value", BIGINT)));
    }

    @Test
    public void testCorrectTableNames()
            throws Exception
    {
        JdbcSplit split = testingMysqlDatabase.getSplit("examplecamelcase", "camelcasenumbers");
        Map<String, JdbcColumnHandle> columnHandles = testingMysqlDatabase.getColumnHandles("example", "numbers");
        JdbcClient jdbcClient = testingMysqlDatabase.getJdbcClient();

        RecordSet recordSet = new JdbcRecordSet(jdbcClient, split, ImmutableList.of(
                columnHandles.get("text"),
                columnHandles.get("text_short"),
                columnHandles.get("value")));

        try (RecordCursor cursor = recordSet.cursor()) {
            assertEquals(cursor.getType(0), createVarcharType(255));
            assertEquals(cursor.getType(1), createVarcharType(32));
            assertEquals(cursor.getType(2), BIGINT);

            Map<String, Long> data = new LinkedHashMap<>();
            while (cursor.advanceNextPosition()) {
                data.put(cursor.getSlice(0).toStringUtf8(), cursor.getLong(2));
                assertEquals(cursor.getSlice(0), cursor.getSlice(1));
                assertFalse(cursor.isNull(0));
                assertFalse(cursor.isNull(1));
                assertFalse(cursor.isNull(2));
            }

            assertEquals(data, ImmutableMap.<String, Long>builder()
                    .put("one", 1L)
                    .put("two", 2L)
                    .put("three", 3L)
                    .put("ten", 10L)
                    .put("eleven", 11L)
                    .put("twelve", 12L)
                    .build());
        }
    }
}
