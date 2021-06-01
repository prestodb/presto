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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.plugin.jdbc.TestingDatabase.CONNECTOR_ID;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_BIGINT;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_DATE;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_DOUBLE;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_REAL;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_VARCHAR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.Collections.emptyMap;
import static java.util.Locale.ENGLISH;
import static java.util.UUID.randomUUID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test
public class TestJdbcClient
{
    private static final ConnectorSession session = testSessionBuilder().build().toConnectorSession();

    private TestingDatabase database;
    private String catalogName;
    private JdbcClient jdbcClient;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        database = new TestingDatabase();
        catalogName = database.getConnection().getCatalog();
        jdbcClient = database.getJdbcClient();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        database.close();
    }

    @Test
    public void testMetadata()
    {
        JdbcIdentity identity = JdbcIdentity.from(session);
        assertTrue(jdbcClient.getSchemaNames(identity).containsAll(ImmutableSet.of("example", "tpch")));
        assertEquals(jdbcClient.getTableNames(identity, Optional.of("example")), ImmutableList.of(
                new SchemaTableName("example", "numbers"),
                new SchemaTableName("example", "view_source"),
                new SchemaTableName("example", "view")));
        assertEquals(jdbcClient.getTableNames(identity, Optional.of("tpch")), ImmutableList.of(
                new SchemaTableName("tpch", "lineitem"),
                new SchemaTableName("tpch", "orders")));

        SchemaTableName schemaTableName = new SchemaTableName("example", "numbers");
        JdbcTableHandle table = jdbcClient.getTableHandle(identity, schemaTableName);
        assertNotNull(table, "table is null");
        assertEquals(table.getCatalogName(), catalogName.toUpperCase(ENGLISH));
        assertEquals(table.getSchemaName(), "EXAMPLE");
        assertEquals(table.getTableName(), "NUMBERS");
        assertEquals(table.getSchemaTableName(), schemaTableName);
        assertEquals(jdbcClient.getColumns(session, table), ImmutableList.of(
                new JdbcColumnHandle(CONNECTOR_ID, "TEXT", JDBC_VARCHAR, VARCHAR, true, Optional.empty()),
                new JdbcColumnHandle(CONNECTOR_ID, "TEXT_SHORT", JDBC_VARCHAR, createVarcharType(32), true, Optional.empty()),
                new JdbcColumnHandle(CONNECTOR_ID, "VALUE", JDBC_BIGINT, BIGINT, true, Optional.empty())));
    }

    @Test
    public void testMetadataWithSchemaPattern()
    {
        SchemaTableName schemaTableName = new SchemaTableName("exa_ple", "num_ers");
        JdbcTableHandle table = jdbcClient.getTableHandle(JdbcIdentity.from(session), schemaTableName);
        assertNotNull(table, "table is null");
        assertEquals(jdbcClient.getColumns(session, table), ImmutableList.of(
                new JdbcColumnHandle(CONNECTOR_ID, "TE_T", JDBC_VARCHAR, VARCHAR, true, Optional.empty()),
                new JdbcColumnHandle(CONNECTOR_ID, "VA%UE", JDBC_BIGINT, BIGINT, true, Optional.empty())));
    }

    @Test
    public void testMetadataWithFloatAndDoubleCol()
    {
        SchemaTableName schemaTableName = new SchemaTableName("exa_ple", "table_with_float_col");
        JdbcTableHandle table = jdbcClient.getTableHandle(JdbcIdentity.from(session), schemaTableName);
        assertNotNull(table, "table is null");
        assertEquals(jdbcClient.getColumns(session, table), ImmutableList.of(
                new JdbcColumnHandle(CONNECTOR_ID, "COL1", JDBC_BIGINT, BIGINT, true, Optional.empty()),
                new JdbcColumnHandle(CONNECTOR_ID, "COL2", JDBC_DOUBLE, DOUBLE, true, Optional.empty()),
                new JdbcColumnHandle(CONNECTOR_ID, "COL3", JDBC_DOUBLE, DOUBLE, true, Optional.empty()),
                new JdbcColumnHandle(CONNECTOR_ID, "COL4", JDBC_REAL, REAL, true, Optional.empty())));
    }

    @Test
    public void testCreateWithNullableColumns()
    {
        String tableName = randomUUID().toString().toUpperCase(ENGLISH);
        SchemaTableName schemaTableName = new SchemaTableName("schema_for_create_table_tests", tableName);
        List<ColumnMetadata> expectedColumns = ImmutableList.of(
                new ColumnMetadata("columnA", BigintType.BIGINT, null, null, false),
                new ColumnMetadata("columnB", BigintType.BIGINT, true, null, null, false, emptyMap()),
                new ColumnMetadata("columnC", BigintType.BIGINT, false, null, null, false, emptyMap()),
                new ColumnMetadata("columnD", DateType.DATE, false, null, null, false, emptyMap()));

        jdbcClient.createTable(session, new ConnectorTableMetadata(schemaTableName, expectedColumns));

        JdbcTableHandle tableHandle = jdbcClient.getTableHandle(JdbcIdentity.from(session), schemaTableName);

        try {
            assertEquals(tableHandle.getTableName(), tableName);
            assertEquals(jdbcClient.getColumns(session, tableHandle), ImmutableList.of(
                    new JdbcColumnHandle(CONNECTOR_ID, "COLUMNA", JDBC_BIGINT, BigintType.BIGINT, true, Optional.empty()),
                    new JdbcColumnHandle(CONNECTOR_ID, "COLUMNB", JDBC_BIGINT, BigintType.BIGINT, true, Optional.empty()),
                    new JdbcColumnHandle(CONNECTOR_ID, "COLUMNC", JDBC_BIGINT, BigintType.BIGINT, false, Optional.empty()),
                    new JdbcColumnHandle(CONNECTOR_ID, "COLUMND", JDBC_DATE, DateType.DATE, false, Optional.empty())));
        }
        finally {
            jdbcClient.dropTable(JdbcIdentity.from(session), tableHandle);
        }
    }

    // disabled due to https://github.com/prestodb/presto/issues/16081
    @Test(enabled = false)
    public void testAlterColumns()
    {
        String tableName = randomUUID().toString().toUpperCase(ENGLISH);
        SchemaTableName schemaTableName = new SchemaTableName("schema_for_create_table_tests", tableName);
        List<ColumnMetadata> expectedColumns = ImmutableList.of(
                new ColumnMetadata("columnA", BigintType.BIGINT, null, null, false));

        jdbcClient.createTable(session, new ConnectorTableMetadata(schemaTableName, expectedColumns));

        JdbcTableHandle tableHandle = jdbcClient.getTableHandle(JdbcIdentity.from(session), schemaTableName);

        try {
            assertEquals(tableHandle.getTableName(), tableName);
            assertEquals(jdbcClient.getColumns(session, tableHandle), ImmutableList.of(
                    new JdbcColumnHandle(CONNECTOR_ID, "COLUMNA", JDBC_BIGINT, BigintType.BIGINT, true, Optional.empty())));

            jdbcClient.addColumn(JdbcIdentity.from(session), tableHandle, new ColumnMetadata("columnB", DoubleType.DOUBLE, null, null, false));
            assertEquals(jdbcClient.getColumns(session, tableHandle), ImmutableList.of(
                    new JdbcColumnHandle(CONNECTOR_ID, "COLUMNA", JDBC_BIGINT, BigintType.BIGINT, true, Optional.empty()),
                    new JdbcColumnHandle(CONNECTOR_ID, "COLUMNB", JDBC_DOUBLE, DoubleType.DOUBLE, true, Optional.empty())));

            jdbcClient.dropColumn(JdbcIdentity.from(session), tableHandle, new JdbcColumnHandle(CONNECTOR_ID, "COLUMNB", JDBC_BIGINT, BigintType.BIGINT, true, Optional.empty()));
            assertEquals(jdbcClient.getColumns(session, tableHandle), ImmutableList.of(
                    new JdbcColumnHandle(CONNECTOR_ID, "COLUMNA", JDBC_BIGINT, BigintType.BIGINT, true, Optional.empty())));
        }
        finally {
            jdbcClient.dropTable(JdbcIdentity.from(session), tableHandle);
        }
    }
}
