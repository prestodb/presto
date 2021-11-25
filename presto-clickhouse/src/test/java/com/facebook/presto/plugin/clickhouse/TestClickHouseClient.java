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
package com.facebook.presto.plugin.clickhouse;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.plugin.clickhouse.TestingClickHouseTypeHandle.JDBC_BIGINT;
import static com.facebook.presto.plugin.clickhouse.TestingClickHouseTypeHandle.JDBC_DATE;
import static com.facebook.presto.plugin.clickhouse.TestingClickHouseTypeHandle.JDBC_DOUBLE;
import static com.facebook.presto.plugin.clickhouse.TestingClickHouseTypeHandle.JDBC_REAL;
import static com.facebook.presto.plugin.clickhouse.TestingClickHouseTypeHandle.JDBC_STRING;
import static com.facebook.presto.plugin.clickhouse.TestingClickHouseTypeHandle.JDBC_VARCHAR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.plugin.clickhouse.TestingDatabase.CONNECTOR_ID;
import static java.util.Collections.emptyMap;
import static java.util.Locale.ENGLISH;
import static java.util.UUID.randomUUID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test
public class TestClickHouseClient {

    private static final ConnectorSession session = testSessionBuilder().build().toConnectorSession();

    private TestingDatabase database;
    private String catalogName;
    private ClickHouseClient clickHouseClient;

    @BeforeClass
    public void setUp()
            throws Exception {
        database = new TestingDatabase();
        catalogName = database.getConnection().getCatalog();
        clickHouseClient = database.getClickHouseClient();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception {
        database.close();
    }

    @Test
    public void testMetadata() {
        ClickHouseIdentity identity = ClickHouseIdentity.from(session);
        assertTrue(clickHouseClient.getSchemaNames(identity).containsAll(ImmutableSet.of("example", "tpch")));
        assertEquals(clickHouseClient.getTableNames(identity, Optional.of("example")), ImmutableList.of(
                new SchemaTableName("example", "numbers"),
                new SchemaTableName("example", "view_source")));
                //new SchemaTableName("example", "view")
        assertEquals(clickHouseClient.getTableNames(identity, Optional.of("tpch")), ImmutableList.of(
                new SchemaTableName("tpch", "lineitem"),
                new SchemaTableName("tpch", "orders")));

        SchemaTableName schemaTableName = new SchemaTableName("example", "numbers");
        ClickHouseTableHandle table = clickHouseClient.getTableHandle(identity, schemaTableName);
        assertNotNull(table, "table is null");
        //assertEquals(table.getCatalogName(), catalogName.toUpperCase(ENGLISH));
        assertEquals(table.getSchemaName(), "example");
        assertEquals(table.getTableName(), "numbers");
        assertEquals(table.getSchemaTableName(), schemaTableName);
        assertEquals(clickHouseClient.getColumns(session, table), ImmutableList.of( //  JDBC_STRING  VARBINARY
                new ClickHouseColumnHandle(CONNECTOR_ID, "text",JDBC_VARCHAR , createVarcharType(32), true),
                new ClickHouseColumnHandle(CONNECTOR_ID, "text_short", JDBC_VARCHAR, createVarcharType(32), true),
                new ClickHouseColumnHandle(CONNECTOR_ID, "value", JDBC_BIGINT, BIGINT, true)));
    }

    @Test
    public void testMetadataWithSchemaPattern()
    {
        SchemaTableName schemaTableName = new SchemaTableName("exa_ple", "num_ers");
        ClickHouseTableHandle table = clickHouseClient.getTableHandle(ClickHouseIdentity.from(session), schemaTableName);
        assertNotNull(table, "table is null");
        assertEquals(clickHouseClient.getColumns(session, table), ImmutableList.of(
                new ClickHouseColumnHandle(CONNECTOR_ID, "te_t", JDBC_VARCHAR, VARCHAR, true),
                new ClickHouseColumnHandle(CONNECTOR_ID, "VA%UE", JDBC_BIGINT, BIGINT, true)));
    }

    @Test
    public void testMetadataWithFloatAndDoubleCol()
    {
        SchemaTableName schemaTableName = new SchemaTableName("exa_ple", "table_with_float_col");
        ClickHouseTableHandle table = clickHouseClient.getTableHandle(ClickHouseIdentity.from(session), schemaTableName);
        assertNotNull(table, "table is null");
        assertEquals(clickHouseClient.getColumns(session, table), ImmutableList.of(
                new ClickHouseColumnHandle(CONNECTOR_ID, "col1", JDBC_BIGINT, BIGINT, true),
                new ClickHouseColumnHandle(CONNECTOR_ID, "col2", JDBC_DOUBLE, DOUBLE, true),
                new ClickHouseColumnHandle(CONNECTOR_ID, "col3", JDBC_DOUBLE, DOUBLE, true),
                new ClickHouseColumnHandle(CONNECTOR_ID, "col4", JDBC_REAL, REAL, true)));
    }

    @Test(enabled = false)
    public void testAlterColumns()
    {
        String tableName = randomUUID().toString().toUpperCase(ENGLISH);
        SchemaTableName schemaTableName = new SchemaTableName("schema_for_create_table_tests", tableName);
        List<ColumnMetadata> expectedColumns = ImmutableList.of(
                new ColumnMetadata("columnA", BigintType.BIGINT, null, null, false));

        clickHouseClient.createTable(session, new ConnectorTableMetadata(schemaTableName, expectedColumns));

        ClickHouseTableHandle tableHandle = clickHouseClient.getTableHandle(ClickHouseIdentity.from(session), schemaTableName);

        try {
            assertEquals(tableHandle.getTableName(), tableName);
            assertEquals(clickHouseClient.getColumns(session, tableHandle), ImmutableList.of(
                    new ClickHouseColumnHandle(CONNECTOR_ID, "COLUMNA", JDBC_BIGINT, BigintType.BIGINT, true)));

            clickHouseClient.addColumn(ClickHouseIdentity.from(session), tableHandle, new ColumnMetadata("columnB", DoubleType.DOUBLE, null, null, false));
            assertEquals(clickHouseClient.getColumns(session, tableHandle), ImmutableList.of(
                    new ClickHouseColumnHandle(CONNECTOR_ID, "COLUMNA", JDBC_BIGINT, BigintType.BIGINT, true),
                    new ClickHouseColumnHandle(CONNECTOR_ID, "COLUMNB", JDBC_DOUBLE, DoubleType.DOUBLE, true)));

            clickHouseClient.dropColumn(ClickHouseIdentity.from(session), tableHandle, new ClickHouseColumnHandle(CONNECTOR_ID, "COLUMNB", JDBC_BIGINT, BigintType.BIGINT, true));
            assertEquals(clickHouseClient.getColumns(session, tableHandle), ImmutableList.of(
                    new ClickHouseColumnHandle(CONNECTOR_ID, "COLUMNA", JDBC_BIGINT, BigintType.BIGINT, true)));
        }
        finally {
            clickHouseClient.dropTable(ClickHouseIdentity.from(session), tableHandle);
        }
    }

}
