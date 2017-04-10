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

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.plugin.jdbc.TestingDatabase.CONNECTOR_ID;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test
public class TestJdbcClient
{
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

    @AfterClass
    public void tearDown()
            throws Exception
    {
        database.close();
    }

    @Test
    public void testMetadata()
            throws Exception
    {
        assertTrue(jdbcClient.getSchemaNames().containsAll(ImmutableSet.of("example", "tpch")));
        assertEquals(jdbcClient.getTableNames("example"), ImmutableList.of(
                new SchemaTableName("example", "numbers"),
                new SchemaTableName("example", "view_source"),
                new SchemaTableName("example", "view")));
        assertEquals(jdbcClient.getTableNames("tpch"), ImmutableList.of(
                new SchemaTableName("tpch", "lineitem"),
                new SchemaTableName("tpch", "orders")));

        SchemaTableName schemaTableName = new SchemaTableName("example", "numbers");
        JdbcTableHandle table = jdbcClient.getTableHandle(schemaTableName);
        assertNotNull(table, "table is null");
        assertEquals(table.getCatalogName(), catalogName.toUpperCase(ENGLISH));
        assertEquals(table.getSchemaName(), "EXAMPLE");
        assertEquals(table.getTableName(), "NUMBERS");
        assertEquals(table.getSchemaTableName(), schemaTableName);
        assertEquals(jdbcClient.getColumns(table), ImmutableList.of(
                new JdbcColumnHandle(CONNECTOR_ID, "TEXT", VARCHAR),
                new JdbcColumnHandle(CONNECTOR_ID, "TEXT_SHORT", createVarcharType(32)),
                new JdbcColumnHandle(CONNECTOR_ID, "VALUE", BIGINT)));
    }

    @Test
    public void testMetadataWithSchemaPattern()
            throws Exception
    {
        SchemaTableName schemaTableName = new SchemaTableName("exa_ple", "num_ers");
        JdbcTableHandle table = jdbcClient.getTableHandle(schemaTableName);
        assertNotNull(table, "table is null");
        assertEquals(jdbcClient.getColumns(table), ImmutableList.of(
                new JdbcColumnHandle(CONNECTOR_ID, "TE_T", VARCHAR),
                new JdbcColumnHandle(CONNECTOR_ID, "VA%UE", BIGINT)));
    }

    @Test
    public void testMetadataWithFloatAndDoubleCol()
            throws Exception
    {
        SchemaTableName schemaTableName = new SchemaTableName("exa_ple", "table_with_float_col");
        JdbcTableHandle table = jdbcClient.getTableHandle(schemaTableName);
        assertNotNull(table, "table is null");
        assertEquals(jdbcClient.getColumns(table), ImmutableList.of(
                new JdbcColumnHandle(CONNECTOR_ID, "COL1", BIGINT),
                new JdbcColumnHandle(CONNECTOR_ID, "COL2", DOUBLE),
                new JdbcColumnHandle(CONNECTOR_ID, "COL3", DOUBLE),
                new JdbcColumnHandle(CONNECTOR_ID, "COL4", REAL)));
    }
}
