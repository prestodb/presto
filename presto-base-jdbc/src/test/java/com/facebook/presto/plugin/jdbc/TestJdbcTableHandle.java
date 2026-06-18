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

import com.facebook.airlift.testing.EquivalenceTester;
import com.facebook.presto.spi.SchemaTableName;
import org.testng.annotations.Test;

import static com.facebook.presto.plugin.jdbc.MetadataUtil.TABLE_CODEC;
import static com.facebook.presto.plugin.jdbc.MetadataUtil.assertJsonRoundTrip;

public class TestJdbcTableHandle
{
    @Test
    public void testJsonRoundTrip()
    {
        assertJsonRoundTrip(TABLE_CODEC, new JdbcTableHandle("connectorId", new SchemaTableName("schema", "table"), "jdbcCatalog", "jdbcSchema", "jdbcTable"));
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(
                        new JdbcTableHandle("connectorId", new SchemaTableName("schema", "table"), "jdbcCatalog", "jdbcSchema", "jdbcTable"),
                        new JdbcTableHandle("connectorId", new SchemaTableName("schema", "table"), "jdbcCatalogX", "jdbcSchema", "jdbcTable"),
                        new JdbcTableHandle("connectorId", new SchemaTableName("schema", "table"), "jdbcCatalog", "jdbcSchemaX", "jdbcTable"),
                        new JdbcTableHandle("connectorId", new SchemaTableName("schema", "table"), "jdbcCatalog", "jdbcSchema", "jdbcTableX"))
                .addEquivalentGroup(
                        new JdbcTableHandle("connectorIdX", new SchemaTableName("schema", "table"), "jdbcCatalog", "jdbcSchema", "jdbcTable"),
                        new JdbcTableHandle("connectorIdX", new SchemaTableName("schema", "table"), "jdbcCatalogX", "jdbcSchema", "jdbcTable"),
                        new JdbcTableHandle("connectorIdX", new SchemaTableName("schema", "table"), "jdbcCatalog", "jdbcSchemaX", "jdbcTable"),
                        new JdbcTableHandle("connectorIdX", new SchemaTableName("schema", "table"), "jdbcCatalog", "jdbcSchema", "jdbcTableX"))
                .addEquivalentGroup(
                        new JdbcTableHandle("connectorId", new SchemaTableName("schemaX", "table"), "jdbcCatalog", "jdbcSchema", "jdbcTable"),
                        new JdbcTableHandle("connectorId", new SchemaTableName("schemaX", "table"), "jdbcCatalogX", "jdbcSchema", "jdbcTable"),
                        new JdbcTableHandle("connectorId", new SchemaTableName("schemaX", "table"), "jdbcCatalog", "jdbcSchemaX", "jdbcTable"),
                        new JdbcTableHandle("connectorId", new SchemaTableName("schemaX", "table"), "jdbcCatalog", "jdbcSchema", "jdbcTableX"))
                .check();
    }
}
