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
package com.facebook.presto.connector.unittest;

import com.facebook.presto.connector.meta.RequiredFeatures;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Consumer;

import static com.facebook.presto.connector.meta.ConnectorFeature.CREATE_TABLE;
import static com.facebook.presto.connector.meta.ConnectorFeature.DROP_TABLE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Arrays.stream;
import static org.junit.jupiter.api.Assertions.assertEquals;

public interface BaseMetadataTest
        extends SPITest
{
    default List<String> systemSchemas()
    {
        return ImmutableList.of();
    }

    default List<SchemaTableName> systemTables()
    {
        return ImmutableList.of();
    }

    /*
     * Connectors that add columns containing connector metadata to a table, like Hive,
     * should return a List containing the metadata for all of the expectedColumns and the connector-specific columns added in the appropriate positions.
     */
    default List<ColumnMetadata> extendWithConnectorSpecificColumns(List<ColumnMetadata> expectedColumns)
    {
        return expectedColumns;
    }

    /*
     * Returns a SchemaTableName to be used when the name of the schema is
     * important.
     *
     * Tests that require tables in multiple schemas should use this method. An
     * example would be a test that renames a table from one schema to another.
     *
     * Tests invoking this method must be annotated with
     * @RequiredFeatures({CREATE_SCHEMA, DROP_SCHEMA})
     */
    default SchemaTableName tableInSpecificSchema(String schemaName, String tableName)
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Test
    default void testEmptyMetadata()
            throws Exception
    {
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());

        withMetadata(
                ImmutableList.of(
                        metadata -> assertEquals(metadata.listSchemaNames(session), systemSchemas()),
                        metadata -> assertEquals(metadata.listTables(session, null), systemTables())));
    }

    /*
     * Arguably, this belongs in MetadataTableTest. Unfortunately many
     * connectors don't support CREATE_TABLE, but do support CREATE_TABLE_AS.
     * As a result, MetadataTableTest is written in terms of CREATE_TABLE_AS,
     * and is annotated with @RequiredFeatures({..., CREATE_TABLE_AS, ...) at
     * the class level.
     *
     * Moving this to MetadataTableTest would require either:
     * 1. Annotating every other methos than this as requiring CREATE_TABLE_AS.
     * 2. Adding a way to unrequire previously required dependencies.
     *
     * 1 is cumbersome, 2 adds confusing semantics and surface area for weird
     * bugs. Putting the test here seems like a good-enough solution.
     */
    @Test
    @RequiredFeatures({CREATE_TABLE, DROP_TABLE})
    default void testCreateDropTable()
            throws Exception
    {
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        String tableName = "table";
        SchemaTableName schemaTableName = tableInDefaultSchema(tableName);

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                schemaTableName,
                ImmutableList.of(
                        new ColumnMetadata("bigint_column", BIGINT),
                        new ColumnMetadata("double_column", DOUBLE)),
                getTableProperties());

        withSchemas(session, ImmutableList.of(schemaTableName.getSchemaName()),
                ImmutableList.of(
                        metadata -> metadata.createTable(session, tableMetadata),
                        metadata -> assertEquals(getOnlyElement(metadata.listTables(session, schemaTableName.getSchemaName())), schemaTableName),
                        metadata -> metadata.dropTable(session, metadata.getTableHandle(session, schemaTableName))));
    }

    default SchemaTablePrefix prefixOfSchemaName(SchemaTableName schemaTableName)
    {
        return new SchemaTablePrefix(schemaTableName.getSchemaName());
    }

    default SchemaTablePrefix prefixOf(SchemaTableName schemaTableName)
    {
        return new SchemaTablePrefix(schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }

    default void withMetadata(List<Consumer<ConnectorMetadata>> consumers)
    {
        consumers.forEach(this::withMetadata);
    }

    default List<String> schemaNamesOf(SchemaTableName... schemaTableNames)
    {
        return stream(schemaTableNames)
                .map(SchemaTableName::getSchemaName)
                .collect(toImmutableList());
    }

    default void withSchemas(ConnectorSession session, List<String> schemaNames, List<Consumer<ConnectorMetadata>> consumers)
            throws Exception
    {
        withSchemas(session, schemaNames,
                () -> {
                    consumers.forEach(this::withMetadata);
                    return null;
                });
    }

    default void withTables(ConnectorSession session, List<ConnectorTableMetadata> tables, List<Consumer<ConnectorMetadata>> consumers)
            throws Exception
    {
        withTables(session, tables,
                () -> {
                    consumers.forEach(this::withMetadata);
                    return null;
                });
    }
}
