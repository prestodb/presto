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
package com.facebook.presto.plugin.clp;

import com.facebook.presto.plugin.clp.metadata.ClpMetadataProvider;
import com.facebook.presto.plugin.clp.metadata.ClpYamlMetadataProvider;
import com.facebook.presto.plugin.clp.split.ClpPinotSplitProvider;
import com.facebook.presto.plugin.clp.split.ClpSplitProvider;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.plugin.clp.ClpConfig.MetadataProviderType.YAML;
import static com.facebook.presto.plugin.clp.ClpMetadata.DEFAULT_SCHEMA_NAME;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;

public class TestClpYamlMetadata
{
    private static final String PINOT_BROKER_URL = "http://localhost:8099";
    private static final String TABLE_NAME = "cockroachdb";
    private static final String SCHEMA1_NAME = "schema1";
    private static final String SCHEMA2_NAME = "schema2";
    private static final String ORDERS_TABLE_NAME = "orders";
    private static final String USERS_TABLE_NAME = "users";
    private ClpMetadata metadata;
    private ClpSplitProvider clpSplitProvider;

    @BeforeTest
    public void setUp() throws Exception
    {
        // Load test resources from classpath
        // ClpYamlMetadataProvider now supports relative paths, so we can use the resource file directly
        java.net.URL tablesSchemaResource = getClass().getClassLoader().getResource("test-tables-schema.yaml");

        if (tablesSchemaResource == null) {
            throw new IllegalStateException("test-tables-schema.yaml not found in test resources");
        }

        // Get the absolute path to test-tables-schema.yaml
        // Relative paths in the YAML will be resolved relative to this file's parent directory
        String tablesSchemaPath = java.nio.file.Paths.get(tablesSchemaResource.toURI()).toString();

        ClpConfig config = new ClpConfig()
                .setPolymorphicTypeEnabled(true)
                .setMetadataDbUrl(PINOT_BROKER_URL)
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(tablesSchemaPath);
        ClpMetadataProvider metadataProvider = new ClpYamlMetadataProvider(config);
        metadata = new ClpMetadata(config, metadataProvider);
        clpSplitProvider = new ClpPinotSplitProvider(config);
    }

    @Test
    public void testListSchemaNames()
    {
        List<String> schemaNames = metadata.listSchemaNames(SESSION);
        assertEquals(new HashSet<>(schemaNames), ImmutableSet.of(DEFAULT_SCHEMA_NAME, SCHEMA1_NAME, SCHEMA2_NAME));
    }

    @Test
    public void testListTables()
    {
        // When no schema is specified, listTables defaults to DEFAULT_SCHEMA_NAME
        ImmutableSet<SchemaTableName> defaultTables = ImmutableSet.of(
                new SchemaTableName(DEFAULT_SCHEMA_NAME, TABLE_NAME));
        assertEquals(new HashSet<>(metadata.listTables(SESSION, Optional.empty())), defaultTables);
    }

    @Test
    public void testListTablesForSpecificSchema()
    {
        // Test listing tables for schema1
        ImmutableSet<SchemaTableName> schema1Tables = ImmutableSet.of(
                new SchemaTableName(SCHEMA1_NAME, ORDERS_TABLE_NAME),
                new SchemaTableName(SCHEMA1_NAME, USERS_TABLE_NAME));
        assertEquals(new HashSet<>(metadata.listTables(SESSION, Optional.of(SCHEMA1_NAME))), schema1Tables);

        // Test listing tables for schema2
        ImmutableSet<SchemaTableName> schema2Tables = ImmutableSet.of(
                new SchemaTableName(SCHEMA2_NAME, ORDERS_TABLE_NAME));
        assertEquals(new HashSet<>(metadata.listTables(SESSION, Optional.of(SCHEMA2_NAME))), schema2Tables);

        // Test listing tables for default schema
        ImmutableSet<SchemaTableName> defaultTables = ImmutableSet.of(
                new SchemaTableName(DEFAULT_SCHEMA_NAME, TABLE_NAME));
        assertEquals(new HashSet<>(metadata.listTables(SESSION, Optional.of(DEFAULT_SCHEMA_NAME))), defaultTables);
    }

    @Test
    public void testListSplits()
    {
        ClpTableLayoutHandle layoutHandle = new ClpTableLayoutHandle(
                new ClpTableHandle(new SchemaTableName(DEFAULT_SCHEMA_NAME, TABLE_NAME), ""),
                Optional.empty(),
                Optional.empty());
        List<ClpSplit> result = clpSplitProvider.listSplits(layoutHandle);
        System.out.println("Hello world");
    }

    @Test
    public void testGetTableMetadata()
    {
        ClpTableHandle clpTableHandle = (ClpTableHandle) metadata.getTableHandle(SESSION, new SchemaTableName(DEFAULT_SCHEMA_NAME, TABLE_NAME));
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, clpTableHandle);
//        ImmutableSet<ColumnMetadata> columnMetadata = ImmutableSet.<ColumnMetadata>builder()
//                .add(ColumnMetadata.builder()
//                        .setName("a_bigint")
//                        .setType(BIGINT)
//                        .setNullable(true)
//                        .build())
//                .add(ColumnMetadata.builder()
//                        .setName("a_varchar")
//                        .setType(VARCHAR)
//                        .setNullable(true)
//                        .build())
//                .add(ColumnMetadata.builder()
//                        .setName("b_double")
//                        .setType(DOUBLE)
//                        .setNullable(true)
//                        .build())
//                .add(ColumnMetadata.builder()
//                        .setName("b_varchar")
//                        .setType(VARCHAR)
//                        .setNullable(true)
//                        .build())
//                .add(ColumnMetadata.builder()
//                        .setName("c")
//                        .setType(RowType.from(ImmutableList.of(
//                                RowType.field("d", BOOLEAN),
//                                RowType.field("e", VARCHAR))))
//                        .setNullable(true)
//                        .build())
//                .add(ColumnMetadata.builder()
//                        .setName("f")
//                        .setType(RowType.from(ImmutableList.of(
//                                RowType.field("g",
//                                        RowType.from(ImmutableList.of(
//                                                RowType.field("h", new ArrayType(VARCHAR))))))))
//                        .setNullable(true)
//                        .build())
//                .build();
//        assertEquals(columnMetadata, ImmutableSet.copyOf(tableMetadata.getColumns()));
        ImmutableSet<ColumnMetadata> actual = ImmutableSet.copyOf(tableMetadata.getColumns());
        System.out.println("Hello world");
    }

    @Test
    public void testGetTableHandleForDuplicateTableNames()
    {
        // Test that we can get distinct table handles for tables with the same name in different schemas
        ClpTableHandle schema1OrdersHandle = (ClpTableHandle) metadata.getTableHandle(SESSION, new SchemaTableName(SCHEMA1_NAME, ORDERS_TABLE_NAME));
        ClpTableHandle schema2OrdersHandle = (ClpTableHandle) metadata.getTableHandle(SESSION, new SchemaTableName(SCHEMA2_NAME, ORDERS_TABLE_NAME));

        // Verify both handles are not null
        assertEquals(schema1OrdersHandle != null, true);
        assertEquals(schema2OrdersHandle != null, true);

        // Verify the schema names are correctly set
        assertEquals(schema1OrdersHandle.getSchemaTableName().getSchemaName(), SCHEMA1_NAME);
        assertEquals(schema2OrdersHandle.getSchemaTableName().getSchemaName(), SCHEMA2_NAME);

        // Verify the table names are the same
        assertEquals(schema1OrdersHandle.getSchemaTableName().getTableName(), ORDERS_TABLE_NAME);
        assertEquals(schema2OrdersHandle.getSchemaTableName().getTableName(), ORDERS_TABLE_NAME);
    }

    @Test
    public void testGetTableMetadataForDuplicateTableNames()
    {
        // Get table handles for orders tables in both schemas
        ClpTableHandle schema1OrdersHandle = (ClpTableHandle) metadata.getTableHandle(SESSION, new SchemaTableName(SCHEMA1_NAME, ORDERS_TABLE_NAME));
        ClpTableHandle schema2OrdersHandle = (ClpTableHandle) metadata.getTableHandle(SESSION, new SchemaTableName(SCHEMA2_NAME, ORDERS_TABLE_NAME));

        // Get metadata for both tables
        ConnectorTableMetadata schema1Metadata = metadata.getTableMetadata(SESSION, schema1OrdersHandle);
        ConnectorTableMetadata schema2Metadata = metadata.getTableMetadata(SESSION, schema2OrdersHandle);

        // Extract column names from both tables
        ImmutableSet<String> schema1Columns = schema1Metadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(ImmutableSet.toImmutableSet());
        ImmutableSet<String> schema2Columns = schema2Metadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(ImmutableSet.toImmutableSet());

        // Verify schema1.orders has the expected columns (from test-orders-schema1.yaml)
        ImmutableSet<String> expectedSchema1Columns = ImmutableSet.of(
                "order_id", "customer_id", "product_name", "quantity", "price");
        assertEquals(schema1Columns, expectedSchema1Columns);

        // Verify schema2.orders has the expected columns (from test-orders-schema2.yaml)
        ImmutableSet<String> expectedSchema2Columns = ImmutableSet.of(
                "order_id", "vendor_id", "item_description", "total_amount", "is_paid", "shipping_address");
        assertEquals(schema2Columns, expectedSchema2Columns);

        // Verify that the two tables have different schemas (different columns)
        assertEquals(schema1Columns.equals(schema2Columns), false);
    }

    @Test
    public void testGetTableMetadataForAllSchemas()
    {
        // Test default.cockroachdb
        ClpTableHandle defaultTableHandle = (ClpTableHandle) metadata.getTableHandle(SESSION, new SchemaTableName(DEFAULT_SCHEMA_NAME, TABLE_NAME));
        ConnectorTableMetadata defaultMetadata = metadata.getTableMetadata(SESSION, defaultTableHandle);
        assertEquals(defaultMetadata != null, true);
        assertEquals(defaultMetadata.getTable().getSchemaName(), DEFAULT_SCHEMA_NAME);
        assertEquals(defaultMetadata.getTable().getTableName(), TABLE_NAME);

        // Test schema1.orders
        ClpTableHandle schema1OrdersHandle = (ClpTableHandle) metadata.getTableHandle(SESSION, new SchemaTableName(SCHEMA1_NAME, ORDERS_TABLE_NAME));
        ConnectorTableMetadata schema1OrdersMetadata = metadata.getTableMetadata(SESSION, schema1OrdersHandle);
        assertEquals(schema1OrdersMetadata != null, true);
        assertEquals(schema1OrdersMetadata.getTable().getSchemaName(), SCHEMA1_NAME);
        assertEquals(schema1OrdersMetadata.getTable().getTableName(), ORDERS_TABLE_NAME);

        // Test schema1.users
        ClpTableHandle schema1UsersHandle = (ClpTableHandle) metadata.getTableHandle(SESSION, new SchemaTableName(SCHEMA1_NAME, USERS_TABLE_NAME));
        ConnectorTableMetadata schema1UsersMetadata = metadata.getTableMetadata(SESSION, schema1UsersHandle);
        assertEquals(schema1UsersMetadata != null, true);
        assertEquals(schema1UsersMetadata.getTable().getSchemaName(), SCHEMA1_NAME);
        assertEquals(schema1UsersMetadata.getTable().getTableName(), USERS_TABLE_NAME);

        // Test schema2.orders
        ClpTableHandle schema2OrdersHandle = (ClpTableHandle) metadata.getTableHandle(SESSION, new SchemaTableName(SCHEMA2_NAME, ORDERS_TABLE_NAME));
        ConnectorTableMetadata schema2OrdersMetadata = metadata.getTableMetadata(SESSION, schema2OrdersHandle);
        assertEquals(schema2OrdersMetadata != null, true);
        assertEquals(schema2OrdersMetadata.getTable().getSchemaName(), SCHEMA2_NAME);
        assertEquals(schema2OrdersMetadata.getTable().getTableName(), ORDERS_TABLE_NAME);
    }
}
