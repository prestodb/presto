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
package com.facebook.presto.example;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.ColumnType.LONG;
import static com.facebook.presto.spi.ColumnType.STRING;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestExampleMetadata
{
    private static final JsonCodec<Map<String, List<ExampleTable>>> CATALOG_CODEC = mapJsonCodec(String.class, listJsonCodec(ExampleTable.class));
    private static final String CONNECTOR_ID = "TEST";
    private static final ExampleTableHandle NUMBERS_TABLE_HANDLE = new ExampleTableHandle(CONNECTOR_ID, "example", "numbers");
    private ExampleMetadata metadata;
    private URI metadataUri;


    @BeforeMethod
    public void setUp()
            throws Exception
    {
        URL metadataUrl = Resources.getResource(TestExampleClient.class, "/example-data/example-metadata.json");
        assertNotNull(metadataUrl, "metadataUrl is null");
        metadataUri = metadataUrl.toURI();
        ExampleClient client = new ExampleClient(new ExampleConfig().setMetadata(metadataUri), CATALOG_CODEC);
        metadata = new ExampleMetadata(new ExampleConnectorId(CONNECTOR_ID), client);
    }

    @Test
    public void testCanHandle()
    {
        assertTrue(metadata.canHandle(new ExampleTableHandle(CONNECTOR_ID, "schema", "table")));
        assertFalse(metadata.canHandle(new ExampleTableHandle("unknown", "schema", "table")));
    }

    @Test
    public void testListSchemaNames()
    {
        assertEquals(metadata.listSchemaNames(), ImmutableSet.of("example", "tpch"));
    }

    @Test
    public void testGetTableHandle()
    {
        assertEquals(metadata.getTableHandle(new SchemaTableName("example", "numbers")), NUMBERS_TABLE_HANDLE);
        assertNull(metadata.getTableHandle(new SchemaTableName("example", "unknown")));
        assertNull(metadata.getTableHandle(new SchemaTableName("unknown", "numbers")));
        assertNull(metadata.getTableHandle(new SchemaTableName("unknown", "unknown")));
    }

    @Test
    public void testGetColumnHandle()
    {
        // known column
        assertEquals(metadata.getColumnHandle(NUMBERS_TABLE_HANDLE, "text"),
                new ExampleColumnHandle(CONNECTOR_ID, "text", STRING, 0));

        // unknown column
        assertNull(metadata.getColumnHandle(NUMBERS_TABLE_HANDLE, "unknown"));

        // unknown table
        try {
            metadata.getColumnHandle(new ExampleTableHandle(CONNECTOR_ID, "unknown", "unknown"), "unknown");
            fail("Expected getColumnHandle of unknown table to throw a TableNotFoundException");
        }
        catch (TableNotFoundException expected) {
        }
        try {
            metadata.getColumnHandle(new ExampleTableHandle(CONNECTOR_ID, "example", "unknown"), "unknown");
            fail("Expected getColumnHandle of unknown table to throw a TableNotFoundException");
        }
        catch (TableNotFoundException expected) {
        }
    }

    @Test
    public void testGetColumnHandles()
    {
        // known table
        assertEquals(metadata.getColumnHandles(NUMBERS_TABLE_HANDLE), ImmutableMap.of(
                "text", new ExampleColumnHandle(CONNECTOR_ID, "text", STRING, 0),
                "value", new ExampleColumnHandle(CONNECTOR_ID, "value", LONG, 1)));

        // unknown table
        try {
            metadata.getColumnHandles(new ExampleTableHandle(CONNECTOR_ID, "unknown", "unknown"));
            fail("Expected getColumnHandle of unknown table to throw a TableNotFoundException");
        }
        catch (TableNotFoundException expected) {
        }
        try {
            metadata.getColumnHandles(new ExampleTableHandle(CONNECTOR_ID, "example", "unknown"));
            fail("Expected getColumnHandle of unknown table to throw a TableNotFoundException");
        }
        catch (TableNotFoundException expected) {
        }
    }

    @Test
    public void getTableMetadata()
    {
        // known table
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(NUMBERS_TABLE_HANDLE);
        assertEquals(tableMetadata.getTable(), new SchemaTableName("example", "numbers"));
        assertEquals(tableMetadata.getColumns(), ImmutableList.of(
                new ColumnMetadata("text", STRING, 0, false),
                new ColumnMetadata("value", ColumnType.LONG, 1, false)));

        // unknown tables should produce null
        assertNull(metadata.getTableMetadata(new ExampleTableHandle(CONNECTOR_ID, "unknown", "unknown")));
        assertNull(metadata.getTableMetadata(new ExampleTableHandle(CONNECTOR_ID, "example", "unknown")));
        assertNull(metadata.getTableMetadata(new ExampleTableHandle(CONNECTOR_ID, "unknown", "numbers")));
    }

    @Test
    public void testListTables()
    {
        // all schemas
        assertEquals(ImmutableSet.copyOf(metadata.listTables(null)), ImmutableSet.of(
                new SchemaTableName("example", "numbers"),
                new SchemaTableName("tpch", "orders"),
                new SchemaTableName("tpch", "lineitem")));

        // specific schema
        assertEquals(ImmutableSet.copyOf(metadata.listTables("example")), ImmutableSet.of(
                new SchemaTableName("example", "numbers")));
        assertEquals(ImmutableSet.copyOf(metadata.listTables("tpch")), ImmutableSet.of(
                new SchemaTableName("tpch", "orders"),
                new SchemaTableName("tpch", "lineitem")));

        // unknown schema
        assertEquals(ImmutableSet.copyOf(metadata.listTables("unknown")), ImmutableSet.of());
    }

    @Test
    public void getColumnMetadata()
    {
        assertEquals(metadata.getColumnMetadata(NUMBERS_TABLE_HANDLE, new ExampleColumnHandle(CONNECTOR_ID, "text", STRING, 0)),
                new ColumnMetadata("text", STRING, 0, false));

        // example connector assumes that the table handle and column handle are
        // properly formed, so it will return a metadata object for any
        // ExampleTableHandle and ExampleColumnHandle passed in.  This is on because
        // it is not possible for the Presto Metadata system to create the handles
        // directly.
    }


    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testCreateTable()
    {
        metadata.createTable(new ConnectorTableMetadata(
                new SchemaTableName("example", "foo"),
                ImmutableList.of(new ColumnMetadata("text", STRING, 0, false))));
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testDropTableTable()
    {
        metadata.dropTable(NUMBERS_TABLE_HANDLE);
    }
}
