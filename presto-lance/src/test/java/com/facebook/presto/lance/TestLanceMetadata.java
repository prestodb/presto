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
package com.facebook.presto.lance;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestLanceMetadata
{
    private LanceMetadata metadata;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        URL dbUrl = Resources.getResource(TestLanceMetadata.class, "/example_db");
        assertNotNull(dbUrl, "example_db resource not found");
        String rootPath = Paths.get(dbUrl.toURI()).toString();
        LanceConfig config = new LanceConfig()
                .setRootUrl(rootPath)
                .setSingleLevelNs(true);
        LanceNamespaceHolder namespaceHolder = new LanceNamespaceHolder(config);
        JsonCodec<LanceCommitTaskData> commitTaskDataCodec = jsonCodec(LanceCommitTaskData.class);
        metadata = new LanceMetadata(namespaceHolder, commitTaskDataCodec);
    }

    @Test
    public void testListSchemaNames()
    {
        List<String> schemas = metadata.listSchemaNames(null);
        assertEquals(schemas.size(), 1);
        assertEquals(schemas.get(0), "default");
    }

    @Test
    public void testGetTableHandle()
    {
        ConnectorTableHandle handle = metadata.getTableHandle(null, new SchemaTableName("default", "test_table1"));
        assertNotNull(handle);
        assertEquals(handle, new LanceTableHandle("default", "test_table1"));

        ConnectorTableHandle handle2 = metadata.getTableHandle(null, new SchemaTableName("default", "test_table2"));
        assertNotNull(handle2);
        assertEquals(handle2, new LanceTableHandle("default", "test_table2"));

        // non-existent schema
        assertNull(metadata.getTableHandle(null, new SchemaTableName("other_schema", "test_table1")));

        // non-existent table
        assertNull(metadata.getTableHandle(null, new SchemaTableName("default", "nonexistent")));
    }

    @Test
    public void testGetColumnHandles()
    {
        LanceTableHandle tableHandle = new LanceTableHandle("default", "test_table1");
        Map<String, ColumnHandle> columns = metadata.getColumnHandles(null, tableHandle);
        assertNotNull(columns);
        assertEquals(columns.size(), 4);
        assertTrue(columns.containsKey("x"));
        assertTrue(columns.containsKey("y"));
        assertTrue(columns.containsKey("b"));
        assertTrue(columns.containsKey("c"));
    }

    @Test
    public void testGetTableMetadata()
    {
        LanceTableHandle tableHandle = new LanceTableHandle("default", "test_table1");
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(null, tableHandle);
        assertNotNull(tableMetadata);
        assertEquals(tableMetadata.getTable(), new SchemaTableName("default", "test_table1"));
        assertEquals(tableMetadata.getColumns().size(), 4);

        // Verify column names
        Set<String> columnNames = tableMetadata.getColumns().stream()
                .map(col -> col.getName())
                .collect(Collectors.toSet());
        assertEquals(columnNames, ImmutableSet.of("x", "y", "b", "c"));
    }

    @Test
    public void testListTables()
    {
        // all tables in default schema
        List<SchemaTableName> tables = metadata.listTables(null, Optional.of("default"));
        Set<SchemaTableName> tableSet = ImmutableSet.copyOf(tables);
        assertEquals(tableSet, ImmutableSet.of(
                new SchemaTableName("default", "test_table1"),
                new SchemaTableName("default", "test_table2"),
                new SchemaTableName("default", "test_table3"),
                new SchemaTableName("default", "test_table4")));

        // no schema filter
        List<SchemaTableName> allTables = metadata.listTables(null, Optional.empty());
        assertEquals(ImmutableSet.copyOf(allTables), tableSet);
    }
}
