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
package com.facebook.presto.plugin.memory;

import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.TestingNodeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMemoryMetadata
{
    private MemoryMetadata metadata;

    @BeforeMethod
    public void setUp()
    {
        metadata = new MemoryMetadata(new TestingNodeManager(), new MemoryConnectorId("test"));
    }

    @Test
    public void tableIsCreatedAfterCommits()
    {
        assertThatNoTableIsCreated();

        SchemaTableName schemaTableName = new SchemaTableName("default", "temp_table");

        ConnectorOutputTableHandle table = metadata.beginCreateTable(
                SESSION,
                new ConnectorTableMetadata(schemaTableName, ImmutableList.of(), ImmutableMap.of()),
                Optional.empty());

        metadata.finishCreateTable(SESSION, table, ImmutableList.of());

        List<SchemaTableName> tables = metadata.listTables(SESSION, null);
        assertTrue(tables.size() == 1, "Expected only one table.");
        assertTrue(tables.get(0).getTableName().equals("temp_table"), "Expected table with name 'temp_table'");
    }

    @Test
    public void testActiveTableIds()
    {
        assertThatNoTableIsCreated();

        SchemaTableName firstTableName = new SchemaTableName("default", "first_table");
        metadata.createTable(SESSION, new ConnectorTableMetadata(firstTableName, ImmutableList.of(), ImmutableMap.of()));

        MemoryTableHandle firstTableHandle = (MemoryTableHandle) metadata.getTableHandle(SESSION, firstTableName);
        Long firstTableId = firstTableHandle.getTableId();

        assertTrue(metadata.beginInsert(SESSION, firstTableHandle).getActiveTableIds().contains(firstTableId));

        SchemaTableName secondTableName = new SchemaTableName("default", "second_table");
        metadata.createTable(SESSION, new ConnectorTableMetadata(secondTableName, ImmutableList.of(), ImmutableMap.of()));

        MemoryTableHandle secondTableHandle = (MemoryTableHandle) metadata.getTableHandle(SESSION, secondTableName);
        Long secondTableId = secondTableHandle.getTableId();

        assertNotEquals(firstTableId, secondTableId);
        assertTrue(metadata.beginInsert(SESSION, secondTableHandle).getActiveTableIds().contains(firstTableId));
        assertTrue(metadata.beginInsert(SESSION, secondTableHandle).getActiveTableIds().contains(secondTableId));
    }

    private void assertThatNoTableIsCreated()
    {
        assertEquals(metadata.listTables(SESSION, null), ImmutableList.of(), "No table was expected");
    }
}
