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
package com.facebook.presto.plugin.turbonium;

import com.facebook.presto.plugin.turbonium.config.TurboniumConfigManager;
import com.facebook.presto.plugin.turbonium.config.db.H2DaoProvider;
import com.facebook.presto.plugin.turbonium.config.db.TurboniumDbConfig;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.TestingNodeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestTurboniumMetadata
{
    private TurboniumMetadata metadata;

    @BeforeMethod
    public void setUp()
    {
        NodeManager nodeManager = new NodeManager() {
            @Override
            public Set<Node> getAllNodes()
            {
                return ImmutableSet.of();
            }

            @Override
            public Node getCurrentNode()
            {
                return null;
            }

            @Override
            public Set<Node> getWorkerNodes()
            {
                return ImmutableSet.of();
            }

            @Override
            public String getEnvironment()
            {
                return null;
            }
        };
        TurboniumDbConfig dbConfig = new TurboniumDbConfig();
        TurboniumConfigManager turboniumConfigManager = new TurboniumConfigManager(
                new H2DaoProvider(dbConfig).get(),
                new TurboniumConfig().setMaxDataPerNode(new DataSize(1, DataSize.Unit.MEGABYTE)),
                nodeManager
        );
        metadata = new TurboniumMetadata(new TestingNodeManager(), new TurboniumConnectorId("test"), turboniumConfigManager);
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

        TurboniumTableHandle firstTableHandle = (TurboniumTableHandle) metadata.getTableHandle(SESSION, firstTableName);
        Long firstTableId = firstTableHandle.getTableId();

        assertTrue(metadata.beginInsert(SESSION, firstTableHandle).getActiveTableIds().contains(firstTableId));

        SchemaTableName secondTableName = new SchemaTableName("default", "second_table");
        metadata.createTable(SESSION, new ConnectorTableMetadata(secondTableName, ImmutableList.of(), ImmutableMap.of()));

        TurboniumTableHandle secondTableHandle = (TurboniumTableHandle) metadata.getTableHandle(SESSION, secondTableName);
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
