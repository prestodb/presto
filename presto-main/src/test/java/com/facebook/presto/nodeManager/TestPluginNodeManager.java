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
package com.facebook.presto.nodeManager;

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.Node;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestPluginNodeManager
{
    private InMemoryNodeManager inMemoryNodeManager;
    private PluginNodeManager pluginNodeManager;

    @BeforeClass
    public void setUp()
    {
        // Initialize the InMemoryNodeManager and PluginNodeManager before each test.
        inMemoryNodeManager = new InMemoryNodeManager();
        pluginNodeManager = new PluginNodeManager(inMemoryNodeManager, "test-env");
    }

    @Test
    public void testGetAllNodes()
    {
        ConnectorId connectorId = new ConnectorId("test-connector");
        InternalNode activeNode1 = new InternalNode("activeNode1", URI.create("http://example1.com"), new NodeVersion("1"), false);
        InternalNode activeNode2 = new InternalNode("activeNode2", URI.create("http://example2.com"), new NodeVersion("1"), false);
        InternalNode coordinatorNode = new InternalNode("coordinatorNode", URI.create("http://example3.com"), new NodeVersion("1"), true);

        inMemoryNodeManager.addNode(connectorId, activeNode1);
        inMemoryNodeManager.addNode(connectorId, activeNode2);
        inMemoryNodeManager.addNode(connectorId, coordinatorNode);

        Set<Node> allNodes = pluginNodeManager.getAllNodes();
        // The expected count is 4, considering two active nodes, one coordinator, and one local node added by InMemoryNodeManager by default.
        assertEquals(4, allNodes.size());
        assertTrue(allNodes.containsAll(Arrays.asList(activeNode1, activeNode2, coordinatorNode)));
    }

    @Test
    public void testGetWorkerNodes()
    {
        ConnectorId connectorId = new ConnectorId("test-connector");
        InternalNode activeNode1 = new InternalNode("activeNode1", URI.create("http://example1.com"), new NodeVersion("1"), false);
        InternalNode activeNode2 = new InternalNode("activeNode2", URI.create("http://example2.com"), new NodeVersion("1"), false);

        inMemoryNodeManager.addNode(connectorId, activeNode1);
        inMemoryNodeManager.addNode(connectorId, activeNode2);

        Set<Node> workerNodes = pluginNodeManager.getWorkerNodes();
        // Expected count is 3, accounting for two explicitly added active nodes and one local node.
        assertEquals(3, workerNodes.size());
        assertTrue(workerNodes.containsAll(Arrays.asList(activeNode1, activeNode2)));
    }

    @Test
    public void testGetEnvironment()
    {
        // Validate that the PluginNodeManager correctly returns the environment string set during initialization.
        assertEquals("test-env", pluginNodeManager.getEnvironment());
    }

    @Test
    public void testGetCurrentNode()
    {
        Node currentNode = pluginNodeManager.getCurrentNode();
        assertNotNull(currentNode);
        // Validate that the current node is not null and its identifier matches the expected local node identifier.
        assertEquals("local", currentNode.getNodeIdentifier());
    }
}
