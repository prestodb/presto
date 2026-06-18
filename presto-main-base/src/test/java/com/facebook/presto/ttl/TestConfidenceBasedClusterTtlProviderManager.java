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
package com.facebook.presto.ttl;

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ttl.ClusterTtlProviderFactory;
import com.facebook.presto.spi.ttl.ConfidenceBasedTtlInfo;
import com.facebook.presto.spi.ttl.NodeInfo;
import com.facebook.presto.spi.ttl.NodeTtl;
import com.facebook.presto.spi.ttl.NodeTtlFetcherFactory;
import com.facebook.presto.spi.ttl.TestingClusterTtlProviderFactory;
import com.facebook.presto.spi.ttl.TestingNodeTtlFetcherFactory;
import com.facebook.presto.ttl.clusterttlprovidermanagers.ConfidenceBasedClusterTtlProviderManager;
import com.facebook.presto.ttl.nodettlfetchermanagers.ConfidenceBasedNodeTtlFetcherManager;
import com.facebook.presto.ttl.nodettlfetchermanagers.NodeTtlFetcherManagerConfig;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class TestConfidenceBasedClusterTtlProviderManager
{
    private final InternalNode node1 = new InternalNode("node_1", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true);
    private final InternalNode node2 = new InternalNode("node_2", URI.create("local://127.0.0.2"), NodeVersion.UNKNOWN, false);
    private final InternalNode node3 = new InternalNode("node_3", URI.create("local://127.0.0.3"), NodeVersion.UNKNOWN, false);
    private final NodeTtl ttl1 = new NodeTtl(ImmutableSet.of(new ConfidenceBasedTtlInfo(10, 90)));
    private final NodeTtl ttl2 = new NodeTtl(ImmutableSet.of(new ConfidenceBasedTtlInfo(20, 80)));
    private final NodeTtl ttl3 = new NodeTtl(ImmutableSet.of(new ConfidenceBasedTtlInfo(30, 70)));
    private final Map<NodeInfo, NodeTtl> nodeToTtl = ImmutableMap.of(
            new NodeInfo(node1.getNodeIdentifier(), node1.getHost()),
            ttl1,
            new NodeInfo(node2.getNodeIdentifier(), node2.getHost()),
            ttl2,
            new NodeInfo(node3.getNodeIdentifier(), node3.getHost()),
            ttl3);
    private ConfidenceBasedClusterTtlProviderManager clusterTtlProviderManager;

    @BeforeClass
    public void setup()
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        nodeManager.addNode(new ConnectorId("prism"), ImmutableSet.of(node1, node2, node3));

        ConfidenceBasedNodeTtlFetcherManager nodeTtlFetcherManager = new ConfidenceBasedNodeTtlFetcherManager(nodeManager, new NodeSchedulerConfig(), new NodeTtlFetcherManagerConfig());
        NodeTtlFetcherFactory nodeTtlFetcherFactory = new TestingNodeTtlFetcherFactory(nodeToTtl);
        nodeTtlFetcherManager.addNodeTtlFetcherFactory(nodeTtlFetcherFactory);
        nodeTtlFetcherManager.load(nodeTtlFetcherFactory.getName(), ImmutableMap.of());
        nodeTtlFetcherManager.refreshTtlInfo();

        clusterTtlProviderManager = new ConfidenceBasedClusterTtlProviderManager(nodeTtlFetcherManager);
        ClusterTtlProviderFactory clusterTtlProviderFactory = new TestingClusterTtlProviderFactory();
        clusterTtlProviderManager.addClusterTtlProviderFactory(clusterTtlProviderFactory);
        clusterTtlProviderManager.load(clusterTtlProviderFactory.getName(), ImmutableMap.of());
    }

    @Test
    public void testGetClusterTtl()
    {
        assertEquals(clusterTtlProviderManager.getClusterTtl(), new ConfidenceBasedTtlInfo(60, 100));
    }
}
