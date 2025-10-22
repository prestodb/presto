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
package com.facebook.presto.execution.scheduler.clusterOverload;

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.execution.ClusterOverloadConfig;
import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.NodeLoadMetrics;
import com.facebook.presto.spi.NodeState;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Consumer;

import static com.facebook.presto.execution.ClusterOverloadConfig.OVERLOAD_POLICY_CNT_BASED;
import static com.facebook.presto.execution.ClusterOverloadConfig.OVERLOAD_POLICY_PCT_BASED;
import static com.facebook.presto.metadata.InternalNode.NodeStatus.ALIVE;
import static com.facebook.presto.spi.NodePoolType.DEFAULT;
import static com.facebook.presto.spi.NodeState.ACTIVE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestCpuMemoryOverloadPolicy
{
    private static final NodeVersion TEST_VERSION = new NodeVersion("test");
    private static final URI TEST_URI = URI.create("http://test.example.com");

    @Test
    public void testIsClusterOverloadedCountBasedNoOverload()
    {
        ClusterOverloadConfig config = new ClusterOverloadConfig()
                .setAllowedOverloadWorkersCnt(1)
                .setOverloadPolicyType(OVERLOAD_POLICY_CNT_BASED);
        CpuMemoryOverloadPolicy policy = new CpuMemoryOverloadPolicy(config);

        // One node is overloaded, but allowed count is 1, so not overloaded
        InternalNodeManager nodeManager = createNodeManager(ImmutableSet.of(createNode("node1", true, false), createNode("node2", false, false), createNode("node3", false, false)));
        assertFalse(policy.isClusterOverloaded(nodeManager));
    }

    @Test
    public void testIsClusterOverloadedCountBasedOverload()
    {
        ClusterOverloadConfig config = new ClusterOverloadConfig()
                .setAllowedOverloadWorkersCnt(1)
                .setOverloadPolicyType(OVERLOAD_POLICY_CNT_BASED);
        CpuMemoryOverloadPolicy policy = new CpuMemoryOverloadPolicy(config);

        // Two nodes are overloaded, but allowed count is 1, so overloaded
        InternalNodeManager nodeManager = createNodeManager(ImmutableSet.of(createNode("node1", true, false), createNode("node2", false, true), createNode("node3", false, false)));
        assertTrue(policy.isClusterOverloaded(nodeManager));
    }

    @Test
    public void testIsClusterOverloadedPctBasedNoOverload()
    {
        ClusterOverloadConfig config = new ClusterOverloadConfig().setAllowedOverloadWorkersPct(0.4).setOverloadPolicyType(OVERLOAD_POLICY_PCT_BASED);
        CpuMemoryOverloadPolicy policy = new CpuMemoryOverloadPolicy(config);

        // 1 out of 3 nodes (33%) are overloaded, allowed is 40%, so not overloaded
        InternalNodeManager nodeManager = createNodeManager(ImmutableSet.of(createNode("node1", true, false), createNode("node2", false, false), createNode("node3", false, false)));
        assertFalse(policy.isClusterOverloaded(nodeManager));
    }

    @Test
    public void testIsClusterOverloadedPctBasedOverload()
    {
        ClusterOverloadConfig config = new ClusterOverloadConfig().setAllowedOverloadWorkersPct(0.3).setOverloadPolicyType(OVERLOAD_POLICY_PCT_BASED);
        CpuMemoryOverloadPolicy policy = new CpuMemoryOverloadPolicy(config);

        // 2 out of 5 nodes (40%) are overloaded, allowed is 30%, so overloaded
        InternalNodeManager nodeManager = createNodeManager(ImmutableSet.of(createNode("node1", true, false), createNode("node2", false, true), createNode("node3", false, false), createNode("node4", false, false), createNode("node5", false, false)));
        assertTrue(policy.isClusterOverloaded(nodeManager));
    }

    @Test
    public void testIsClusterOverloadedBothMetricsOverloaded()
    {
        ClusterOverloadConfig config = new ClusterOverloadConfig()
                .setAllowedOverloadWorkersCnt(0)
                .setOverloadPolicyType(OVERLOAD_POLICY_CNT_BASED);
        CpuMemoryOverloadPolicy policy = new CpuMemoryOverloadPolicy(config);

        // Node has both CPU and memory overloaded, should only count as one overloaded node
        InternalNodeManager nodeManager = createNodeManager(ImmutableSet.of(createNode("node1", true, true)));
        assertTrue(policy.isClusterOverloaded(nodeManager));
    }

    @Test
    public void testIsClusterOverloadedNoNodes()
    {
        ClusterOverloadConfig config = new ClusterOverloadConfig()
                .setAllowedOverloadWorkersCnt(0)
                .setOverloadPolicyType(OVERLOAD_POLICY_CNT_BASED);
        CpuMemoryOverloadPolicy policy = new CpuMemoryOverloadPolicy(config);

        // No nodes, should not be overloaded
        InternalNodeManager nodeManager = createNodeManager(ImmutableSet.of());
        assertFalse(policy.isClusterOverloaded(nodeManager));
    }

    @Test
    public void testGetNameCountBased()
    {
        ClusterOverloadConfig config = new ClusterOverloadConfig()
                .setOverloadPolicyType(OVERLOAD_POLICY_CNT_BASED);
        CpuMemoryOverloadPolicy policy = new CpuMemoryOverloadPolicy(config);

        assertEquals(policy.getName(), "cpu-memory-overload-cnt");
    }

    @Test
    public void testGetNamePctBased()
    {
        ClusterOverloadConfig config = new ClusterOverloadConfig()
                .setOverloadPolicyType(OVERLOAD_POLICY_PCT_BASED);
        CpuMemoryOverloadPolicy policy = new CpuMemoryOverloadPolicy(config);
        assertEquals(policy.getName(), "cpu-memory-overload-pct");
    }

    // Store metrics separately since they're no longer part of InternalNode
    private static final Map<String, NodeLoadMetrics> NODE_METRICS = new HashMap<>();

    private static InternalNode createNode(String nodeId, boolean cpuOverload, boolean memoryOverload)
    {
        // Store metrics in the map for later retrieval
        NODE_METRICS.put(nodeId, new NodeLoadMetrics(0.0, 0.0, 0, cpuOverload, memoryOverload));
        return new InternalNode(
                nodeId,
                TEST_URI,
                OptionalInt.empty(),
                TEST_VERSION,
                false,
                false,
                false,
                false,
                ALIVE,
                OptionalInt.empty(),
                DEFAULT);
    }

    private static InternalNodeManager createNodeManager(Set<InternalNode> nodes)
    {
        return new InternalNodeManager()
        {
            @Override
            public Set<InternalNode> getNodes(NodeState state)
            {
                if (state == ACTIVE) {
                    return nodes;
                }
                return ImmutableSet.of();
            }

            @Override
            public Set<InternalNode> getActiveConnectorNodes(ConnectorId connectorId)
            {
                return ImmutableSet.of();
            }

            @Override
            public Set<InternalNode> getAllConnectorNodes(ConnectorId connectorId)
            {
                return Collections.emptySet();
            }

            @Override
            public InternalNode getCurrentNode()
            {
                return null;
            }

            @Override
            public Set<InternalNode> getCoordinators()
            {
                return ImmutableSet.of();
            }

            @Override
            public Set<InternalNode> getShuttingDownCoordinator()
            {
                return ImmutableSet.of();
            }

            @Override
            public Set<InternalNode> getResourceManagers()
            {
                return ImmutableSet.of();
            }

            @Override
            public Set<InternalNode> getCatalogServers()
            {
                return ImmutableSet.of();
            }

            @Override
            public Set<InternalNode> getCoordinatorSidecars()
            {
                return ImmutableSet.of();
            }

            @Override
            public AllNodes getAllNodes()
            {
                return new AllNodes(nodes, ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of());
            }

            @Override
            public void refreshNodes()
            {
            }

            @Override
            public void addNodeChangeListener(Consumer<AllNodes> listener)
            {
            }

            @Override
            public void removeNodeChangeListener(Consumer<AllNodes> listener)
            {
            }

            @Override
            public Optional<NodeLoadMetrics> getNodeLoadMetrics(String nodeIdentifier)
            {
                NodeLoadMetrics metrics = NODE_METRICS.get(nodeIdentifier);
                return metrics != null ? Optional.of(metrics) : Optional.empty();
            }
        };
    }
}
