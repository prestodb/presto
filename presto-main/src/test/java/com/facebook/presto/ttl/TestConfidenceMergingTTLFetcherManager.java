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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestConfidenceMergingTTLFetcherManager
{
    private final InternalNode node1 = new InternalNode("node_1", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true);
    private final InternalNode node2 = new InternalNode("node_2", URI.create("local://127.0.0.2"), NodeVersion.UNKNOWN, false);
    private final InternalNode node3 = new InternalNode("node_3", URI.create("local://127.0.0.3"), NodeVersion.UNKNOWN, false);
    private final Map<InternalNode, Set<ConfidenceBasedTTLInfo>> nodeToTTL1 = ImmutableMap.of(
            node1,
            ImmutableSet.of(new ConfidenceBasedTTLInfo(10, 90)),
            node2,
            ImmutableSet.of(new ConfidenceBasedTTLInfo(20, 80)),
            node3,
            ImmutableSet.of(new ConfidenceBasedTTLInfo(30, 70)));
    private InMemoryNodeManager nodeManager;
    private TTLFetcher ttlFetcher;

    @BeforeClass
    public void setup()
    {
        nodeManager = new InMemoryNodeManager();
        nodeManager.addNode(new ConnectorId("prism"), ImmutableSet.of(node1, node2, node3));
        ttlFetcher = new InMemoryTTLFetcher(nodeToTTL1);
    }

    @Test
    public void testEmptyTtlInfo()
    {
        TTLFetcherManager ttlFetcherManager = new ConfidenceBasedTTLFetcherManager(ttlFetcher,
                nodeManager,
                new NodeSchedulerConfig().setIncludeCoordinator(true));
        assertFalse(ttlFetcherManager.getTTLInfo(nodeManager.getCurrentNode()).isPresent());
    }

    @Test
    public void testNonEmptyTtlInfo()
    {
        TTLFetcherManager ttlFetcherManager = new ConfidenceBasedTTLFetcherManager(ttlFetcher,
                nodeManager,
                new NodeSchedulerConfig().setIncludeCoordinator(true));
        assertEquals(ttlFetcherManager.getTTLInfo(node1),
                Optional.of(new NodeTTL(ImmutableSet.of(new ConfidenceBasedTTLInfo(10, 90)))));
        assertEquals(ttlFetcherManager.getTTLInfo(node2),
                Optional.of(new NodeTTL(ImmutableSet.of(new ConfidenceBasedTTLInfo(20, 80)))));
    }
}
