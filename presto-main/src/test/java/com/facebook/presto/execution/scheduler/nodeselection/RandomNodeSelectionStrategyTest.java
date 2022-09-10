package com.facebook.presto.execution.scheduler.nodeselection;

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelectionStrategy;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelectionHint;
import com.facebook.presto.execution.scheduler.nodeSelection.RandomNodeSelectionStrategy;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.testing.assertions.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.objects.Object2IntArrayMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.UUID;

public class RandomNodeSelectionStrategyTest
{
    public static final int TEST_ITERS = 5;

    @Test
    public void testReturnAllNodes()
            throws Exception
    {
        InternalNode node = new InternalNode(UUID.randomUUID().toString(), new URI("/"), NodeVersion.UNKNOWN, false);
        InternalNode coordinator = new InternalNode(UUID.randomUUID().toString(), new URI("/"), NodeVersion.UNKNOWN, true);
        ImmutableList<InternalNode> candidateNodes = ImmutableList.of(node, coordinator);
        NodeSelectionHint hint = NodeSelectionHint.newBuilder().includeCoordinator(true).build();
        NodeSelectionStrategy selector = new RandomNodeSelectionStrategy();

        List<InternalNode> selectedNodes = selector.select(candidateNodes, hint);

        Assert.assertTrue(selectedNodes.contains(node));
        Assert.assertTrue(selectedNodes.contains(coordinator));
    }

    @Test
    public void testReturnOneNode()
            throws Exception
    {
        InternalNode node1 = new InternalNode(UUID.randomUUID().toString(), new URI("/"), NodeVersion.UNKNOWN, false);
        InternalNode node2 = new InternalNode(UUID.randomUUID().toString(), new URI("/"), NodeVersion.UNKNOWN, false);
        ImmutableList<InternalNode> candidateNodes = ImmutableList.of(node1, node2);
        NodeSelectionHint hint = NodeSelectionHint.newBuilder().limit(1).build();

        NodeSelectionStrategy selector = new RandomNodeSelectionStrategy();

        Object2IntMap<InternalNode> countMap = new Object2IntArrayMap<>(2);
        int count = TEST_ITERS;
        while (count-- > 0) {
            List<InternalNode> selectedNodes = selector.select(candidateNodes, hint);

            Assert.assertEquals(1, selectedNodes.size());
            countMap.mergeInt(selectedNodes.get(0), 1, Integer::sum);
        }

        for (Integer value : countMap.values()) {
            Assert.assertNotEquals(0, value);
        }
    }

    @Test
    public void testIgnoreCoordinator()
            throws Exception
    {
        InternalNode node1 = new InternalNode(UUID.randomUUID().toString(), new URI("/"), NodeVersion.UNKNOWN, false);
        InternalNode node2 = new InternalNode(UUID.randomUUID().toString(), new URI("/"), NodeVersion.UNKNOWN, true);
        InternalNode node3 = new InternalNode(UUID.randomUUID().toString(), new URI("/"), NodeVersion.UNKNOWN, true);
        ImmutableList<InternalNode> candidateNodes = ImmutableList.of(node1, node2, node3);
        NodeSelectionHint hint = NodeSelectionHint.newBuilder()
                .limit(1)
                .includeCoordinator(false)
                .build();
        NodeSelectionStrategy selector = new RandomNodeSelectionStrategy();

        int count = TEST_ITERS;
        while (count-- > 0) {
            List<InternalNode> selectedNodes = selector.select(candidateNodes, hint);

            Assert.assertEquals(1, selectedNodes.size());
            Assert.assertEquals(node1, selectedNodes.get(0));
        }
    }

    @Test
    public void testSkipNodesOnExclusionList()
            throws Exception
    {
        InternalNode node1 = new InternalNode("node1", new URI("/"), NodeVersion.UNKNOWN, false);
        InternalNode node2 = new InternalNode("node2", new URI("/"), NodeVersion.UNKNOWN, true);
        InternalNode node3 = new InternalNode("node3", new URI("/"), NodeVersion.UNKNOWN, true);
        InternalNode node4 = new InternalNode("node4", new URI("/"), NodeVersion.UNKNOWN, false);
        InternalNode node5 = new InternalNode("node5", new URI("/"), NodeVersion.UNKNOWN, false);
        ImmutableList<InternalNode> candidateNodes = ImmutableList.of(node1, node2, node3, node4, node5);
        NodeSelectionHint hint = NodeSelectionHint.newBuilder()
                .limit(1)
                .includeCoordinator(false)
                .excludeNodes(ImmutableSet.of(node4, node5))
                .build();
        NodeSelectionStrategy selector = new RandomNodeSelectionStrategy();

        int count = TEST_ITERS;
        while (count-- > 0) {
            List<InternalNode> selectedNodes = selector.select(candidateNodes, hint);

            Assert.assertEquals(1, selectedNodes.size());
            Assert.assertEquals(node1, selectedNodes.get(0));
        }
    }
}