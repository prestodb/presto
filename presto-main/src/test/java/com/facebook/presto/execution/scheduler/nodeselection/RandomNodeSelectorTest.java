package com.facebook.presto.execution.scheduler.nodeselection;

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.execution.scheduler.nodeSelection.INodeSelector;
import com.facebook.presto.execution.scheduler.nodeSelection.INodeSelector.NodeSelectionHint;
import com.facebook.presto.execution.scheduler.nodeSelection.RandomNodeSelector;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.testing.assertions.Assert;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.objects.Object2IntArrayMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.UUID;

public class RandomNodeSelectorTest
{
    public static final int TEST_ITERS = 5;

    @Test
    public void shouldReturnAllNodes()
            throws Exception
    {
        InternalNode node = new InternalNode(UUID.randomUUID().toString(), new URI("/"), NodeVersion.UNKNOWN, false);
        InternalNode coordinator = new InternalNode(UUID.randomUUID().toString(), new URI("/"), NodeVersion.UNKNOWN, true);
        ImmutableList<InternalNode> candidateNodes = ImmutableList.of(node, coordinator);
        NodeSelectionHint hint = NodeSelectionHint.newBuilder().build();
        INodeSelector selector = new RandomNodeSelector();

        List<InternalNode> selectedNodes = selector.select(candidateNodes, hint);

        Assert.assertEquals(candidateNodes, selectedNodes);
    }

    @Test
    public void shouldReturnOneNode()
            throws Exception
    {
        InternalNode node1 = new InternalNode(UUID.randomUUID().toString(), new URI("/"), NodeVersion.UNKNOWN, false);
        InternalNode node2 = new InternalNode(UUID.randomUUID().toString(), new URI("/"), NodeVersion.UNKNOWN, false);
        ImmutableList<InternalNode> candidateNodes = ImmutableList.of(node1, node2);
        NodeSelectionHint hint = NodeSelectionHint.newBuilder().limit(1).build();

        INodeSelector selector = new RandomNodeSelector();

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
    public void shouldIgnoreCoordinator()
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
        INodeSelector selector = new RandomNodeSelector();

        int count = TEST_ITERS;
        while (count-- > 0) {
            List<InternalNode> selectedNodes = selector.select(candidateNodes, hint);

            Assert.assertEquals(1, selectedNodes.size());
            Assert.assertEquals(node1, selectedNodes.get(0));
        }
    }
}