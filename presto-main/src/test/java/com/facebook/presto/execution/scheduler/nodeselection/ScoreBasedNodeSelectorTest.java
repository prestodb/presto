package com.facebook.presto.execution.scheduler.nodeselection;

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.execution.scheduler.nodeSelection.INodeSelector;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeScorer;
import com.facebook.presto.execution.scheduler.nodeSelection.ScoreBasedNodeSelector;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.testing.assertions.Assert;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.longs.LongComparators;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.UUID;

public class ScoreBasedNodeSelectorTest
{
    @Test
    public void shouldReturnNodeWithHighestScore()
            throws URISyntaxException
    {
        InternalNode node1 = new InternalNode(UUID.randomUUID().toString(), new URI("/"), NodeVersion.UNKNOWN, false);
        InternalNode node2 = new InternalNode(UUID.randomUUID().toString(), new URI("/"), NodeVersion.UNKNOWN, true);
        InternalNode node3 = new InternalNode(UUID.randomUUID().toString(), new URI("/"), NodeVersion.UNKNOWN, false);
        InternalNode node4 = new InternalNode(UUID.randomUUID().toString(), new URI("/"), NodeVersion.UNKNOWN, false);
        ImmutableList<InternalNode> candidateNodes = ImmutableList.of(node1, node2, node3, node4);
        INodeSelector.NodeSelectionHint hint = INodeSelector.NodeSelectionHint.newBuilder()
                .limit(1)
                .includeCoordinator(true)
                .build();

        NodeScorer scorer = (node) -> node == node2 ? 20 : (node == node4 ? 10 : 1);
        INodeSelector selector = new ScoreBasedNodeSelector(scorer, LongComparators.NATURAL_COMPARATOR);

        List<InternalNode> selectedNodes = selector.select(candidateNodes, hint);

        Assert.assertEquals(1, selectedNodes.size());
        Assert.assertEquals(node2, selectedNodes.get(0));
    }

    @Test
    public void shouldReturnNodeWithHighestScoreSkippingCoordinator()
            throws URISyntaxException
    {
        InternalNode node1 = new InternalNode(UUID.randomUUID().toString(), new URI("/"), NodeVersion.UNKNOWN, false);
        InternalNode node2 = new InternalNode(UUID.randomUUID().toString(), new URI("/"), NodeVersion.UNKNOWN, true);
        InternalNode node3 = new InternalNode(UUID.randomUUID().toString(), new URI("/"), NodeVersion.UNKNOWN, false);
        InternalNode node4 = new InternalNode(UUID.randomUUID().toString(), new URI("/"), NodeVersion.UNKNOWN, false);
        ImmutableList<InternalNode> candidateNodes = ImmutableList.of(node1, node2, node3, node4);
        INodeSelector.NodeSelectionHint hint = INodeSelector.NodeSelectionHint.newBuilder()
                .limit(1)
                .includeCoordinator(false)
                .build();

        NodeScorer scorer = (node) -> node == node2 ? 10 : (node == node4 ? 20 : 1);
        INodeSelector selector = new ScoreBasedNodeSelector(scorer, LongComparators.NATURAL_COMPARATOR);

        List<InternalNode> selectedNodes = selector.select(candidateNodes, hint);

        Assert.assertEquals(1, selectedNodes.size());
        Assert.assertEquals(node4, selectedNodes.get(0));
    }
}