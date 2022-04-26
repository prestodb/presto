package com.facebook.presto.execution.scheduler.nodeselection;

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelection;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeScorer;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelectionHint;
import com.facebook.presto.execution.scheduler.nodeSelection.ScoreBasedNodeSelection;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.testing.assertions.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.longs.LongComparators;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.UUID;

public class ScoreBasedNodeSelectionTest
{
    private InternalNode node1;
    private InternalNode coordinator;
    private InternalNode node2;
    private InternalNode node3;

    @BeforeTest
    public void setup() throws URISyntaxException {
        coordinator = new InternalNode("coordinator", new URI("/"), NodeVersion.UNKNOWN, true);
        node1 = new InternalNode("node1", new URI("/"), NodeVersion.UNKNOWN, false);
        node2 = new InternalNode("node2", new URI("/"), NodeVersion.UNKNOWN, false);
        node3 = new InternalNode("node3", new URI("/"), NodeVersion.UNKNOWN, false);
    }

    @Test
    public void shouldReturnNodeWithHighestScore()
    {
        ImmutableList<InternalNode> candidateNodes = ImmutableList.of(node1, coordinator, node2, node3);
        NodeSelectionHint hint = NodeSelectionHint.newBuilder()
                .limit(1)
                .includeCoordinator(true)
                .build();

        NodeScorer scorer = (node) -> node == coordinator ? 20 : (node == node3 ? 10 : 1);
        NodeSelection selector = new ScoreBasedNodeSelection(scorer, LongComparators.NATURAL_COMPARATOR);

        List<InternalNode> selectedNodes = selector.select(candidateNodes, hint);

        Assert.assertEquals(1, selectedNodes.size());
        Assert.assertEquals(coordinator, selectedNodes.get(0));
    }

    @Test
    public void shouldReturnNodeWithHighestScoreSkippingCoordinator()
    {
        ImmutableList<InternalNode> candidateNodes = ImmutableList.of(node1, coordinator, node2, node3);
        NodeSelectionHint hint = NodeSelectionHint.newBuilder()
                .limit(1)
                .includeCoordinator(false)
                .build();

        NodeScorer scorer = (node) -> node == coordinator ? 30 : (node == node3 ? 20 : 1);
        NodeSelection selector = new ScoreBasedNodeSelection(scorer, LongComparators.NATURAL_COMPARATOR);

        List<InternalNode> selectedNodes = selector.select(candidateNodes, hint);

        Assert.assertEquals(1, selectedNodes.size());
        Assert.assertEquals(node3, selectedNodes.get(0));
    }

    @Test
    public void shouldReturnNodeWithHighestScoreSkippingExclusionList()
    {
        ImmutableList<InternalNode> candidateNodes = ImmutableList.of(node1, coordinator, node2, node3);
        NodeSelectionHint hint = NodeSelectionHint.newBuilder()
                .limit(1)
                .includeCoordinator(true)
                .excludeNodes(ImmutableSet.of(coordinator))
                .build();

        NodeScorer scorer = (node) -> node == coordinator ? 30 : (node == node3 ? 20 : 1);
        NodeSelection selector = new ScoreBasedNodeSelection(scorer, LongComparators.NATURAL_COMPARATOR);

        List<InternalNode> selectedNodes = selector.select(candidateNodes, hint);

        Assert.assertEquals(1, selectedNodes.size());
        Assert.assertEquals(node3, selectedNodes.get(0));
    }
}