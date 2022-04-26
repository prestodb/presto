package com.facebook.presto.execution.scheduler.nodeselection;

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelection;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeScorer;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelectionHint;
import com.facebook.presto.execution.scheduler.nodeSelection.PowerOfTwoChoiceNodeSelection;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.testing.assertions.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.longs.LongComparators;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.UUID;

public class PowerOfTwoChoiceNodeSelectionTest
{
    private final InternalNode node1 = new InternalNode("node1", new URI("/"), NodeVersion.UNKNOWN, false);
    private final InternalNode node2 = new InternalNode("node2", new URI("/"), NodeVersion.UNKNOWN, false);
    private final InternalNode coordinator = new InternalNode("coordinator", new URI("/"), NodeVersion.UNKNOWN, true);

    @Test
    public void testReturnNodes()
    {
        ImmutableList<InternalNode> candidateNodes = ImmutableList.of(node1, node2, coordinator);
        NodeSelectionHint hint = NodeSelectionHint.newBuilder()
                .limit(1)
                .build();

        NodeScorer scorer = (node) -> node == node2 ? 20 : (node == coordinator ? 10 : 1);

        NodeSelection selector = new PowerOfTwoChoiceNodeSelection(scorer, LongComparators.NATURAL_COMPARATOR);

        int count = 5;
        while (count-- > 0) {
            List<InternalNode> selectedNodes = selector.select(candidateNodes, hint);

            Assert.assertEquals(1, selectedNodes.size());
            Assert.assertTrue(node2 == selectedNodes.get(0) || coordinator == selectedNodes.get(0));
        }
    }

    @Test
    public void testSkipCoordinatorInSelection()
    {
        ImmutableList<InternalNode> candidateNodes = ImmutableList.of(node1, node2, coordinator);
        NodeSelectionHint hint = NodeSelectionHint.newBuilder()
                .limit(1)
                .includeCoordinator(false)
                .build();

        NodeScorer scorer = (node) -> node == node2 ? 20 : (node == coordinator ? 40 : 1);

        NodeSelection selector = new PowerOfTwoChoiceNodeSelection(scorer, LongComparators.NATURAL_COMPARATOR);

        int count = 5;
        while (count-- > 0) {
            List<InternalNode> selectedNodes = selector.select(candidateNodes, hint);

            Assert.assertEquals(1, selectedNodes.size());
            Assert.assertSame(node2, selectedNodes.get(0));
        }
    }

    @Test
    public void testSkipNodesOnExclusionListInSelection()
    {
        ImmutableList<InternalNode> candidateNodes = ImmutableList.of(node1, node2, coordinator);
        NodeSelectionHint hint = NodeSelectionHint.newBuilder()
                .limit(1)
                .includeCoordinator(true)
                .excludeNodes(ImmutableSet.of(coordinator))
                .build();

        NodeScorer scorer = (node) -> node == node2 ? 20 : (node == coordinator ? 40 : 1);

        NodeSelection selector = new PowerOfTwoChoiceNodeSelection(scorer, LongComparators.NATURAL_COMPARATOR);

        int count = 5;
        while (count-- > 0) {
            List<InternalNode> selectedNodes = selector.select(candidateNodes, hint);

            Assert.assertEquals(1, selectedNodes.size());
            Assert.assertSame(node2, selectedNodes.get(0));
        }
    }

    @Test
    public void testSelectFromSingleNode()
    {
        ImmutableList<InternalNode> candidateNodes = ImmutableList.of(node1);
        NodeSelectionHint hint = NodeSelectionHint.newBuilder()
                .limit(10)
                .build();

        NodeScorer scorer = (node) -> node == node2 ? 20 : (node == coordinator ? 10 : 1);

        NodeSelection selector = new PowerOfTwoChoiceNodeSelection(scorer, LongComparators.NATURAL_COMPARATOR);

        int count = 5;
        while (count-- > 0) {
            List<InternalNode> selectedNodes = selector.select(candidateNodes, hint);

            Assert.assertEquals(1, selectedNodes.size());
            Assert.assertSame(node1, selectedNodes.get(0));
        }
    }

    @Test
    public void testSelectFromEmptyCandidateSet()
    {
        ImmutableList<InternalNode> candidateNodes = ImmutableList.of();
        NodeSelectionHint hint = NodeSelectionHint.newBuilder()
                .limit(10)
                .build();

        NodeScorer scorer = (node) -> node == node2 ? 20 : (node == coordinator ? 10 : 1);

        NodeSelection selector = new PowerOfTwoChoiceNodeSelection(scorer, LongComparators.NATURAL_COMPARATOR);

        int count = 5;
        while (count-- > 0) {
            List<InternalNode> selectedNodes = selector.select(candidateNodes, hint);

            Assert.assertEquals(0, selectedNodes.size());
        }
    }

    public PowerOfTwoChoiceNodeSelectionTest()
            throws URISyntaxException
    {}
}