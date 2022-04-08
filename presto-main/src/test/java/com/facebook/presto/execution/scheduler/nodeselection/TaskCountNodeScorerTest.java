package com.facebook.presto.execution.scheduler.nodeselection;

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeScorer;
import com.facebook.presto.execution.scheduler.nodeSelection.TaskCountNodeScorer;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.util.FinalizerService;
import org.testng.annotations.Test;

import java.net.URI;

import static org.testng.Assert.assertEquals;

public class TaskCountNodeScorerTest
{
    @Test
    public void testScorerToProduceScoreForMissingNode()
            throws Exception
    {
        InternalNode node = new InternalNode("1234", new URI("/"), NodeVersion.UNKNOWN, false);
        NodeTaskMap nodeTaskMap = new NodeTaskMap(new FinalizerService());

        NodeScorer nodeScorer = new TaskCountNodeScorer(nodeTaskMap);

        assertEquals(nodeScorer.score(node), 0);
    }
}