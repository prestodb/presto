package com.facebook.presto.execution.scheduler.nodeSelection;

import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.metadata.InternalNode;
import com.google.inject.Inject;

public class TaskCountNodeScorer implements NodeScorer
{
    private final NodeTaskMap nodeTaskMap;

    @Inject
    public TaskCountNodeScorer(NodeTaskMap nodeTaskMap) {
        this.nodeTaskMap = nodeTaskMap;
    }

    @Override
    public long score(InternalNode node)
    {
        return nodeTaskMap.getPartitionedSplitsOnNode(node).getCount();
    }
}
