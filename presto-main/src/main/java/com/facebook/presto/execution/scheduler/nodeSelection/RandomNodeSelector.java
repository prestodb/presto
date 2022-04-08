package com.facebook.presto.execution.scheduler.nodeSelection;

import com.facebook.presto.execution.scheduler.ResettableRandomizedIterator;
import com.facebook.presto.metadata.InternalNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

import java.util.List;

/**
 * This NodeSelector makes a selection from the candidates randomly. The selection
 * is strictly random and based on ResettableRandomizedIterator
 */
public class RandomNodeSelector
        implements INodeSelector
{
    @Override
    public List<InternalNode> select(List<InternalNode> candidates, NodeSelectionHint hint)
    {
        return Streams.stream(new ResettableRandomizedIterator<>(candidates))
                .filter(node -> hint.canIncludeCoordinator() || !node.isCoordinator())
                .limit(hint.getLimit().orElse(Long.MAX_VALUE))
                .collect(ImmutableList.toImmutableList());
    }
}
