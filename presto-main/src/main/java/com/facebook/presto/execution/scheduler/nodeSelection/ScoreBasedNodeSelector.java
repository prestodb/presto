package com.facebook.presto.execution.scheduler.nodeSelection;

import com.facebook.presto.metadata.InternalNode;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.longs.LongComparator;
import it.unimi.dsi.fastutil.longs.LongComparators;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.objects.Object2LongArrayMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;

import java.util.Comparator;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * This NodeSelector uses node score generated from the passed NodeScorer
 * and ranks them using the provided comparator to select the nodes with
 * highest scores.
 */
public class ScoreBasedNodeSelector
        implements INodeSelector
{
    private final NodeScorer nodeScorer;
    private final LongComparator scoreComparator;

    public ScoreBasedNodeSelector(NodeScorer nodeScorer, LongComparator scoreComparator)
    {
        this.nodeScorer = requireNonNull(nodeScorer, "NodeScorer cannot be null");
        this.scoreComparator = LongComparators.oppositeComparator(requireNonNull(scoreComparator, "Comparator cannot be null"));
    }

    @Override
    public List<InternalNode> select(List<InternalNode> candidates, NodeSelectionHint hint)
    {
        Object2LongMap<InternalNode> nodeScoreMap = new Object2LongArrayMap<>(candidates.size());
        LongList scores = nodeScorer.score(candidates);

        for (int i = 0; i < candidates.size(); i++) {
            nodeScoreMap.put(candidates.get(i), scores.getLong(i));
        }

        return candidates.stream()
                .filter(node -> hint.canIncludeCoordinator() || !node.isCoordinator())
                .sorted(Comparator.comparing(nodeScoreMap::getLong, scoreComparator))
                .limit(hint.getLimit().orElse(Long.MAX_VALUE))
                .collect(ImmutableList.toImmutableList());
    }
}
