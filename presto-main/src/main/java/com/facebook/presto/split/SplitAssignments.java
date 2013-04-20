package com.facebook.presto.split;

import com.facebook.presto.execution.QueryState;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;

import javax.annotation.concurrent.Immutable;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class SplitAssignments
{
    private final Map<PlanNodeId, ? extends Split> splits;
    private final List<Node> nodes;

    public SplitAssignments(Map<PlanNodeId, ? extends Split> splits, List<Node> nodes)
    {
        this.splits = ImmutableMap.copyOf(checkNotNull(splits, "split is null"));
        this.nodes = ImmutableList.copyOf(checkNotNull(nodes, "nodes is null"));
        checkArgument(!nodes.isEmpty(), "nodes is empty");
    }

    public Split getSplit(PlanNodeId planNodeId)
    {
        checkNotNull(planNodeId);
        return checkNotNull(splits.get(planNodeId), "No split for plan node %s", planNodeId);
    }

    public Map<PlanNodeId, ? extends Split> getSplits()
    {
        return splits;
    }

    public List<Node> getNodes()
    {
        return nodes;
    }

    public static Multimap<Node, Map<PlanNodeId, ? extends Split>> balancedNodeAssignment(AtomicReference<QueryState> queryState, Iterable<SplitAssignments> splitAssignments)
    {
        final Multimap<Node, Map<PlanNodeId, ? extends Split>> result = HashMultimap.create();

        Comparator<Node> byAssignedSplitsCount = new Comparator<Node>()
        {
            @Override
            public int compare(Node o1, Node o2)
            {
                return Ints.compare(result.get(o1).size(), result.get(o2).size());
            }
        };

        for (SplitAssignments assignment : splitAssignments) {
            // if query has been canceled, exit cleanly; query will never run regardless
            if (queryState.get().isDone()) {
                return result;
            }

            // for each split, pick the node with the smallest number of assignments
            Node chosen = Ordering.from(byAssignedSplitsCount).min(assignment.getNodes());
            result.put(chosen, assignment.getSplits());
        }

        return result;
    }
}
