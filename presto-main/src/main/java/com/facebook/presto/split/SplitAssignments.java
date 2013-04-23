package com.facebook.presto.split;

import com.facebook.presto.execution.QueryState;
import com.facebook.presto.metadata.Node;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;

import javax.annotation.concurrent.Immutable;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class SplitAssignments
{
    private final Split split;
    private final List<Node> nodes;

    public SplitAssignments(Split split, List<Node> nodes)
    {
        this.split = checkNotNull(split, "split is null");
        this.nodes = ImmutableList.copyOf(checkNotNull(nodes, "nodes is null"));
        checkArgument(!nodes.isEmpty(), "nodes is empty");
    }

    public Split getSplit()
    {
        return split;
    }

    public List<Node> getNodes()
    {
        return nodes;
    }

    public static Multimap<Node, Split> balancedNodeAssignment(AtomicReference<QueryState> queryState, Iterable<SplitAssignments> splitAssignments)
    {
        final Multimap<Node, Split> result = HashMultimap.create();

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
            result.put(chosen, assignment.getSplit());
        }

        return result;
    }
}
