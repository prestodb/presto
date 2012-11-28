package com.facebook.presto.split;

import com.facebook.presto.metadata.Node;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import javax.annotation.concurrent.Immutable;
import java.util.List;
import java.util.Random;

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

    public static Multimap<Node, Split> randomNodeAssignment(final Random random, Iterable<SplitAssignments> splitAssignments)
    {
        ImmutableListMultimap<Node,SplitAssignments> index = Multimaps.index(splitAssignments, new Function<SplitAssignments, Node>()
        {
            @Override
            public Node apply(SplitAssignments splitAssignments)
            {
                List<Node> nodes = splitAssignments.getNodes();
                return nodes.get(random.nextInt(nodes.size()));
            }
        });

        return Multimaps.transformValues(index, new Function<SplitAssignments, Split>() {
            @Override
            public Split apply(SplitAssignments splitAssignments)
            {
                return splitAssignments.getSplit();
            }
        });
    }
}
