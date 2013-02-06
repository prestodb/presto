/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.Node;
import com.facebook.presto.split.Split;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class Partition
{
    private final Node node;
    private final List<Split> splits;

    public Partition(Node node, Iterable<? extends Split> splits)
    {
        this.node = node;
        this.splits = ImmutableList.copyOf(splits);
    }

    public Node getNode()
    {
        return node;
    }

    public List<Split> getSplits()
    {
        return splits;
    }

    public static Function<Partition, String> nodeIdentifierGetter()
    {
        return new Function<Partition, String>()
        {
            @Override
            public String apply(Partition partition)
            {
                return partition.getNode().getNodeIdentifier();
            }
        };
    }
}
