/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.sql.planner.ExecutionPlanner;
import com.facebook.presto.sql.planner.JoinNode;

import java.util.IdentityHashMap;

public class SourceHashProviderFactory
{
    // TODO: assign ids to each JoinNode instead of using identity hashmap
    private final IdentityHashMap<JoinNode, SourceHashProvider> joinHashes = new IdentityHashMap<>();

    public synchronized SourceHashProvider getSourceHashProvider(JoinNode node, ExecutionPlanner executionPlanner, int channel)
    {
        SourceHashProvider hashProvider = joinHashes.get(node);
        if (hashProvider == null) {
            Operator rightOperator = executionPlanner.plan(node.getRight());
            hashProvider = new SourceHashProvider(rightOperator, channel, 1_500_000);
            joinHashes.put(node, hashProvider);
        }
        return hashProvider;
    }
}
