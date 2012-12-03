/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.sql.planner.ExecutionPlanner;
import com.facebook.presto.sql.planner.JoinNode;

import java.util.IdentityHashMap;

public class SourceHashProviderFactory
{
    private final IdentityHashMap<JoinNode, SourceHashProvider> joinHashes = new IdentityHashMap<>();

    public SourceHashProvider getSourceHashProvider(JoinNode node, ExecutionPlanner executionPlanner)
    {
        SourceHashProvider hashProvider = joinHashes.get(node);
        if (hashProvider == null) {
            Operator rightOperator = executionPlanner.plan(node.getRight());
            hashProvider = new SourceHashProvider(rightOperator, 0, 1_000_000);
            joinHashes.put(node, hashProvider);
        }
        return hashProvider;
    }
}
