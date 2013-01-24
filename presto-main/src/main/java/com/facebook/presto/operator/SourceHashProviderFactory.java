/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.base.Preconditions;
import io.airlift.units.DataSize;

import java.util.IdentityHashMap;

public class SourceHashProviderFactory
{
    // TODO: assign ids to each JoinNode instead of using identity hashmap
    private final IdentityHashMap<JoinNode, SourceHashProvider> joinHashes = new IdentityHashMap<>();

    private final DataSize maxSize;

    public SourceHashProviderFactory(DataSize maxSize)
    {
        Preconditions.checkNotNull(maxSize, "maxSize is null");
        this.maxSize = maxSize;
    }

    public synchronized SourceHashProvider getSourceHashProvider(JoinNode node, Operator rightOperator, int channel, OperatorStats operatorStats)
    {
        SourceHashProvider hashProvider = joinHashes.get(node);
        if (hashProvider == null) {
            hashProvider = new SourceHashProvider(rightOperator, channel, 1_500_000, maxSize, operatorStats);
            joinHashes.put(node, hashProvider);
        }
        return hashProvider;
    }
}
