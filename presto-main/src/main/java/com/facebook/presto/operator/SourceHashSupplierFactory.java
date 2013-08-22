/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.base.Preconditions;

import java.util.IdentityHashMap;

public class SourceHashSupplierFactory
{
    // TODO: assign ids to each JoinNode instead of using identity hashmap
    private final IdentityHashMap<JoinNode, SourceHashSupplier> joinHashes = new IdentityHashMap<>();

    private final TaskMemoryManager taskMemoryManager;

    public SourceHashSupplierFactory(TaskMemoryManager taskMemoryManager)
    {
        Preconditions.checkNotNull(taskMemoryManager, "taskMemoryManager is null");
        this.taskMemoryManager = taskMemoryManager;
    }

    public synchronized SourceHashSupplier getSourceHashSupplier(JoinNode node, Operator rightOperator, int channel, OperatorStats operatorStats)
    {
        SourceHashSupplier hashSupplier = joinHashes.get(node);
        if (hashSupplier == null) {
            hashSupplier = new SourceHashSupplier(rightOperator, channel, 1_500_000, taskMemoryManager, operatorStats);
            joinHashes.put(node, hashSupplier);
        }
        return hashSupplier;
    }
}
