/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.google.common.base.Preconditions;

import java.util.IdentityHashMap;

public class JoinBuildSourceSupplierFactory
{
    // TODO: assign ids to each JoinNode instead of using identity hashmap
    private final IdentityHashMap<JoinNode, SourceHashSupplier> joinHashes = new IdentityHashMap<>();
    private final IdentityHashMap<SemiJoinNode, SourceSetSupplier> semiJoinSets = new IdentityHashMap<>();

    private final TaskMemoryManager taskMemoryManager;

    public JoinBuildSourceSupplierFactory(TaskMemoryManager taskMemoryManager)
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

    public synchronized SourceSetSupplier getSourceSetSupplier(SemiJoinNode node, Operator filteringSource, int channel, OperatorStats operatorStats)
    {
        SourceSetSupplier setSupplier = semiJoinSets.get(node);
        if (setSupplier == null) {
            setSupplier = new SourceSetSupplier(filteringSource, channel, 1_500_000, taskMemoryManager, operatorStats);
            semiJoinSets.put(node, setSupplier);
        }
        return setSupplier;
    }
}
