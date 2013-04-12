/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.metadata.Node;
import com.facebook.presto.split.Split;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.OutputReceiver;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.Multimap;

import java.net.URI;
import java.util.Map;
import java.util.Set;

public interface RemoteTaskFactory
{
    RemoteTask createRemoteTask(Session session,
            TaskId taskId,
            Node node,
            PlanFragment fragment,
            Map<PlanNodeId, ? extends Split> initialSplits,
            Map<PlanNodeId, OutputReceiver> outputReceivers,
            Multimap<PlanNodeId, URI> initialExchangeLocations,
            Set<String> initialOutputIds);
}
