/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.metadata.Node;
import com.facebook.presto.split.Split;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.Multimap;

import java.net.URI;
import java.util.Set;

public interface RemoteTaskFactory
{
    RemoteTask createRemoteTask(Session session,
            String queryId,
            String stageId,
            String taskId,
            Node node,
            PlanFragment fragment,
            Split initialSplit, Multimap<PlanNodeId, URI> initialExchangeLocations,
            Set<String> initialOutputIds);
}
