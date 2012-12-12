/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.metadata.Node;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragmentSource;

import java.util.List;
import java.util.Map;

public interface RemoteTaskFactory
{
    RemoteTask createRemoteTask(String queryId,
            String stageId,
            String taskId,
            Node node,
            PlanFragment fragment,
            List<PlanFragmentSource> splits,
            Map<String, ExchangePlanFragmentSource> exchangeSources,
            List<String> outputIds);
}
