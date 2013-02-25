/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.split.Split;
import com.facebook.presto.sql.planner.plan.PlanNodeId;

public interface RemoteTask
{
    String getTaskId();

    TaskInfo getTaskInfo();

    void start();

    void addSource(PlanNodeId sourceId, Split split);

    void noMoreSources(PlanNodeId sourceId);

    void cancel();

    void updateState();
}
