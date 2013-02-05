/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.operator.Page;
import com.facebook.presto.split.Split;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import io.airlift.units.Duration;

import java.util.List;

public interface TaskExecution
{

    String getTaskId();

    TaskInfo getTaskInfo();

    void addSource(PlanNodeId sourceId, Split split);

    void noMoreSources(String sourceId);

    void cancel();

    void fail(Throwable cause);

    List<Page> getResults(String outputId, int maxPageCount, Duration maxWait)
            throws InterruptedException;

    void abortResults(String outputId);
}
