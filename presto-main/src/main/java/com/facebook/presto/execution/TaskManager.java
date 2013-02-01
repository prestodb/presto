/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.operator.Page;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragmentSource;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import io.airlift.units.Duration;

import java.util.List;
import java.util.Map;

public interface TaskManager
{
    List<TaskInfo> getAllTaskInfo();

    TaskInfo createTask(Session session,
            String queryId,
            String stageId,
            String taskId,
            PlanFragment fragment,
            List<PlanFragmentSource> splits,
            Map<PlanNodeId, ExchangePlanFragmentSource> exchangeSources,
            List<String> outputIds);

    TaskInfo getTaskInfo(String taskId);

    List<Page> getTaskResults(String taskId, String outputName, int maxPageCount, Duration maxWaitTime)
            throws InterruptedException;

    void abortTaskResults(String taskId, String outputId);

    void cancelTask(String taskId);
}
