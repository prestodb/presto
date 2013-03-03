/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.operator.Page;
import com.facebook.presto.split.Split;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import io.airlift.units.Duration;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface TaskManager
{
    List<TaskInfo> getAllTaskInfo();

    TaskInfo createTask(Session session,
            String queryId,
            String stageId,
            String taskId,
            PlanFragment fragment,
            Map<PlanNodeId, Set<Split>> initialSources,
            List<String> initialOutputIds);

    TaskInfo getTaskInfo(String taskId);

    TaskInfo addResultQueue(String taskId, String outputName);

    List<Page> getTaskResults(String taskId, String outputName, int maxPageCount, Duration maxWaitTime)
            throws InterruptedException;

    TaskInfo noMoreResultQueues(String taskId);

    TaskInfo addSplit(String taskId, PlanNodeId sourceId, Split source);

    TaskInfo noMoreSplits(String taskId, PlanNodeId sourceId);

    TaskInfo abortTaskResults(String taskId, String outputId);

    TaskInfo cancelTask(String taskId);
}
