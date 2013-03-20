/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.TaskSource;
import com.facebook.presto.operator.Page;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.PlanFragment;
import io.airlift.units.Duration;

import java.util.List;

public interface TaskManager
{
    List<TaskInfo> getAllTaskInfo();

    TaskInfo getTaskInfo(String taskId);

    TaskInfo updateTask(Session session, String queryId, String stageId, String taskId, PlanFragment fragment, List<TaskSource> sources, OutputBuffers outputIds);

    List<Page> getTaskResults(String taskId, String outputName, int maxPageCount, Duration maxWaitTime)
            throws InterruptedException;

    TaskInfo abortTaskResults(String taskId, String outputId);

    TaskInfo cancelTask(String taskId);
}
