/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.TaskSource;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.PlanFragment;
import io.airlift.units.Duration;

import java.util.List;

public interface TaskManager
{
    List<TaskInfo> getAllTaskInfo(boolean full);

    void waitForStateChange(TaskId taskId, TaskState currentState, Duration maxWait)
            throws InterruptedException;

    TaskInfo getTaskInfo(TaskId taskId, boolean full);

    TaskInfo updateTask(Session session, TaskId taskId, PlanFragment fragment, List<TaskSource> sources, OutputBuffers outputIds);

    BufferResult getTaskResults(TaskId taskId, String outputName, long startingSequenceId, int maxPageCount, Duration maxWaitTime)
            throws InterruptedException;

    TaskInfo abortTaskResults(TaskId taskId, String outputId);

    TaskInfo cancelTask(TaskId taskId);
}
