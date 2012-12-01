/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.operator.Page;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragmentSource;
import io.airlift.units.Duration;

import java.util.List;
import java.util.Map;

public interface QueryTaskManager
{
    List<QueryTaskInfo> getAllQueryTaskInfo();

    QueryTask createQueryTask(PlanFragment planFragment, Map<String, List<PlanFragmentSource>> fragmentSources);

    QueryTaskInfo getQueryTaskInfo(String taskId);

    List<Page> getQueryTaskResults(String taskId, String outputName, int maxPageCount, Duration maxWaitTime)
            throws InterruptedException;

    void cancelQueryTask(String taskId);
}
