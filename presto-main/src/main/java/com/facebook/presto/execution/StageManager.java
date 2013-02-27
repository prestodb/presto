/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.StageExecutionPlan;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public interface StageManager
{
    List<StageInfo> getAllStage();

    StageInfo getStage(String stageId);

    StageExecution createStage(Session session, String queryId, AtomicReference<QueryState> queryState, StageExecutionPlan plan);

    void cancelStage(String stageId);
}
