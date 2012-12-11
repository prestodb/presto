/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.sql.planner.PlanFragment;

import java.net.URI;
import java.util.List;

public interface StageManager
{
    List<StageInfo> getAllStage();

    StageInfo getStage(String stageId);

    StageExecution createStage(String queryId,
            String stageId,
            URI location,
            PlanFragment plan,
            Iterable<? extends RemoteTask> tasks,
            Iterable<? extends StageExecution> subStages);

    void cancelStage(String stageId);
}
