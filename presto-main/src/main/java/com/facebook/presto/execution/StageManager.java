/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.split.SplitAssignments;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.base.Optional;

import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public interface StageManager
{
    List<StageInfo> getAllStage();

    StageInfo getStage(String stageId);

    StageExecution createStage(Session session,
            String queryId,
            String stageId,
            URI location,
            AtomicReference<QueryState> queryState,
            PlanFragment plan,
            Optional<Iterable<SplitAssignments>> tasks,
            Iterable<? extends StageExecution> subStages);

    void cancelStage(String stageId);
}
