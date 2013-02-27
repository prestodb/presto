/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.StageExecutionPlan;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;

public class SqlStageManager
        implements StageManager
{
    private final ConcurrentMap<String, StageExecution> stages = new ConcurrentHashMap<>();

    private final NodeManager nodeManager;
    private final RemoteTaskFactory remoteTaskFactory;
    private final LocationFactory locationFactory;


    @Inject
    public SqlStageManager(NodeManager nodeManager, RemoteTaskFactory remoteTaskFactory, LocationFactory locationFactory)
    {
        this.nodeManager = nodeManager;
        this.remoteTaskFactory = remoteTaskFactory;
        this.locationFactory = locationFactory;
    }

    @Override
    public List<StageInfo> getAllStage()
    {
        return ImmutableList.copyOf(filter(transform(stages.values(), new Function<StageExecution, StageInfo>()
        {
            @Override
            public StageInfo apply(StageExecution stageExecution)
            {
                try {
                    return stageExecution.getStageInfo();
                }
                catch (Exception ignored) {
                    return null;
                }
            }
        }), Predicates.notNull()));
    }

    @Override
    public StageExecution createStage(Session session,
            String queryId,
            AtomicReference<QueryState> queryState,
            StageExecutionPlan plan)
    {
        SqlStageExecution stageExecution = new SqlStageExecution(queryId, locationFactory, plan, nodeManager, remoteTaskFactory, session, queryState);
        stages.put(stageExecution.getStageId(), stageExecution);
        // todo add substages
        return stageExecution;
    }

    @Override
    public StageInfo getStage(String stageId)
    {
        Preconditions.checkNotNull(stageId, "stageId is null");

        StageExecution stageExecution = stages.get(stageId);
        if (stageExecution == null) {
            throw new NoSuchElementException("Unknown query stage " + stageId);
        }
        return stageExecution.getStageInfo();
    }

    @Override
    public void cancelStage(String stageId)
    {
        Preconditions.checkNotNull(stageId, "stageId is null");

        StageExecution stageExecution = stages.remove(stageId);
        if (stageExecution != null) {
            stageExecution.cancel();
        }
    }
}
