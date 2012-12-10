/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;

public class SqlStageManager
        implements StageManager
{
    private final ConcurrentMap<String, StageExecution> stages = new ConcurrentHashMap<>();

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
    public StageExecution createStage(String queryId, String stageId, URI location, Iterable<? extends RemoteTask> tasks, Iterable<? extends StageExecution> subStages)
    {
        SqlStageExecution stageExecution = new SqlStageExecution(queryId, stageId, location, tasks, subStages);
        stages.put(stageExecution.getStageId(), stageExecution);
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
