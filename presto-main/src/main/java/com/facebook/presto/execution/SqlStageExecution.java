/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.log.Logger;

import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.execution.TaskInfo.taskStateGetter;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.collect.Iterables.all;
import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.transform;

public class SqlStageExecution
        implements StageExecution
{
    private static final Logger log = Logger.get(SqlStageExecution.class);

    private final String queryId;
    private final String stageId;
    private final URI location;
    private final List<RemoteTask> tasks;
    private final List<StageExecution> subStages;

    private final AtomicReference<StageState> stageState = new AtomicReference<>(StageState.PLANNED);

    public SqlStageExecution(String queryId,
            String stageId,
            URI location,
            Iterable<? extends RemoteTask> tasks,
            Iterable<? extends StageExecution> subStages)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(stageId, "stageId is null");
        Preconditions.checkNotNull(location, "location is null");
        Preconditions.checkNotNull(tasks, "tasks is null");
        Preconditions.checkArgument(!Iterables.isEmpty(tasks), "tasks is empty");
        Preconditions.checkNotNull(subStages, "subStages is null");

        this.queryId = queryId;
        this.stageId = stageId;
        this.location = location;
        this.subStages = ImmutableList.copyOf(subStages);
        this.tasks = ImmutableList.copyOf(tasks);
    }

    @Override
    public String getStageId()
    {
        return stageId;
    }

    @Override
    public List<StageExecution> getSubStages()
    {
        return subStages;
    }

    @Override
    public ExchangePlanFragmentSource getExchangeSourceFor(String outputId)
    {
        Preconditions.checkNotNull(outputId, "outputId is null");

        // get locations for the dependent stage
        ImmutableMap.Builder<String, URI> sources = ImmutableMap.builder();
        for (RemoteTask task : tasks) {
            sources.put(task.getTaskId(), task.getTaskInfo().getSelf());
        }

        return new ExchangePlanFragmentSource(sources.build(),
                outputId,
                tasks.get(0).getTaskInfo().getTupleInfos());
    }

    @Override
    public StageInfo getStageInfo()
    {
        List<TaskInfo> taskInfos = IterableTransformer.on(tasks).transform(taskInfoGetter()).list();
        List<StageInfo> subStageInfos = IterableTransformer.on(subStages).transform(stageInfoGetter()).list();

        return new StageInfo(queryId,
                stageId,
                location,
                stageState.get(),
                taskInfos,
                subStageInfos);
    }

    public void start()
    {
        try {
            for (RemoteTask task : tasks) {
                task.start();
            }
        }
        catch (Throwable e) {
            stageState.set(StageState.FAILED);
            log.error(e, "Stage %s failed to start", stageId);
            cancel();
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void updateState()
    {
        for (RemoteTask task : tasks) {
            task.updateState();
        }

        for (StageExecution subStage : subStages) {
            subStage.updateState();
        }

        if (stageState.get().isDone()) {
            return;
        }

        List<TaskState> taskStates = ImmutableList.copyOf(transform(transform(tasks, taskInfoGetter()), taskStateGetter()));
        if (any(taskStates, equalTo(TaskState.FAILED))) {
            stageState.set(StageState.FAILED);
            cancelAll();
        } else if (all(taskStates, TaskState.inDoneState())) {
            stageState.set(StageState.FINISHED);
        } else if (any(taskStates, equalTo(TaskState.RUNNING))) {
            stageState.set(StageState.RUNNING);
        } else if (any(taskStates, equalTo(TaskState.QUEUED))) {
            stageState.set(StageState.SCHEDULED);
        }
    }

    @Override
    public void cancel()
    {
        while (true) {
            StageState stageState = this.stageState.get();
            if (stageState.isDone()) {
                return;
            }
            if (this.stageState.compareAndSet(stageState, StageState.CANCELED)) {
                break;
            }
        }

        cancelAll();
    }

    private void cancelAll()
    {
        for (RemoteTask task : tasks) {
            task.cancel();
        }
        for (StageExecution subStage : subStages) {
            subStage.cancel();
        }
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("stageId", stageId)
                .add("location", location)
                .add("stageState", stageState.get())
                .toString();
    }

    public static Function<RemoteTask, TaskInfo> taskInfoGetter()
    {
        return new Function<RemoteTask, TaskInfo>()
        {
            @Override
            public TaskInfo apply(RemoteTask remoteTask)
            {
                return remoteTask.getTaskInfo();
            }
        };
    }
    public static Function<StageExecution, StageInfo> stageInfoGetter()
    {
        return new Function<StageExecution, StageInfo>()
        {
            @Override
            public StageInfo apply(StageExecution stage)
            {
                return stage.getStageInfo();
            }
        };
    }
}
