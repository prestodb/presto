/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.log.Logger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.execution.TaskInfo.taskStateGetter;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.collect.Iterables.all;
import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.transform;

@ThreadSafe
public class SqlStageExecution
        implements StageExecution
{
    private static final Logger log = Logger.get(SqlStageExecution.class);

    private final String queryId;
    private final String stageId;
    private final URI location;
    private final PlanFragment plan;
    private final List<RemoteTask> tasks;
    private final List<StageExecution> subStages;

    // Changes to state must happen within a synchronized lock.
    // The only reason we use an atomic reference here is so read-only threads don't have to block.
    @GuardedBy("this")
    private final AtomicReference<StageState> stageState = new AtomicReference<>(StageState.PLANNED);

    public SqlStageExecution(String queryId,
            String stageId,
            URI location,
            PlanFragment plan,
            Iterable<? extends RemoteTask> tasks,
            Iterable<? extends StageExecution> subStages)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(stageId, "stageId is null");
        Preconditions.checkNotNull(location, "location is null");
        Preconditions.checkNotNull(plan, "plan is null");
        Preconditions.checkNotNull(tasks, "tasks is null");
        Preconditions.checkArgument(!Iterables.isEmpty(tasks), "tasks is empty");
        Preconditions.checkNotNull(subStages, "subStages is null");

        this.queryId = queryId;
        this.stageId = stageId;
        this.location = location;
        this.plan = plan;
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
                stageState.get(),
                location,
                plan,
                taskInfos,
                subStageInfos);
    }

    public void startTasks()
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Can not start while holding a lock on this");

        // transition to scheduling
        synchronized (this) {
            Preconditions.checkState(stageState.compareAndSet(StageState.PLANNED, StageState.SCHEDULING), "Stage has already been started");
        }

        try {
            // start tasks out side of loop
            for (RemoteTask task : tasks) {
                task.start();
            }

            synchronized (this) {
                // only transition to scheduled if still in the scheduling stage
                // another thread may have canceled the execution while scheduling
                stageState.compareAndSet(StageState.SCHEDULING, StageState.SCHEDULED);
            }
        }
        catch (Throwable e) {
            synchronized (this) {
                stageState.set(StageState.FAILED);
            }
            log.error(e, "Stage %s failed to start", stageId);
            cancel();
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void updateState()
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Can not update state while holding a lock on this");

        // propagate update to tasks and stages
        for (RemoteTask task : tasks) {
            task.updateState();
        }
        for (StageExecution subStage : subStages) {
            subStage.updateState();
        }

        synchronized (this) {
            if (stageState.get().isDone()) {
                return;
            }

            List<TaskState> taskStates = ImmutableList.copyOf(transform(transform(tasks, taskInfoGetter()), taskStateGetter()));
            if (any(taskStates, equalTo(TaskState.FAILED))) {
                stageState.set(StageState.FAILED);
            } else if (all(taskStates, TaskState.inDoneState())) {
                stageState.set(StageState.FINISHED);
            } else if (any(taskStates, equalTo(TaskState.RUNNING))) {
                stageState.set(StageState.RUNNING);
            } else if (any(taskStates, equalTo(TaskState.QUEUED))) {
                stageState.set(StageState.SCHEDULED);
            }
        }

        if (stageState.get().isDone()) {
            // finish tasks and stages
            cancelAll();
        }
    }

    @Override
    public void cancel()
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Can not cancel while holding a lock on this");

        // transition to canceled state, only if not already finished
        synchronized (this) {
            if (stageState.get().isDone()) {
                return;
            }
            stageState.set(StageState.CANCELED);
        }

        cancelAll();
    }

    private void cancelAll()
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Can not cancel while holding a lock on this");

        // propagate update to tasks and stages
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
