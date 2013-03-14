/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.operator.SourceHashProviderFactory;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.server.ExchangeOperatorFactory;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.split.Split;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.lang.ref.WeakReference;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SqlTaskExecution
        implements TaskExecution
{
    private final String taskId;
    private final TaskOutput taskOutput;
    private final DataStreamProvider dataStreamProvider;
    private final ExchangeOperatorFactory exchangeOperatorFactory;
    private final ListeningExecutorService shardExecutor;
    private final PlanFragment fragment;
    private final Metadata metadata;
    private final DataSize maxOperatorMemoryUsage;
    private final Session session;

    private final AtomicInteger pendingWorkerCount = new AtomicInteger();

    @GuardedBy("this")
    private final SetMultimap<PlanNodeId, Split> unpartitionedSources = HashMultimap.create();
    @GuardedBy("this")
    private final List<WeakReference<SplitWorker>> splitWorkers = new ArrayList<>();
    @GuardedBy("this")
    private SourceHashProviderFactory sourceHashProviderFactory;

    private final BlockingDeque<FutureTask<?>> unfinishedWorkerTasks = new LinkedBlockingDeque<>();

    public static SqlTaskExecution createSqlTaskExecution(Session session,
            String queryId,
            String stageId,
            String taskId,
            URI location,
            PlanFragment fragment,
            Map<PlanNodeId, Set<Split>> initialSources,
            List<String> initialOutputIds,
            int pageBufferMax,
            DataStreamProvider dataStreamProvider,
            ExchangeOperatorFactory exchangeOperatorFactory,
            Metadata metadata,
            ExecutorService taskMasterExecutor,
            ListeningExecutorService shardExecutor,
            DataSize maxOperatorMemoryUsage)
    {
        SqlTaskExecution task = new SqlTaskExecution(session,
                queryId,
                stageId,
                taskId,
                location,
                fragment,
                initialOutputIds,
                pageBufferMax,
                dataStreamProvider,
                exchangeOperatorFactory,
                metadata,
                shardExecutor,
                maxOperatorMemoryUsage);

        task.start(taskMasterExecutor, initialSources);

        return task;
    }

    private SqlTaskExecution(Session session,
            String queryId,
            String stageId,
            String taskId,
            URI location,
            PlanFragment fragment,
            List<String> initialOutputIds,
            int pageBufferMax,
            DataStreamProvider dataStreamProvider,
            ExchangeOperatorFactory exchangeOperatorFactory,
            Metadata metadata,
            ListeningExecutorService shardExecutor,
            DataSize maxOperatorMemoryUsage)
    {
        Preconditions.checkNotNull(session, "session is null");
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(stageId, "stageId is null");
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(fragment, "fragment is null");
        Preconditions.checkNotNull(initialOutputIds, "initialOutputIds is null");
        Preconditions.checkArgument(pageBufferMax > 0, "pageBufferMax must be at least 1");
        Preconditions.checkNotNull(metadata, "metadata is null");
        Preconditions.checkNotNull(shardExecutor, "shardExecutor is null");
        Preconditions.checkNotNull(maxOperatorMemoryUsage, "maxOperatorMemoryUsage is null");

        this.session = session;
        this.taskId = taskId;
        this.fragment = fragment;
        this.dataStreamProvider = dataStreamProvider;
        this.exchangeOperatorFactory = exchangeOperatorFactory;
        this.shardExecutor = shardExecutor;
        this.metadata = metadata;
        this.maxOperatorMemoryUsage = maxOperatorMemoryUsage;

        // create output buffers
        this.taskOutput = new TaskOutput(queryId, stageId, taskId, location, initialOutputIds, pageBufferMax);
    }

    //
    // This code starts threads so it can not be in the constructor
    // TODO: merge the partitioned and unparitioned paths somehow
    private void start(ExecutorService taskMasterExecutor, Map<PlanNodeId, Set<Split>> initialSources)
    {
        // if plan is unpartitioned, add a worker
        if (!fragment.isPartitioned()) {
            scheduleSplitWorker(null, null);
        }

        // add all the splits
        for (Entry<PlanNodeId, Set<Split>> entry : initialSources.entrySet()) {
            PlanNodeId planNodeId = entry.getKey();
            for (Split split : entry.getValue()) {
                addSplit(planNodeId, split);
            }
        }

        // NOTE: this must be started after the unpartitioned task or the task can be ended early
        taskMasterExecutor.submit(new TaskWorker(taskOutput, unfinishedWorkerTasks));
    }

    @Override
    public String getTaskId()
    {
        return taskId;
    }

    @Override
    public TaskInfo getTaskInfo()
    {
        checkTaskCompletion();
        return taskOutput.getTaskInfo();
    }

    @Override
    public synchronized void addSplit(PlanNodeId sourceId, Split split)
    {
        // is this a partitioned source
        if (fragment.isPartitioned() && fragment.getPartitionedSource().equals(sourceId)) {
            scheduleSplitWorker(sourceId, split);
        }
        else {
            // add this to all of the existing workers
            unpartitionedSources.put(sourceId, split);
            // mark split as started since all worker are will be reading from it
            getTaskInfo().getStats().splitStarted();
            for (WeakReference<SplitWorker> workerReference : splitWorkers) {
                SplitWorker worker = workerReference.get();
                // this should not happen until the all sources have been closed
                Preconditions.checkState(worker != null, "SplitWorker has been GCed");
                worker.addSplit(sourceId, split);
            }
        }
    }

    private synchronized void scheduleSplitWorker(@Nullable PlanNodeId partitionedSourceId, @Nullable Split partitionedSplit)
    {
        // create a new split worker
        SplitWorker worker = new SplitWorker(session,
                taskOutput,
                fragment,
                getSourceHashProviderFactory(),
                metadata,
                maxOperatorMemoryUsage,
                dataStreamProvider,
                exchangeOperatorFactory);

        // TableScanOperator requires partitioned split to be added before task is started
        if (partitionedSourceId != null) {
            worker.addSplit(partitionedSourceId, partitionedSplit);
        }

        // add unpartitioned sources
        for (Entry<PlanNodeId, Split> entry : unpartitionedSources.entries()) {
            worker.addSplit(entry.getKey(), entry.getValue());
        }

        // record new worker
        splitWorkers.add(new WeakReference<>(worker));
        pendingWorkerCount.incrementAndGet();
        getTaskInfo().getStats().addSplits(1);

        // execute worker
        final FutureTask<?> workerFutureTask = new FutureTask<>(worker);
        unfinishedWorkerTasks.addFirst(workerFutureTask);
        ListenableFuture<?> future = shardExecutor.submit(workerFutureTask);
        Futures.addCallback(future, new FutureCallback<Object>()
        {
            @Override
            public void onSuccess(Object result)
            {
                pendingWorkerCount.decrementAndGet();
                checkTaskCompletion();
                // free the reference to the this task in the scheduled workers, so the memory can be released
                unfinishedWorkerTasks.removeFirstOccurrence(workerFutureTask);
            }

            @Override
            public void onFailure(Throwable t)
            {
                taskOutput.queryFailed(t);
                pendingWorkerCount.decrementAndGet();
                // free the reference to the this task in the scheduled workers, so the memory can be released
                unfinishedWorkerTasks.removeFirstOccurrence(workerFutureTask);
            }
        });
    }

    @Override
    public synchronized void noMoreSplits(PlanNodeId sourceId)
    {
        taskOutput.noMoreSplits(sourceId);
        if (sourceId.equals(fragment.getPartitionedSource())) {
            // all workers have been created
            // clear hash provider since it has a hard reference to every hash table
            sourceHashProviderFactory = null;
        }
        else {
            // add this to all of the existing workers
            for (WeakReference<SplitWorker> workerReference : splitWorkers) {
                SplitWorker worker = workerReference.get();
                // this should not happen until the all sources have been closed
                Preconditions.checkState(worker != null, "SplitWorker has been GCed");
                worker.noMoreSplits(sourceId);
            }
        }
        checkTaskCompletion();
    }

    private synchronized void checkTaskCompletion()
    {
        // are there more partition splits expected?
        if (fragment.isPartitioned() && !taskOutput.getNoMoreSplits().contains(fragment.getPartitionedSource())) {
            return;
        }
        // do we still have running tasks?
        if (pendingWorkerCount.get() != 0) {
            return;
        }
        // Cool! All done!
        taskOutput.finish();
    }

    private synchronized SourceHashProviderFactory getSourceHashProviderFactory()
    {
        if (sourceHashProviderFactory == null) {
            sourceHashProviderFactory = new SourceHashProviderFactory(maxOperatorMemoryUsage);
        }
        return sourceHashProviderFactory;
    }

    @Override
    public void cancel()
    {
        taskOutput.cancel();
    }

    @Override
    public void fail(Throwable cause)
    {
        taskOutput.queryFailed(cause);
    }

    @Override
    public void addResultQueue(String outputName)
    {
        taskOutput.addResultQueue(outputName);
    }

    @Override
    public List<Page> getResults(String outputId, int maxPageCount, Duration maxWait)
            throws InterruptedException
    {
        return taskOutput.getResults(outputId, maxPageCount, maxWait);
    }

    @Override
    public void noMoreResultQueues()
    {
        taskOutput.noMoreResultQueues();
    }

    @Override
    public void abortResults(String outputId)
    {
        taskOutput.abortResults(outputId);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("taskId", taskId)
                .add("unpartitionedSources", unpartitionedSources)
                .add("taskOutput", taskOutput)
                .toString();
    }

    private static class TaskWorker
            implements Callable<Void>
    {
        private final TaskOutput taskOutput;
        private final BlockingDeque<FutureTask<?>> scheduledWorkers;

        private TaskWorker(TaskOutput taskOutput, BlockingDeque<FutureTask<?>> scheduledWorkers)
        {
            this.taskOutput = taskOutput;
            this.scheduledWorkers = scheduledWorkers;
        }

        @Override
        public Void call()
                throws InterruptedException
        {
            while (!taskOutput.getState().isDone()) {
                FutureTask<?> futureTask = scheduledWorkers.pollFirst(1, TimeUnit.SECONDS);
                // if we got a task and the state is not done, run the task
                if (futureTask != null && taskOutput.getState().isDone()) {
                    futureTask.run();
                }
            }

            // assure all tasks are complete
            for (FutureTask<?> futureTask = scheduledWorkers.poll(); futureTask != null; futureTask = scheduledWorkers.poll()) {
                futureTask.cancel(true);
            }

            return null;
        }
    }

    private static class SplitWorker
            implements Callable<Void>
    {
        private final AtomicBoolean started = new AtomicBoolean();
        private final TaskOutput taskOutput;
        private final Operator operator;
        private final OperatorStats operatorStats;
        private final Map<PlanNodeId, SourceOperator> sourceOperators;

        private SplitWorker(Session session,
                TaskOutput taskOutput,
                PlanFragment fragment,
                SourceHashProviderFactory sourceHashProviderFactory,
                Metadata metadata,
                DataSize maxOperatorMemoryUsage,
                DataStreamProvider dataStreamProvider,
                ExchangeOperatorFactory exchangeOperatorFactory)
        {
            this.taskOutput = taskOutput;

            operatorStats = new OperatorStats(taskOutput);

            LocalExecutionPlanner planner = new LocalExecutionPlanner(session,
                    metadata,
                    fragment.getSymbols(),
                    operatorStats,
                    sourceHashProviderFactory,
                    maxOperatorMemoryUsage,
                    dataStreamProvider,
                    exchangeOperatorFactory);

            LocalExecutionPlan localExecutionPlan = planner.plan(fragment.getRoot());
            operator = localExecutionPlan.getRootOperator();
            sourceOperators = localExecutionPlan.getSourceOperators();
        }

        public void addSplit(PlanNodeId sourceId, Split split)
        {
            SourceOperator sourceOperator = sourceOperators.get(sourceId);
            Preconditions.checkArgument(sourceOperator != null, "Unknown plan source %s; known sources are %s", sourceId, sourceOperators.keySet());
            sourceOperator.addSplit(split);
        }

        public void noMoreSplits(PlanNodeId sourceId)
        {
            SourceOperator sourceOperator = sourceOperators.get(sourceId);
            Preconditions.checkArgument(sourceOperator != null, "Unknown plan source %s; known sources are %s", sourceId, sourceOperators.keySet());
            sourceOperator.noMoreSplits();
        }

        @Override
        public Void call()
                throws InterruptedException
        {
            if (!started.compareAndSet(false, true)) {
                return null;
            }

            if (taskOutput.getState().isDone()) {
                return null;
            }

            taskOutput.getStats().recordExecutionStart();
            operatorStats.start();
            try (PageIterator pages = operator.iterator(operatorStats)) {
                while (pages.hasNext()) {
                    Page page = pages.next();
                    taskOutput.getStats().addOutputDataSize(page.getDataSize());
                    taskOutput.getStats().addOutputPositions(page.getPositionCount());
                    if (!taskOutput.addPage(page)) {
                        break;
                    }
                }
            }
            finally {
                operatorStats.finish();
            }
            return null;
        }
    }
}
