/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.ScheduledSplit;
import com.facebook.presto.TaskSource;
import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.metadata.LocalStorageManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.OutputProducingOperator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.operator.SourceHashProviderFactory;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.spi.Split;
import com.facebook.presto.split.CollocatedSplit;
import com.facebook.presto.split.DataStreamProvider;
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
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Provider;
import io.airlift.node.NodeInfo;
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
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Math.max;

public class SqlTaskExecution
        implements TaskExecution
{
    private final TaskId taskId;
    private final TaskOutput taskOutput;
    private final DataStreamProvider dataStreamProvider;
    private final Provider<ExchangeClient> exchangeClientProvider;
    private final ListeningExecutorService shardExecutor;
    private final PlanFragment fragment;
    private final Metadata metadata;
    private final LocalStorageManager storageManager;
    private final DataSize maxOperatorMemoryUsage;
    private final Session session;
    private final QueryMonitor queryMonitor;
    private final NodeInfo nodeInfo;

    private final AtomicInteger pendingWorkerCount = new AtomicInteger();

    @GuardedBy("this")
    private final SetMultimap<PlanNodeId, Split> unpartitionedSources = HashMultimap.create();
    @GuardedBy("this")
    private final List<WeakReference<SplitWorker>> splitWorkers = new ArrayList<>();
    @GuardedBy("this")
    private SourceHashProviderFactory sourceHashProviderFactory;
    @GuardedBy("this")
    private long maxAcknowledgedSplit = Long.MIN_VALUE;

    private final BlockingDeque<FutureTask<?>> unfinishedWorkerTasks = new LinkedBlockingDeque<>();

    public static SqlTaskExecution createSqlTaskExecution(Session session,
            NodeInfo nodeInfo,
            TaskId taskId,
            URI location,
            PlanFragment fragment,
            DataSize maxBufferSize,
            DataStreamProvider dataStreamProvider,
            Provider<ExchangeClient> exchangeClientProvider,
            Metadata metadata,
            LocalStorageManager storageManager,
            ExecutorService taskMasterExecutor,
            ListeningExecutorService shardExecutor,
            DataSize maxOperatorMemoryUsage,
            QueryMonitor queryMonitor,
            SqlTaskManagerStats globalStats)
    {
        SqlTaskExecution task = new SqlTaskExecution(session,
                nodeInfo,
                taskId,
                location,
                fragment,
                maxBufferSize,
                dataStreamProvider,
                exchangeClientProvider,
                metadata,
                storageManager,
                shardExecutor,
                maxOperatorMemoryUsage,
                queryMonitor,
                taskMasterExecutor,
                globalStats);

        task.start(taskMasterExecutor);

        return task;
    }

    private SqlTaskExecution(Session session,
            NodeInfo nodeInfo,
            TaskId taskId,
            URI location,
            PlanFragment fragment,
            DataSize maxBufferSize,
            DataStreamProvider dataStreamProvider,
            Provider<ExchangeClient> exchangeClientProvider,
            Metadata metadata,
            LocalStorageManager storageManager,
            ListeningExecutorService shardExecutor,
            DataSize maxOperatorMemoryUsage,
            QueryMonitor queryMonitor,
            Executor notificationExecutor,
            SqlTaskManagerStats globalStats)
    {
        Preconditions.checkNotNull(session, "session is null");
        Preconditions.checkNotNull(nodeInfo, "nodeInfo is null");
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(fragment, "fragment is null");
        Preconditions.checkArgument(maxBufferSize.toBytes() > 0, "maxBufferSize must be at least 1");
        Preconditions.checkNotNull(metadata, "metadata is null");
        Preconditions.checkNotNull(storageManager, "storageManager is null");
        Preconditions.checkNotNull(shardExecutor, "shardExecutor is null");
        Preconditions.checkNotNull(maxOperatorMemoryUsage, "maxOperatorMemoryUsage is null");
        Preconditions.checkNotNull(queryMonitor, "queryMonitor is null");
        Preconditions.checkNotNull(globalStats, "globalStats is null");

        this.session = session;
        this.nodeInfo = nodeInfo;
        this.taskId = taskId;
        this.fragment = fragment;
        this.dataStreamProvider = dataStreamProvider;
        this.exchangeClientProvider = exchangeClientProvider;
        this.shardExecutor = shardExecutor;
        this.metadata = metadata;
        this.storageManager = storageManager;
        this.maxOperatorMemoryUsage = maxOperatorMemoryUsage;
        this.queryMonitor = queryMonitor;

        // create output buffers
        this.taskOutput = new TaskOutput(taskId, location, maxBufferSize, notificationExecutor, globalStats);
    }

    //
    // This code starts threads so it can not be in the constructor
    // TODO: merge the partitioned and unparitioned paths somehow
    private void start(ExecutorService taskMasterExecutor)
    {
        // if plan is unpartitioned, add a worker
        if (!fragment.isPartitioned()) {
            scheduleSplitWorker(null, null);
        }

        // NOTE: this must be started after the unpartitioned task or the task can be ended early
        taskMasterExecutor.submit(new TaskWorker(taskOutput, unfinishedWorkerTasks));
    }

    @Override
    public TaskId getTaskId()
    {
        return taskId;
    }

    @Override
    public void waitForStateChange(TaskState currentState, Duration maxWait)
            throws InterruptedException
    {
        taskOutput.waitForStateChange(currentState, maxWait);
    }

    @Override
    public TaskInfo getTaskInfo(boolean full)
    {
        checkTaskCompletion();
        return taskOutput.getTaskInfo(full);
    }

    @Override
    public synchronized void addSources(List<TaskSource> sources)
    {
        Preconditions.checkNotNull(sources, "sources is null");

        long newMaxAcknowledgedSplit = maxAcknowledgedSplit;
        for (TaskSource source : sources) {
            PlanNodeId sourceId = source.getPlanNodeId();
            for (ScheduledSplit scheduledSplit : source.getSplits()) {
                // only add a split if we have not already scheduled it
                if (scheduledSplit.getSequenceId() > maxAcknowledgedSplit) {
                    addSplit(sourceId, scheduledSplit.getSplit());
                    newMaxAcknowledgedSplit = max(scheduledSplit.getSequenceId(), newMaxAcknowledgedSplit);
                }
            }
            if (source.isNoMoreSplits()) {
                noMoreSplits(sourceId);
            }
        }
        maxAcknowledgedSplit = newMaxAcknowledgedSplit;
    }

    @Override
    public synchronized void addResultQueue(OutputBuffers outputIds)
    {
        Preconditions.checkNotNull(outputIds, "outputIds is null");

        for (String bufferId : outputIds.getBufferIds()) {
            taskOutput.addResultQueue(bufferId);
        }
        if (outputIds.isNoMoreBufferIds()) {
            taskOutput.noMoreResultQueues();
        }
    }

    private synchronized void addSplit(PlanNodeId sourceId, Split split)
    {
        // is this a partitioned source
        if (fragment.isPartitioned() && fragment.getPartitionedSource().equals(sourceId)) {
            scheduleSplitWorker(sourceId, split);
        }
        else {
            // add this to all of the existing workers
            if (!unpartitionedSources.put(sourceId, split)) {
                return;
            }
            for (WeakReference<SplitWorker> workerReference : splitWorkers) {
                SplitWorker worker = workerReference.get();
                // the worker can be GCed due to a failure or a limit
                if (worker != null) {
                    worker.addSplit(sourceId, split);
                }
            }
        }
    }

    private synchronized void scheduleSplitWorker(@Nullable PlanNodeId partitionedSourceId, @Nullable Split partitionedSplit)
    {
        // create a new split worker
        SplitWorker worker = new SplitWorker(session,
                nodeInfo,
                taskOutput,
                fragment,
                getSourceHashProviderFactory(),
                metadata,
                maxOperatorMemoryUsage,
                storageManager,
                dataStreamProvider,
                exchangeClientProvider,
                queryMonitor);

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
        taskOutput.getStats().addSplits(1);

        // execute worker
        final ListenableFutureTask<?> workerFutureTask = ListenableFutureTask.create(worker);
        unfinishedWorkerTasks.addFirst(workerFutureTask);
        shardExecutor.submit(workerFutureTask);
        // The callback must be added to the workerFutureTask and NOT the future returned
        // by the submit.  This is because the future task catches the exception internally
        // to the executor only sees a successful return, and the errors will be ignored.
        Futures.addCallback(workerFutureTask, new FutureCallback<Object>()
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

    private synchronized void noMoreSplits(PlanNodeId sourceId)
    {
        // don't bother updating is this source has already been closed
        if (!taskOutput.noMoreSplits(sourceId)) {
            return;
        }

        if (sourceId.equals(fragment.getPartitionedSource())) {
            // all workers have been created
            // clear hash provider since it has a hard reference to every hash table
            sourceHashProviderFactory = null;
        }
        else {
            // add this to all of the existing workers
            for (WeakReference<SplitWorker> workerReference : splitWorkers) {
                SplitWorker worker = workerReference.get();
                // the worker can be GCed due to a failure or a limit
                if (worker != null) {
                    worker.noMoreSplits(sourceId);
                }
            }
        }
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
    public BufferResult getResults(String outputId, long startingSequenceId, DataSize maxSize, Duration maxWait)
            throws InterruptedException
    {
        Preconditions.checkNotNull(outputId, "outputId is null");
        Preconditions.checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");
        Preconditions.checkNotNull(maxWait, "maxWait is null");

        return taskOutput.getResults(outputId, startingSequenceId, maxSize, maxWait);
    }

    @Override
    public void abortResults(String outputId)
    {
        taskOutput.abortResults(outputId);
    }

    @Override
    public void recordHeartbeat()
    {
        taskOutput.getStats().recordHeartbeat();
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
                if (futureTask != null && !taskOutput.getState().isDone()) {
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
        private final PlanNodeId partitionedSource;
        private final Operator operator;
        private final OperatorStats operatorStats;
        private final QueryMonitor queryMonitor;
        private final Map<PlanNodeId, SourceOperator> sourceOperators;
        private final Map<PlanNodeId, OutputProducingOperator<?>> outputOperators;

        private SplitWorker(Session session,
                NodeInfo nodeInfo,
                TaskOutput taskOutput,
                PlanFragment fragment,
                SourceHashProviderFactory sourceHashProviderFactory,
                Metadata metadata,
                DataSize maxOperatorMemoryUsage,
                LocalStorageManager storageManager,
                DataStreamProvider dataStreamProvider,
                Provider<ExchangeClient> exchangeClientProvider,
                QueryMonitor queryMonitor)
        {
            this.taskOutput = taskOutput;
            partitionedSource = fragment.getPartitionedSource();
            operatorStats = new OperatorStats(taskOutput);
            this.queryMonitor = queryMonitor;

            LocalExecutionPlanner planner = new LocalExecutionPlanner(session,
                    nodeInfo,
                    metadata,
                    fragment.getSymbols(),
                    operatorStats,
                    sourceHashProviderFactory,
                    maxOperatorMemoryUsage,
                    dataStreamProvider,
                    storageManager,
                    exchangeClientProvider);

            LocalExecutionPlan localExecutionPlan = planner.plan(fragment.getRoot());
            operator = localExecutionPlan.getRootOperator();
            sourceOperators = localExecutionPlan.getSourceOperators();
            outputOperators = localExecutionPlan.getOutputOperators();
        }

        public void addSplit(PlanNodeId sourceId, Split split)
        {
            SourceOperator sourceOperator = sourceOperators.get(sourceId);
            Preconditions.checkArgument(sourceOperator != null, "Unknown plan source %s; known sources are %s", sourceId, sourceOperators.keySet());
            if (split instanceof CollocatedSplit) {
                CollocatedSplit collocatedSplit = (CollocatedSplit) split;
                // unwind collocated splits
                for (Entry<PlanNodeId, Split> entry : collocatedSplit.getSplits().entrySet()) {
                    addSplit(entry.getKey(), entry.getValue());
                }
            } else {
                sourceOperator.addSplit(split);
                if (sourceId.equals(partitionedSource)) {
                    operatorStats.addSplitInfo(split.getInfo());
                }
            }
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
                queryMonitor.splitCompletionEvent(taskOutput.getTaskInfo(false), operatorStats.snapshot());

                for (Map.Entry<PlanNodeId, OutputProducingOperator<?>> entry : outputOperators.entrySet()) {
                    taskOutput.addOutput(entry.getKey(), entry.getValue().getOutput());
                }
            }
            return null;
        }
    }
}
