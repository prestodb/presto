/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.ScheduledSplit;
import com.facebook.presto.TaskSource;
import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.OutputProducingOperator;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ListeningExecutorService;
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
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.max;

public class SqlTaskExecution
        implements TaskExecution
{
    private final TaskId taskId;
    private final TaskOutput taskOutput;
    private final DataStreamProvider dataStreamProvider;
    private final ExchangeOperatorFactory exchangeOperatorFactory;
    private final ListeningExecutorService shardExecutor;
    private final PlanFragment fragment;
    private final Metadata metadata;
    private final DataSize maxOperatorMemoryUsage;
    private final Session session;
    private final QueryMonitor queryMonitor;

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
            TaskId taskId,
            URI location,
            PlanFragment fragment,
            int pageBufferMax,
            DataStreamProvider dataStreamProvider,
            ExchangeOperatorFactory exchangeOperatorFactory,
            Metadata metadata,
            ExecutorService taskMasterExecutor,
            ListeningExecutorService shardExecutor,
            DataSize maxOperatorMemoryUsage,
            QueryMonitor queryMonitor)
    {
        SqlTaskExecution task = new SqlTaskExecution(session,
                taskId,
                location,
                fragment,
                pageBufferMax,
                dataStreamProvider,
                exchangeOperatorFactory,
                metadata,
                shardExecutor,
                maxOperatorMemoryUsage,
                queryMonitor);

        task.start(taskMasterExecutor);

        return task;
    }

    private SqlTaskExecution(Session session,
            TaskId taskId,
            URI location,
            PlanFragment fragment,
            int pageBufferMax,
            DataStreamProvider dataStreamProvider,
            ExchangeOperatorFactory exchangeOperatorFactory,
            Metadata metadata,
            ListeningExecutorService shardExecutor,
            DataSize maxOperatorMemoryUsage,
            QueryMonitor queryMonitor)
    {
        Preconditions.checkNotNull(session, "session is null");
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(fragment, "fragment is null");
        Preconditions.checkArgument(pageBufferMax > 0, "pageBufferMax must be at least 1");
        Preconditions.checkNotNull(metadata, "metadata is null");
        Preconditions.checkNotNull(shardExecutor, "shardExecutor is null");
        Preconditions.checkNotNull(maxOperatorMemoryUsage, "maxOperatorMemoryUsage is null");
        Preconditions.checkNotNull(queryMonitor, "queryMonitor is null");

        this.session = session;
        this.taskId = taskId;
        this.fragment = fragment;
        this.dataStreamProvider = dataStreamProvider;
        this.exchangeOperatorFactory = exchangeOperatorFactory;
        this.shardExecutor = shardExecutor;
        this.metadata = metadata;
        this.maxOperatorMemoryUsage = maxOperatorMemoryUsage;
        this.queryMonitor = queryMonitor;

        // create output buffers
        this.taskOutput = new TaskOutput(taskId, location, pageBufferMax);
    }

    //
    // This code starts threads so it can not be in the constructor
    // TODO: merge the partitioned and unparitioned paths somehow
    private void start(ExecutorService taskMasterExecutor)
    {
        // if plan is unpartitioned, add a worker
        if (!fragment.isPartitioned()) {
            scheduleSplitWorker(null);
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
            for (ScheduledSplit scheduledSplit : source.getSplits()) {
                // only add a split if we have not already scheduled it
                if (scheduledSplit.getSequenceId() > maxAcknowledgedSplit) {
                    addSplits(scheduledSplit.getSplits());
                    newMaxAcknowledgedSplit = max(scheduledSplit.getSequenceId(), newMaxAcknowledgedSplit);
                }
            }

            // All nodes that were visible in that split are now done.
            if (source.isNoMoreSplits()) {
                noMoreSplits(source.getPlanNodeIds());
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

    private synchronized void addSplits(Map<PlanNodeId, ? extends Split> splits)
    {
        // is this a partitioned source
        if (fragment.isPartitioned() && fragment.getPartitionedSources().equals(splits.keySet())) {
            scheduleSplitWorker(splits);
        }
        else {
            checkState(splits.size() == 1, "Unpartitioned splits can have only a single source");
            Map.Entry<PlanNodeId, ? extends Split> entry = Iterables.getOnlyElement(splits.entrySet());
            if (!unpartitionedSources.put(entry.getKey(), entry.getValue())) {
                return;
            }
            for (WeakReference<SplitWorker> workerReference : splitWorkers) {
                SplitWorker worker = workerReference.get();
                // this should not happen until the all sources have been closed
                Preconditions.checkState(worker != null, "SplitWorker has been GCed");
                worker.addSplits(splits);
            }
        }
    }

    private synchronized void scheduleSplitWorker(@Nullable Map<PlanNodeId, ? extends Split> splits)
    {
        // create a new split worker
        SplitWorker worker = new SplitWorker(session,
                taskOutput,
                fragment,
                getSourceHashProviderFactory(),
                metadata,
                maxOperatorMemoryUsage,
                dataStreamProvider,
                exchangeOperatorFactory,
                queryMonitor);

        // TableScanOperator requires partitioned split to be added before task is started
        if (splits != null) {
            worker.addSplits(splits);
        }

        // add unpartitioned sources
        for (Map.Entry<PlanNodeId, ? extends Split> entry : unpartitionedSources.entries()) {
            Map<PlanNodeId, ? extends Split> unpartitionedSplit = ImmutableMap.of(entry.getKey(), entry.getValue());
            worker.addSplits(unpartitionedSplit);
        }

        // record new worker
        splitWorkers.add(new WeakReference<>(worker));
        pendingWorkerCount.incrementAndGet();
        taskOutput.getStats().addSplits(1);

        // execute worker
        final ListenableFutureTask<?> workerFutureTask = ListenableFutureTask.create(worker);
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

        unfinishedWorkerTasks.addFirst(workerFutureTask);
        shardExecutor.submit(workerFutureTask);
    }

    private synchronized void noMoreSplits(Set<PlanNodeId> sourceIds)
    {
        sourceIds = ImmutableSet.copyOf(sourceIds);

        // don't bother updating is this source has already been closed
        if (!taskOutput.noMoreSplits(sourceIds)) {
            return;
        }

        if (fragment.getPartitionedSources().equals(sourceIds)) {
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
                worker.noMoreSplits(sourceIds);
            }
        }
    }

    private synchronized void checkTaskCompletion()
    {
        // are there more partition splits expected?
        if (fragment.isPartitioned() && !taskOutput.getNoMoreSplits().containsAll(fragment.getPartitionedSources())) {
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
    public BufferResult<Page> getResults(String outputId, int maxPageCount, Duration maxWait)
            throws InterruptedException
    {
        return taskOutput.getResults(outputId, maxPageCount, maxWait);
    }

    @Override
    public void abortResults(String outputId)
    {
        taskOutput.abortResults(outputId);
    }

    @Override
    public void recordHeartBeat()
    {
        taskOutput.getStats().recordHeartBeat();
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
        private final Set<PlanNodeId> partitionedSources;
        private final Operator operator;
        private final OperatorStats operatorStats;
        private final QueryMonitor queryMonitor;
        private final Map<PlanNodeId, SourceOperator> sourceOperators;
        private final Map<PlanNodeId, OutputProducingOperator<?>> outputOperators;

        private SplitWorker(Session session,
                TaskOutput taskOutput,
                PlanFragment fragment,
                SourceHashProviderFactory sourceHashProviderFactory,
                Metadata metadata,
                DataSize maxOperatorMemoryUsage,
                DataStreamProvider dataStreamProvider,
                ExchangeOperatorFactory exchangeOperatorFactory,
                QueryMonitor queryMonitor)
        {
            this.taskOutput = taskOutput;
            this.partitionedSources = fragment.getPartitionedSources();
            operatorStats = new OperatorStats(taskOutput);
            this.queryMonitor = queryMonitor;

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
            outputOperators = localExecutionPlan.getOutputOperators();
        }

        public void addSplits(Map<PlanNodeId, ? extends Split> splits)
        {
            checkNotNull(splits, "splits is null");
            checkState(splits.size() > 0, "no splits given");

            for (Map.Entry<PlanNodeId, ? extends Split> entry : splits.entrySet()) {
                SourceOperator sourceOperator = sourceOperators.get(entry.getKey());
                checkArgument(sourceOperator != null, "Unknown plan source %s; known sources are %s", entry.getKey(), sourceOperators.keySet());
                Split operatorSplit = entry.getValue();
                sourceOperator.addSplit(operatorSplit);
                if (partitionedSources.contains(entry.getKey())) {
                    operatorStats.addSplitInfo(operatorSplit.getInfo());
                }
            }
        }

        public void noMoreSplits(Set<PlanNodeId> planNodeIds)
        {
            checkNotNull(planNodeIds, "planNodeIds is null");

            for (PlanNodeId planNodeId : planNodeIds) {
                SourceOperator sourceOperator = sourceOperators.get(planNodeId);
                Preconditions.checkArgument(sourceOperator != null, "Unknown plan source %s; known sources are %s", planNodeId, sourceOperators.keySet());
                sourceOperator.noMoreSplits();
            }
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
