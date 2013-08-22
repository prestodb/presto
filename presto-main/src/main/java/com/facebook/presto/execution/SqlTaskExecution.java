/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.ScheduledSplit;
import com.facebook.presto.TaskSource;
import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.OutputProducingOperator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.operator.SourceHashSupplierFactory;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.spi.Split;
import com.facebook.presto.split.CollocatedSplit;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.util.SetThreadName;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFutureTask;
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
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Math.max;

public class SqlTaskExecution
        implements TaskExecution
{
    private final TaskId taskId;
    private final TaskOutput taskOutput;
    private final ListeningExecutorService shardExecutor;
    private final PlanFragment fragment;
    private final LocalExecutionPlanner planner;
    private final TaskMemoryManager taskMemoryManager;
    private final Session session;
    private final QueryMonitor queryMonitor;

    private final AtomicInteger pendingWorkerCount = new AtomicInteger();

    @GuardedBy("this")
    private final SetMultimap<PlanNodeId, Split> unpartitionedSources = HashMultimap.create();
    @GuardedBy("this")
    private final List<WeakReference<SplitWorker>> splitWorkers = new ArrayList<>();
    @GuardedBy("this")
    private SourceHashSupplierFactory sourceHashSupplierFactory;
    @GuardedBy("this")
    private long maxAcknowledgedSplit = Long.MIN_VALUE;

    @GuardedBy("this")
    private long nextSplitWorkerId = 0;

    private final BlockingDeque<FutureTask<?>> unfinishedWorkerTasks = new LinkedBlockingDeque<>();

    public static SqlTaskExecution createSqlTaskExecution(Session session,
            TaskId taskId,
            URI location,
            PlanFragment fragment,
            LocalExecutionPlanner planner,
            DataSize maxBufferSize,
            ExecutorService taskMasterExecutor,
            ListeningExecutorService shardExecutor,
            DataSize maxTaskMemoryUsage,
            DataSize operatorPreAllocatedMemory,
            QueryMonitor queryMonitor,
            SqlTaskManagerStats globalStats)
    {
        SqlTaskExecution task = new SqlTaskExecution(session,
                taskId,
                location,
                fragment,
                planner,
                maxBufferSize,
                shardExecutor,
                maxTaskMemoryUsage,
                operatorPreAllocatedMemory,
                queryMonitor,
                taskMasterExecutor,
                globalStats);

        try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskId)){
            task.start(taskMasterExecutor);
            return task;
        }
    }

    private SqlTaskExecution(Session session,
            TaskId taskId,
            URI location,
            PlanFragment fragment,
            LocalExecutionPlanner planner,
            DataSize maxBufferSize,
            ListeningExecutorService shardExecutor,
            DataSize maxTaskMemoryUsage,
            DataSize operatorPreAllocatedMemory,
            QueryMonitor queryMonitor,
            Executor notificationExecutor,
            SqlTaskManagerStats globalStats)
    {
        try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskId)){
            this.session = checkNotNull(session, "session is null");
            this.taskId = checkNotNull(taskId, "taskId is null");
            this.fragment = checkNotNull(fragment, "fragment is null");
            this.planner = checkNotNull(planner, "planner is null");
            this.shardExecutor = checkNotNull(shardExecutor, "shardExecutor is null");
            this.taskMemoryManager = new TaskMemoryManager(
                    checkNotNull(maxTaskMemoryUsage, "maxTaskMemoryUsage is null"),
                    checkNotNull(operatorPreAllocatedMemory, "operatorPreAllocatedMemory is null"));
            this.queryMonitor = checkNotNull(queryMonitor, "queryMonitor is null");

            // create output buffers
            this.taskOutput = new TaskOutput(taskId,
                    checkNotNull(location, "location is null"),
                    checkNotNull(maxBufferSize, "maxBufferSize is null"),
                    checkNotNull(notificationExecutor, "notificationExecutor is null"),
                    checkNotNull(globalStats, "globalStats is null"));
        }
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
        try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskOutput.getTaskId())){
            taskOutput.waitForStateChange(currentState, maxWait);
        }
    }

    @Override
    public TaskInfo getTaskInfo(boolean full)
    {
        try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskOutput.getTaskId())){
            checkTaskCompletion();
            return taskOutput.getTaskInfo(full);
        }
    }

    @Override
    public synchronized void addSources(List<TaskSource> sources)
    {
        checkNotNull(sources, "sources is null");

        try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskOutput.getTaskId())){
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
    }

    @Override
    public synchronized void addResultQueue(OutputBuffers outputIds)
    {
        checkNotNull(outputIds, "outputIds is null");

        try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskOutput.getTaskId())){
            for (String bufferId : outputIds.getBufferIds()) {
                taskOutput.addResultQueue(bufferId);
            }
            if (outputIds.isNoMoreBufferIds()) {
                taskOutput.noMoreResultQueues();
            }
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
        SplitWorker worker = new SplitWorker(nextSplitWorkerId++,
                session,
                taskOutput,
                planner,
                fragment,
                getSourceHashSupplierFactory(),
                taskMemoryManager,
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
                try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskOutput.getTaskId())){
                    pendingWorkerCount.decrementAndGet();
                    checkTaskCompletion();
                    // free the reference to the this task in the scheduled workers, so the memory can be released
                    unfinishedWorkerTasks.removeFirstOccurrence(workerFutureTask);
                }
            }

            @Override
            public void onFailure(Throwable t)
            {
                try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskOutput.getTaskId())){
                    taskOutput.queryFailed(t);
                    pendingWorkerCount.decrementAndGet();
                    // free the reference to the this task in the scheduled workers, so the memory can be released
                    unfinishedWorkerTasks.removeFirstOccurrence(workerFutureTask);
                }
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
            sourceHashSupplierFactory = null;
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

    private synchronized SourceHashSupplierFactory getSourceHashSupplierFactory()
    {
        if (sourceHashSupplierFactory == null) {
            sourceHashSupplierFactory = new SourceHashSupplierFactory(taskMemoryManager);
        }
        return sourceHashSupplierFactory;
    }

    @Override
    public void cancel()
    {
        try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskOutput.getTaskId())){
            taskOutput.cancel();
        }
    }

    @Override
    public void fail(Throwable cause)
    {
        try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskOutput.getTaskId())){
            taskOutput.queryFailed(cause);
        }
    }

    @Override
    public BufferResult getResults(String outputId, long startingSequenceId, DataSize maxSize, Duration maxWait)
            throws InterruptedException
    {
        checkNotNull(outputId, "outputId is null");
        Preconditions.checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");
        checkNotNull(maxWait, "maxWait is null");

        try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskOutput.getTaskId())){
            return taskOutput.getResults(outputId, startingSequenceId, maxSize, maxWait);
        }
    }

    @Override
    public void abortResults(String outputId)
    {
        try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskOutput.getTaskId())){
            taskOutput.abortResults(outputId);
        }
    }

    @Override
    public void recordHeartbeat()
    {
        try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskOutput.getTaskId())){
            taskOutput.getStats().recordHeartbeat();
        }
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
            try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskOutput.getTaskId())){
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
    }

    private static class SplitWorker
            implements Callable<Void>
    {
        private final long workerId;
        private final AtomicBoolean started = new AtomicBoolean();
        private final TaskOutput taskOutput;
        private final PlanNodeId partitionedSource;
        private final AtomicReference<Operator> operator;
        private final OperatorStats operatorStats;
        private final QueryMonitor queryMonitor;
        private final Map<PlanNodeId, SourceOperator> sourceOperators;
        private final Map<PlanNodeId, OutputProducingOperator<?>> outputOperators;

        private SplitWorker(long workerId,
                Session session,
                TaskOutput taskOutput,
                LocalExecutionPlanner planner,
                PlanFragment fragment,
                SourceHashSupplierFactory sourceHashSupplierFactory,
                TaskMemoryManager taskMemoryManager,
                QueryMonitor queryMonitor)
        {
            this.workerId = workerId;
            this.taskOutput = taskOutput;
            partitionedSource = fragment.getPartitionedSource();
            operatorStats = new OperatorStats(taskOutput);
            this.queryMonitor = queryMonitor;

            LocalExecutionPlan localExecutionPlan = planner.plan(session, fragment.getRoot(), fragment.getSymbols(), sourceHashSupplierFactory, taskMemoryManager, operatorStats);
            operator = new AtomicReference<>(localExecutionPlan.getRootOperator());
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

            try (SetThreadName setThreadName = new SetThreadName("SplitWorker-Task-%s-%s", taskOutput.getTaskId(), workerId)){
                operatorStats.start();
                try (PageIterator pages = operator.get().iterator(operatorStats)) {
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

                    for (Entry<PlanNodeId, OutputProducingOperator<?>> entry : outputOperators.entrySet()) {
                        taskOutput.addOutput(entry.getKey(), entry.getValue().getOutput());
                    }
                }
                // remove this when ExpressionInterpreter changed to no hold onto input resolvers
                operator.set(null);
                return null;
            }
        }
    }
}
