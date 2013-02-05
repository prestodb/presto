/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.concurrent.FairBatchExecutor;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.operator.SourceHashProviderFactory;
import com.facebook.presto.split.Split;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragmentSourceProvider;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class SqlTaskExecution
        implements TaskExecution
{
    private final String taskId;
    private final TaskOutput taskOutput;
    private final Map<PlanNodeId, ExchangePlanFragmentSource> exchangeSources;
    private final PlanFragmentSourceProvider sourceProvider;
    private final FairBatchExecutor shardExecutor;
    private final PlanFragment fragment;
    private final Metadata metadata;
    private final DataSize maxOperatorMemoryUsage;
    private final Session session;

    private final LinkedBlockingQueue<FutureTask<Void>> splitWorkers = new LinkedBlockingQueue<>();
    private final AtomicBoolean noMoreSources = new AtomicBoolean();
    private SourceHashProviderFactory sourceHashProviderFactory;

    public SqlTaskExecution(Session session,
            String queryId,
            String stageId,
            String taskId,
            URI location,
            PlanFragment fragment,
            Map<PlanNodeId, ExchangePlanFragmentSource> exchangeSources,
            List<String> outputIds,
            int pageBufferMax,
            PlanFragmentSourceProvider sourceProvider,
            Metadata metadata,
            FairBatchExecutor shardExecutor,
            DataSize maxOperatorMemoryUsage)
    {
        Preconditions.checkNotNull(session, "session is null");
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(stageId, "stageId is null");
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(fragment, "fragment is null");
        Preconditions.checkNotNull(exchangeSources, "exchangeSources is null");
        Preconditions.checkNotNull(outputIds, "outputIds is null");
        Preconditions.checkArgument(!outputIds.isEmpty(), "outputIds is empty");
        Preconditions.checkArgument(pageBufferMax > 0, "pageBufferMax must be at least 1");
        Preconditions.checkNotNull(sourceProvider, "sourceProvider is null");
        Preconditions.checkNotNull(metadata, "metadata is null");
        Preconditions.checkNotNull(shardExecutor, "shardExecutor is null");
        Preconditions.checkNotNull(maxOperatorMemoryUsage, "maxOperatorMemoryUsage is null");

        this.session = session;
        this.taskId = taskId;
        this.fragment = fragment;
        this.exchangeSources = exchangeSources;
        this.sourceProvider = sourceProvider;
        this.shardExecutor = shardExecutor;
        this.metadata = metadata;
        this.maxOperatorMemoryUsage = maxOperatorMemoryUsage;

        // create output buffers
        this.taskOutput = new TaskOutput(queryId, stageId, taskId, location, outputIds, pageBufferMax, 0);

        // todo is this correct?
        taskOutput.getStats().recordExecutionStart();

        // plans without a partition are immediately executed
        if (!fragment.isPartitioned()) {
            addSource(null, null);
        }
    }

    @Override
    public String getTaskId()
    {
        return taskId;
    }

    @Override
    public TaskInfo getTaskInfo()
    {
        if (noMoreSources.get() && !taskOutput.getState().isDone()) {
            boolean allDone = true;
            RuntimeException queryFailedException = null;
            for (Future<Void> result : splitWorkers) {
                try {
                    if (result.isDone()) {
                        // since the result is "done" this should not interrupt
                        Uninterruptibles.getUninterruptibly(result);
                    }
                    else {
                        allDone = false;
                    }
                }
                catch (Throwable e) {
                    if (queryFailedException == null) {
                        queryFailedException = new RuntimeException("Query failed");
                    }
                    Throwable cause = e.getCause();
                    queryFailedException.addSuppressed(cause);
                }
            }
            if (queryFailedException != null) {
                taskOutput.queryFailed(queryFailedException);
            }
            else if (allDone) {
                taskOutput.finish();
            }
        }
        return taskOutput.getTaskInfo();
    }

    @Override
    public void addSource(PlanNodeId sourceId, Split split)
    {
        if (sourceId != null) {
            Preconditions.checkNotNull(split, "split is null");
            Preconditions.checkArgument(sourceId.equals(fragment.getPartitionedSource()), "Expected sourceId to be %s but was %s", fragment.getPartitionedSource(), sourceId);
        }

        SplitWorker worker = new SplitWorker(session,
                taskOutput,
                fragment,
                split,
                exchangeSources,
                getSourceHashProviderFactory(),
                sourceProvider,
                metadata,
                maxOperatorMemoryUsage);

        List<FutureTask<Void>> results = shardExecutor.processBatch(ImmutableList.of(worker));
        splitWorkers.addAll(results);
        getTaskInfo().getStats().addSplits(1);
    }

    @Override
    public void noMoreSources(String sourceId)
    {
        this.noMoreSources.set(true);
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
        splits.set(null);
        taskOutput.queryFailed(cause);
    }

    @Override
    public List<Page> getResults(String outputId, int maxPageCount, Duration maxWait)
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
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("taskId", taskId)
                .add("splits", exchangeSources)
                .add("taskOutput", taskOutput)
                .toString();
    }

    private static class SplitWorker
            implements Callable<Void>
    {
        private final AtomicBoolean started = new AtomicBoolean();
        private final TaskOutput taskOutput;
        private final Operator operator;
        private final OperatorStats operatorStats;

        private SplitWorker(Session session,
                TaskOutput taskOutput,
                PlanFragment fragment,
                @Nullable Split split,
                Map<PlanNodeId, ExchangePlanFragmentSource> exchangeSources,
                SourceHashProviderFactory sourceHashProviderFactory,
                PlanFragmentSourceProvider sourceProvider,
                Metadata metadata,
                DataSize maxOperatorMemoryUsage)
        {
            this.taskOutput = taskOutput;

            operatorStats = new OperatorStats(taskOutput);

            LocalExecutionPlanner planner = new LocalExecutionPlanner(session,
                    metadata,
                    sourceProvider,
                    fragment.getSymbols(),
                    split,
                    null,
                    exchangeSources,
                    operatorStats,
                    sourceHashProviderFactory,
                    maxOperatorMemoryUsage
            );

            operator = planner.plan(fragment.getRoot());
        }

        @Override
        public Void call()
                throws Exception
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
                return null;
            }
            catch (Throwable e) {
                taskOutput.queryFailed(e);
                throw e;
            }
            finally {
                operatorStats.finish();
            }
        }
    }
}
