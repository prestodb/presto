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
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragmentSource;
import com.facebook.presto.sql.planner.PlanFragmentSourceProvider;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class SqlTaskExecution
        implements TaskExecution
{
    private final String taskId;
    private final TaskOutput taskOutput;
    private final AtomicReference<List<PlanFragmentSource>> splits;
    private final Map<PlanNodeId, ExchangePlanFragmentSource> exchangeSources;
    private final PlanFragmentSourceProvider sourceProvider;
    private final FairBatchExecutor shardExecutor;
    private final PlanFragment fragment;
    private final Metadata metadata;
    private final DataSize maxOperatorMemoryUsage;
    private final Session session;

    public SqlTaskExecution(Session session,
            String queryId,
            String stageId,
            String taskId,
            URI location,
            PlanFragment fragment,
            List<PlanFragmentSource> splits,
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
        Preconditions.checkNotNull(splits, "splits is null");
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
        this.splits = new AtomicReference<List<PlanFragmentSource>>(ImmutableList.copyOf(splits));
        this.exchangeSources = exchangeSources;
        this.sourceProvider = sourceProvider;
        this.shardExecutor = shardExecutor;
        this.metadata = metadata;
        this.maxOperatorMemoryUsage = maxOperatorMemoryUsage;

        // create output buffers
        this.taskOutput = new TaskOutput(queryId, stageId, taskId, location, outputIds, pageBufferMax, splits.size());
    }

    @Override
    public String getTaskId()
    {
        return taskId;
    }

    @Override
    public TaskInfo getTaskInfo()
    {
        return taskOutput.getTaskInfo();
    }

    @Override
    public void run()
    {
        taskOutput.getStats().recordExecutionStart();
        try {
            // get the splits... if the task is canceled the splits will be null
            // set the splits to null to release the memory asap
            List<PlanFragmentSource> splits = this.splits.getAndSet(null);
            if (splits == null) {
                return;
            }

            final SourceHashProviderFactory sourceHashProviderFactory = new SourceHashProviderFactory(maxOperatorMemoryUsage);

            // if we have a single split, just execute in the current thread; otherwise use the thread pool
            if (splits.size() <= 1) {
                PlanFragmentSource split = splits.isEmpty() ? null : splits.get(0);
                SplitWorker worker = new SplitWorker(session,
                        taskOutput,
                        fragment,
                        split,
                        exchangeSources,
                        sourceHashProviderFactory,
                        sourceProvider,
                        metadata,
                        maxOperatorMemoryUsage);

                worker.call();
            }
            else {
                List<Callable<Void>> workers = ImmutableList.copyOf(Lists.transform(splits, new Function<PlanFragmentSource, Callable<Void>>()
                {
                    @Override
                    public Callable<Void> apply(PlanFragmentSource split)
                    {
                        return new SplitWorker(session,
                                taskOutput,
                                fragment,
                                split,
                                exchangeSources,
                                sourceHashProviderFactory,
                                sourceProvider,
                                metadata,
                                maxOperatorMemoryUsage);
                    }
                }));

                // Race the multithreaded executors in reverse order. This guarantees that at least
                // one thread is allocated to processing this task.
                // SplitWorkers are designed to be "once-only" callables and become no-ops once someone
                // invokes "call" on them. Therefore it is safe to invoke them here
                List<FutureTask<Void>> results = shardExecutor.processBatch(workers);
                for (FutureTask<Void> worker : Lists.reverse(results)) {
                    if (taskOutput.getState().isDone()) {
                        worker.cancel(true);
                    } else {
                        worker.run();
                    }
                }

                checkQueryResults(results);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            taskOutput.queryFailed(e);
            throw Throwables.propagate(e);
        }
        catch (Throwable e) {
            taskOutput.queryFailed(e);
            throw Throwables.propagate(e);
        }
        finally {
            taskOutput.finish();
        }
    }

    private static void checkQueryResults(Iterable<? extends Future<Void>> results)
            throws InterruptedException
    {
        RuntimeException queryFailedException = null;
        for (Future<Void> result : results) {
            try {
                result.get();
            }
            catch (CancellationException e) {
                // this is allowed
            }
            catch (ExecutionException e) {
                if (queryFailedException == null) {
                    queryFailedException = new RuntimeException("Query failed");
                }
                Throwable cause = e.getCause();
                queryFailedException.addSuppressed(cause);
            }
        }
        if (queryFailedException != null) {
            throw queryFailedException;
        }
    }

    @Override
    public void cancel()
    {
        splits.set(null);
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
        splits.set(null);
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
                @Nullable PlanFragmentSource split,
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
