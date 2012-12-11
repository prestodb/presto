/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.operator.SourceHashProviderFactory;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragmentSource;
import com.facebook.presto.sql.planner.PlanFragmentSourceProvider;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class SqlTaskExecution
        implements TaskExecution
{
    private final String taskId;
    private final TaskOutput taskOutput;
    private final List<PlanFragmentSource> splits;
    private final Map<String, ExchangePlanFragmentSource> exchangeSources;
    private final PlanFragmentSourceProvider sourceProvider;
    private final ExecutorService shardExecutor;
    private final PlanFragment fragment;
    private final Metadata metadata;

    public SqlTaskExecution(String queryId,
            String stageId,
            String taskId,
            URI location,
            PlanFragment fragment,
            List<PlanFragmentSource> splits,
            Map<String, ExchangePlanFragmentSource> exchangeSources,
            List<String> outputIds,
            int pageBufferMax,
            PlanFragmentSourceProvider sourceProvider,
            Metadata metadata,
            ExecutorService shardExecutor)
    {
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

        this.taskId = taskId;
        this.fragment = fragment;
        this.splits = splits;
        this.exchangeSources = exchangeSources;
        this.sourceProvider = sourceProvider;
        this.shardExecutor = shardExecutor;
        this.metadata = metadata;

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
    public void start()
    {
        try {
            // if we have a single split, just execute in the current thread; otherwise use the thread pool
            final SourceHashProviderFactory sourceHashProviderFactory = new SourceHashProviderFactory();
            if (splits.size() <= 1) {
                PlanFragmentSource split = splits.isEmpty() ? null : splits.get(0);
                SplitWorker worker = new SplitWorker(taskOutput, fragment, split, exchangeSources, sourceHashProviderFactory, sourceProvider, metadata);
                worker.call();
            }
            else {
                List<Future<Void>> results = shardExecutor.invokeAll(Lists.transform(splits, new Function<PlanFragmentSource, Callable<Void>>()
                {
                    @Override
                    public Callable<Void> apply(PlanFragmentSource split)
                    {
                        return new SplitWorker(taskOutput, fragment, split, exchangeSources, sourceHashProviderFactory, sourceProvider, metadata);
                    }
                }));

                checkQueryResults(results);
            }

            taskOutput.finish();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            taskOutput.queryFailed(e);
            throw Throwables.propagate(e);
        }
        catch (Exception e) {
            taskOutput.queryFailed(e);
            throw Throwables.propagate(e);
        }
    }

    private static void checkQueryResults(Iterable<Future<Void>> results)
            throws InterruptedException
    {
        RuntimeException queryFailedException = null;
        for (Future<Void> result : results) {
            try {
                result.get();
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
        taskOutput.cancel();
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
        private final TaskOutput taskOutput;
        private final Operator operator;
        private final Optional<DataSize> inputDataSize;
        private final Optional<Integer> inputPositionCount;

        private SplitWorker(TaskOutput taskOutput,
                PlanFragment fragment,
                @Nullable PlanFragmentSource split,
                Map<String, ExchangePlanFragmentSource> exchangeSources,
                SourceHashProviderFactory sourceHashProviderFactory,
                PlanFragmentSourceProvider sourceProvider,
                Metadata metadata)
        {
            this.taskOutput = taskOutput;

            LocalExecutionPlanner planner = new LocalExecutionPlanner(metadata,
                    sourceProvider,
                    fragment.getSymbols(),
                    split,
                    null,
                    exchangeSources,
                    sourceHashProviderFactory);

            operator = planner.plan(fragment.getRoot());

            inputDataSize = planner.getInputDataSize();
            if (inputDataSize.isPresent()) {
                taskOutput.getStats().addInputDataSize(inputDataSize.get());
            }
            inputPositionCount = planner.getInputPositionCount();
            if (inputPositionCount.isPresent()) {
                taskOutput.getStats().addInputPositions(inputPositionCount.get());
            }
        }

        @Override
        public Void call()
                throws Exception
        {
            taskOutput.getStats().splitStarted();
            long startTime = System.nanoTime();
            try (PageIterator pages = operator.iterator()) {
                while (pages.hasNext()) {
                    Page page = pages.next();
                    if (!taskOutput.addPage(page)) {
                        pages.close();
                    }
                }
                return null;
            }
            catch (Exception e) {
                taskOutput.queryFailed(e);
                throw Throwables.propagate(e);
            }
            finally {
                taskOutput.getStats().addSplitCpuTime(Duration.nanosSince(startTime));
                taskOutput.getStats().splitCompleted();
                if (inputDataSize.isPresent()) {
                    taskOutput.getStats().addCompletedDataSize(inputDataSize.get());
                }
                if (inputPositionCount.isPresent()) {
                    taskOutput.getStats().addCompletedPositions(inputPositionCount.get());
                }

            }
        }
    }
}
