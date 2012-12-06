/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.operator.SourceHashProviderFactory;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragmentSource;
import com.facebook.presto.sql.planner.PlanFragmentSourceProvider;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.util.Threads.threadsNamed;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;

public class SqlQueryTaskManager
        implements QueryTaskManager
{
    private final int pageBufferMax;

    private final ExecutorService taskExecutor;
    private final ExecutorService shardExecutor;
    private final Metadata metadata;
    private final PlanFragmentSourceProvider sourceProvider;
    private final HttpServerInfo httpServerInfo;

    private final ConcurrentMap<String, TaskOutput> tasks = new ConcurrentHashMap<>();

    @Inject
    public SqlQueryTaskManager(
            Metadata metadata,
            PlanFragmentSourceProvider sourceProvider,
            HttpServerInfo httpServerInfo)
    {
        Preconditions.checkNotNull(metadata, "metadata is null");
        Preconditions.checkNotNull(sourceProvider, "sourceProvider is null");
        Preconditions.checkNotNull(httpServerInfo, "httpServerInfo is null");

        this.metadata = metadata;
        this.sourceProvider = sourceProvider;
        this.httpServerInfo = httpServerInfo;
        this.pageBufferMax = 20;

        int processors = Runtime.getRuntime().availableProcessors();
        taskExecutor = new ThreadPoolExecutor(1000,
                1000,
                1, TimeUnit.MINUTES,
                new LinkedBlockingQueue<Runnable>(),
                threadsNamed("task-processor-%d"));
        shardExecutor = new ThreadPoolExecutor(8 * processors,
                8 * processors,
                1, TimeUnit.MINUTES,
                new SynchronousQueue<Runnable>(),
                threadsNamed("shard-processor-%d"),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Override
    public List<QueryTaskInfo> getAllQueryTaskInfo()
    {
        return ImmutableList.copyOf(filter(transform(tasks.values(), new Function<TaskOutput, QueryTaskInfo>()
        {
            @Override
            public QueryTaskInfo apply(TaskOutput taskOutput)
            {
                try {
                    return taskOutput.getQueryTaskInfo();
                }
                catch (Exception ignored) {
                    return null;
                }
            }
        }), Predicates.notNull()));
    }

    @Override
    public QueryTaskInfo getQueryTaskInfo(String taskId)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");

        TaskOutput taskOutput = tasks.get(taskId);
        if (taskOutput == null) {
            throw new NoSuchElementException("Unknown query task " + taskId);
        }
        return taskOutput.getQueryTaskInfo();
    }

    @Override
    public QueryTaskInfo createQueryTask(PlanFragment fragment,
            List<PlanFragmentSource> splits,
            Map<String, ExchangePlanFragmentSource> exchangeSources,
            List<String> outputIds)
    {
        Preconditions.checkNotNull(fragment, "fragment is null");
        Preconditions.checkNotNull(outputIds, "outputIds is null");
        Preconditions.checkNotNull(splits, "splits is null");
        Preconditions.checkNotNull(exchangeSources, "exchangeSources is null");

        String taskId = UUID.randomUUID().toString();
        URI location = uriBuilderFrom(httpServerInfo.getHttpUri()).appendPath("v1/presto/task").appendPath(taskId).build();

        // create output buffers
        List<TupleInfo> tupleInfos = ImmutableList.copyOf(IterableTransformer.on(fragment.getRoot().getOutputSymbols())
                .transform(Functions.forMap(fragment.getSymbols()))
                .transform(com.facebook.presto.sql.analyzer.Type.toRaw())
                .transform(new Function<Type, TupleInfo>()
                {
                    @Override
                    public TupleInfo apply(Type input)
                    {
                        return new TupleInfo(input);
                    }
                })
                .list());

        TaskOutput taskOutput = new TaskOutput(taskId, location, outputIds, tupleInfos, pageBufferMax, splits.size());

        SqlQueryTask queryTask = new SqlQueryTask(taskId, fragment, taskOutput, splits, exchangeSources, sourceProvider, metadata, shardExecutor);
        taskExecutor.submit(queryTask);

        tasks.put(taskId, taskOutput);
        return taskOutput.getQueryTaskInfo();
    }

    @Override
    public List<Page> getQueryTaskResults(String taskId, String outputName, int maxPageCount, Duration maxWaitTime)
            throws InterruptedException
    {
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(outputName, "outputName is null");

        TaskOutput taskOutput = tasks.get(taskId);
        if (taskOutput == null) {
            throw new NoSuchElementException("Unknown query task " + taskId);
        }
        return taskOutput.getNextPages(outputName, maxPageCount, maxWaitTime);
    }

    @Override
    public void abortQueryTaskResults(String taskId, String outputId)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(outputId, "outputId is null");

        TaskOutput taskOutput = tasks.get(taskId);
        if (taskOutput == null) {
            throw new NoSuchElementException();
        }
        taskOutput.abortResults(outputId);
    }

    @Override
    public void cancelQueryTask(String taskId)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");

        TaskOutput taskOutput = tasks.remove(taskId);
        if (taskOutput != null) {
            taskOutput.cancel();
        }
    }

    private static class SqlQueryTask
            implements Runnable
    {
        private final String taskId;
        private final TaskOutput taskOutput;
        private final List<PlanFragmentSource> splits;
        private final Map<String, ExchangePlanFragmentSource> exchangeSources;
        private final PlanFragmentSourceProvider sourceProvider;
        private final ExecutorService shardExecutor;
        private final PlanFragment fragment;
        private final Metadata metadata;

        private SqlQueryTask(String taskId,
                PlanFragment fragment,
                TaskOutput taskOutput,
                List<PlanFragmentSource> splits,
                Map<String, ExchangePlanFragmentSource> exchangeSources,
                PlanFragmentSourceProvider sourceProvider,
                Metadata metadata,
                ExecutorService shardExecutor)
        {
            this.taskId = taskId;
            this.fragment = fragment;
            this.taskOutput = taskOutput;
            this.splits = splits;
            this.exchangeSources = exchangeSources;
            this.sourceProvider = sourceProvider;
            this.shardExecutor = shardExecutor;
            this.metadata = metadata;
        }

        public String getTaskId()
        {
            return taskId;
        }

        @Override
        public void run()
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

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                    .add("taskId", taskId)
                    .add("splits", exchangeSources)
                    .add("taskOutput", taskOutput)
                    .toString();
        }
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
                taskOutput.addInputDataSize(inputDataSize.get());
            }
            inputPositionCount = planner.getInputPositionCount();
            if (inputPositionCount.isPresent()) {
                taskOutput.addInputPositions(inputPositionCount.get());
            }
        }

        @Override
        public Void call()
                throws Exception
        {
            taskOutput.splitStarted();
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
                taskOutput.addSplitCpuTime(Duration.nanosSince(startTime));
                taskOutput.splitCompleted();
                if (inputDataSize.isPresent()) {
                    taskOutput.addCompletedDataSize(inputDataSize.get());
                }
                if (inputPositionCount.isPresent()) {
                    taskOutput.addCompletedPositions(inputPositionCount.get());
                }

            }
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
}
