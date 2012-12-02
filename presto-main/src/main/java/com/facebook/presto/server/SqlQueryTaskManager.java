/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.sql.compiler.SessionMetadata;
import com.facebook.presto.sql.planner.ExecutionPlanner;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
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
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.util.Threads.threadsNamed;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;

public class SqlQueryTaskManager
        implements QueryTaskManager
{
    private final int pageBufferMax;

    private final ExecutorService taskExecutor;
    private final ExecutorService shardExecutor;
    private final Metadata metadata;
    private final PlanFragmentSourceProvider sourceProvider;

    private final AtomicInteger nextTaskId = new AtomicInteger();
    private final ConcurrentMap<String, SqlQueryTask> queryTasks = new ConcurrentHashMap<>();

    @Inject
    public SqlQueryTaskManager(
            Metadata metadata,
            PlanFragmentSourceProvider sourceProvider)
    {
        this.pageBufferMax = 20;

        int processors = Runtime.getRuntime().availableProcessors();
        taskExecutor = new ThreadPoolExecutor(1000,
                1000,
                1, TimeUnit.MINUTES,
                new LinkedBlockingQueue<Runnable>(),
                threadsNamed("task-processor-%d"));
        shardExecutor = new ThreadPoolExecutor(processors,
                processors,
                1, TimeUnit.MINUTES,
                new SynchronousQueue<Runnable>(),
                threadsNamed("shard-processor-%d"),
                new ThreadPoolExecutor.CallerRunsPolicy());

        this.metadata = metadata;
        this.sourceProvider = sourceProvider;
    }

    @Override
    public List<QueryTaskInfo> getAllQueryTaskInfo()
    {
        return ImmutableList.copyOf(filter(transform(queryTasks.values(), new Function<SqlQueryTask, QueryTaskInfo>()
        {
            @Override
            public QueryTaskInfo apply(SqlQueryTask queryTask)
            {
                try {
                    return queryTask.getQueryTaskInfo();
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

        SqlQueryTask queryTask = queryTasks.get(taskId);
        if (queryTask == null) {
            throw new NoSuchElementException("Unknown query task " + taskId);
        }
        return queryTask.getQueryTaskInfo();
    }

    @Override
    public QueryTask createQueryTask(PlanFragment planFragment, List<String> outputIds, Map<String, List<PlanFragmentSource>> fragmentSources)
    {
        Preconditions.checkNotNull(planFragment, "planFragment is null");
        Preconditions.checkNotNull(fragmentSources, "fragmentSources is null");

        String taskId = String.valueOf(nextTaskId.getAndIncrement());
        SqlQueryTask queryTask = new SqlQueryTask(taskId, planFragment, outputIds, fragmentSources, sourceProvider, metadata, shardExecutor, pageBufferMax);
        queryTasks.put(taskId, queryTask);
        taskExecutor.submit(queryTask);

        return queryTask;
    }

    @Override
    public List<Page> getQueryTaskResults(String taskId, String outputName, int maxPageCount, Duration maxWaitTime)
            throws InterruptedException
    {
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(outputName, "outputName is null");

        SqlQueryTask queryTask = queryTasks.get(taskId);
        if (queryTask == null) {
            throw new NoSuchElementException("Unknown query task " + taskId);
        }
        return queryTask.getNextPages(outputName, maxPageCount, maxWaitTime);
    }

    @Override
    public void cancelQueryTask(String taskId)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");

        SqlQueryTask queryTask = queryTasks.remove(taskId);
        if (queryTask != null) {
            queryTask.cancel();
        }
    }

    private static class SqlQueryTask
            implements QueryTask, Runnable
    {
        private final String taskId;
        private final Map<String, QueryState> outputBuffers;
        private final Map<String, List<PlanFragmentSource>> fragmentSources;
        private final PlanFragmentSourceProvider sourceProvider;
        private final ExecutorService shardExecutor;
        private final PlanFragment fragment;
        private final List<TupleInfo> tupleInfos;
        private final Metadata metadata;

        private SqlQueryTask(String taskId,
                PlanFragment fragment,
                List<String> outputIds,
                Map<String, List<PlanFragmentSource>> fragmentSources,
                PlanFragmentSourceProvider sourceProvider,
                Metadata metadata,
                ExecutorService shardExecutor,
                int pageBufferMax)
        {
            this.taskId = taskId;
            this.fragment = fragment;
            this.fragmentSources = fragmentSources;
            this.sourceProvider = sourceProvider;
            this.shardExecutor = shardExecutor;
            this.metadata = metadata;

            this.tupleInfos = ImmutableList.copyOf(IterableTransformer.on(fragment.getRoot().getOutputSymbols())
                    .transform(Functions.forMap(fragment.getSymbols()))
                    .transform(com.facebook.presto.sql.compiler.Type.toRaw())
                    .transform(new Function<Type, TupleInfo>()
                    {
                        @Override
                        public TupleInfo apply(Type input)
                        {
                            return new TupleInfo(input);
                        }
                    })
                    .list());

            ImmutableMap.Builder<String, QueryState> builder = ImmutableMap.builder();
            for (String outputId : outputIds) {
                builder.put(outputId, new QueryState(tupleInfos, 1, pageBufferMax, this.fragmentSources.size()));
            }
            this.outputBuffers = builder.build();
        }

        @Override
        public String getTaskId()
        {
            return taskId;
        }

        public QueryTaskInfo getQueryTaskInfo()
        {
            QueryState outputBuffer = outputBuffers.values().iterator().next();
            return outputBuffer.toQueryTaskInfo(taskId);
        }

        public List<Page> getNextPages(String outputName, int maxPageCount, Duration maxWaitTime)
                throws InterruptedException
        {
            QueryState outputBuffer = outputBuffers.get(outputName);
            Preconditions.checkArgument(outputBuffer != null, "Unknown output %s: available outputs %s", outputName, outputBuffers.keySet());
            return outputBuffer.getNextPages(maxPageCount, maxWaitTime);
        }

        @Override
        public void cancel()
        {
            for (QueryState outputBuffer : outputBuffers.values()) {
                outputBuffer.cancel();
            }
        }

        @Override
        public void run()
        {
            // todo add support for multiple outputs
            final QueryState outputBuffer = outputBuffers.values().iterator().next();
            Preconditions.checkNotNull(outputBuffer, "outputBuffer is null");
            try {
                // todo move all of this planning into the planner
                Preconditions.checkState(fragmentSources.size() == 1, "Expected single source");
                final String sourceName = fragmentSources.keySet().iterator().next();
                List<PlanFragmentSource> sources = fragmentSources.values().iterator().next();

                // if we have a single source, just execute in the current thread; otherwise use the thread pool
                if (sources.size() == 1) {
                    new SplitWorker(outputBuffer, fragment, ImmutableMap.<String, PlanFragmentSource>of(sourceName, sources.get(0)), sourceProvider, metadata).call();
                }
                else {
                    List<Future<Void>> results = shardExecutor.invokeAll(Lists.transform(sources, new Function<PlanFragmentSource, Callable<Void>>()
                    {
                        @Override
                        public Callable<Void> apply(PlanFragmentSource fragmentSource)
                        {
                            return new SplitWorker(outputBuffer, fragment, ImmutableMap.of(sourceName, fragmentSource), sourceProvider, metadata);
                        }
                    }));

                    checkQueryResults(results);
                }

                outputBuffer.sourceFinished();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                outputBuffer.queryFailed(e);
                throw Throwables.propagate(e);
            }
            catch (Exception e) {
                e.printStackTrace();
                outputBuffer.queryFailed(e);
                throw Throwables.propagate(e);
            }
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                    .add("taskId", taskId)
                    .add("splits", fragmentSources)
                    .add("tupleInfos", tupleInfos)
                    .toString();
        }
    }

    private static class SplitWorker
            implements Callable<Void>
    {
        private final QueryState queryState;
        private final Operator operator;
        private final Optional<DataSize> inputDataSize;
        private final Optional<Integer> inputPositionCount;

        private SplitWorker(QueryState queryState,
                PlanFragment fragment,
                Map<String, PlanFragmentSource> fragmentSources,
                PlanFragmentSourceProvider sourceProvider,
                Metadata metadata)
        {
            this.queryState = queryState;

            ExecutionPlanner planner = new ExecutionPlanner(new SessionMetadata(metadata),
                    sourceProvider,
                    fragment.getSymbols(),
                    fragmentSources);

            operator = planner.plan(fragment.getRoot());

            inputDataSize = planner.getInputDataSize();
            if (inputDataSize.isPresent()) {
                queryState.addInputDataSize(inputDataSize.get());
            }
            inputPositionCount = planner.getInputPositionCount();
            if (inputPositionCount.isPresent()) {
                queryState.addInputPositions(inputPositionCount.get());
            }
        }

        @Override
        public Void call()
                throws Exception
        {
            queryState.splitStarted();
            long startTime = System.nanoTime();
            try {
                for (Page page : operator) {
                    queryState.addPage(page);
                }
                return null;
            }
            catch (Exception e) {
                queryState.queryFailed(e);
                throw Throwables.propagate(e);
            }
            finally {
                queryState.addSplitCpuTime(Duration.nanosSince(startTime));
                queryState.splitCompleted();
                if (inputDataSize.isPresent()) {
                    queryState.addCompletedDataSize(inputDataSize.get());
                }
                if (inputPositionCount.isPresent()) {
                    queryState.addCompletedPositions(inputPositionCount.get());
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
