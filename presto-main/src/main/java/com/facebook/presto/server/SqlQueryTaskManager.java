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
    private final ConcurrentMap<String, TaskOutput> tasks = new ConcurrentHashMap<>();

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
    public QueryTaskInfo createQueryTask(PlanFragment fragment, List<String> outputIds, Map<String, List<PlanFragmentSource>> fragmentSources)
    {
        Preconditions.checkNotNull(fragment, "fragment is null");
        Preconditions.checkNotNull(fragmentSources, "fragmentSources is null");

        String taskId = String.valueOf(nextTaskId.getAndIncrement());

        // create output buffers
        List<TupleInfo> tupleInfos = ImmutableList.copyOf(IterableTransformer.on(fragment.getRoot().getOutputSymbols())
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
        TaskOutput taskOutput = new TaskOutput(taskId, outputIds, tupleInfos, pageBufferMax, fragmentSources.size());

        SqlQueryTask queryTask = new SqlQueryTask(taskId, fragment, taskOutput, fragmentSources, sourceProvider, metadata, shardExecutor);
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
        private final Map<String, List<PlanFragmentSource>> fragmentSources;
        private final PlanFragmentSourceProvider sourceProvider;
        private final ExecutorService shardExecutor;
        private final PlanFragment fragment;
        private final Metadata metadata;

        private SqlQueryTask(String taskId,
                PlanFragment fragment,
                TaskOutput taskOutput,
                Map<String, List<PlanFragmentSource>> fragmentSources,
                PlanFragmentSourceProvider sourceProvider,
                Metadata metadata,
                ExecutorService shardExecutor)
        {
            this.taskId = taskId;
            this.fragment = fragment;
            this.taskOutput = taskOutput;
            this.fragmentSources = fragmentSources;
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
                // todo move all of this planning into the planner
                Preconditions.checkState(fragmentSources.size() == 1, "Expected single source");
                final String sourceName = fragmentSources.keySet().iterator().next();
                List<PlanFragmentSource> sources = fragmentSources.values().iterator().next();

                // if we have a single source, just execute in the current thread; otherwise use the thread pool
                if (sources.size() == 1) {
                    new SplitWorker(taskOutput, fragment, ImmutableMap.<String, PlanFragmentSource>of(sourceName, sources.get(0)), sourceProvider, metadata).call();
                }
                else {
                    List<Future<Void>> results = shardExecutor.invokeAll(Lists.transform(sources, new Function<PlanFragmentSource, Callable<Void>>()
                    {
                        @Override
                        public Callable<Void> apply(PlanFragmentSource fragmentSource)
                        {
                            return new SplitWorker(taskOutput, fragment, ImmutableMap.of(sourceName, fragmentSource), sourceProvider, metadata);
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
                e.printStackTrace();
                taskOutput.queryFailed(e);
                throw Throwables.propagate(e);
            }
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                    .add("taskId", taskId)
                    .add("splits", fragmentSources)
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
                Map<String, PlanFragmentSource> fragmentSources,
                PlanFragmentSourceProvider sourceProvider,
                Metadata metadata)
        {
            this.taskOutput = taskOutput;

            ExecutionPlanner planner = new ExecutionPlanner(new SessionMetadata(metadata),
                    sourceProvider,
                    fragment.getSymbols(),
                    fragmentSources);

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
            try {
                for (Page page : operator) {
                    taskOutput.addPage(page);
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
