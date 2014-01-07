/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.server;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.TaskSource;
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.execution.BufferResult;
import com.facebook.presto.execution.SharedBuffer;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.util.Threads;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.inject.Inject;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.util.Failures.toFailures;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class MockTaskManager
        implements TaskManager
{
    private final Executor executor = Executors.newCachedThreadPool(Threads.daemonThreadsNamed("test-%d"));

    private final HttpServerInfo httpServerInfo;
    private final DataSize maxBufferSize;
    private final int initialPages;

    private final ConcurrentMap<TaskId, MockTask> tasks = new ConcurrentHashMap<>();

    @Inject
    public MockTaskManager(HttpServerInfo httpServerInfo)
    {
        this(httpServerInfo, new DataSize(100, Unit.MEGABYTE), 12);
    }

    public MockTaskManager(HttpServerInfo httpServerInfo, DataSize maxBufferSize, int initialPages)
    {
        checkNotNull(httpServerInfo, "httpServerInfo is null");
        Preconditions.checkArgument(maxBufferSize.toBytes() > 0, "pageBufferMax must be at least 1");
        Preconditions.checkArgument(initialPages >= 0, "initialPages is negative");
        Preconditions.checkArgument(initialPages <= maxBufferSize.toBytes(), "initialPages is greater than maxBufferSize");
        this.httpServerInfo = httpServerInfo;
        this.maxBufferSize = maxBufferSize;
        this.initialPages = initialPages;
    }

    @Override
    public synchronized List<TaskInfo> getAllTaskInfo(boolean full)
    {
        ImmutableList.Builder<TaskInfo> builder = ImmutableList.builder();
        for (MockTask task : tasks.values()) {
            builder.add(task.getTaskInfo());
        }
        return builder.build();
    }

    @Override
    public void waitForStateChange(TaskId taskId, TaskState currentState, Duration maxWait)
            throws InterruptedException
    {
    }

    @Override
    public synchronized TaskInfo getTaskInfo(TaskId taskId, boolean full)
    {
        checkNotNull(taskId, "taskId is null");

        MockTask task = tasks.get(taskId);
        if (task == null) {
            throw new NoSuchElementException();
        }
        return task.getTaskInfo();
    }

    @Override
    public synchronized TaskInfo updateTask(Session session, TaskId taskId, PlanFragment ignored, List<TaskSource> sources, OutputBuffers outputBuffers)
    {
        checkNotNull(session, "session is null");
        checkNotNull(taskId, "taskId is null");
        checkNotNull(sources, "sources is null");
        checkNotNull(outputBuffers, "outputBuffers is null");

        MockTask task = tasks.get(taskId);
        if (task == null) {
            task = new MockTask(session,
                    taskId,
                    uriBuilderFrom(httpServerInfo.getHttpUri()).appendPath("v1/task").appendPath(taskId.toString()).build(),
                    outputBuffers,
                    maxBufferSize,
                    initialPages,
                    executor
            );
            tasks.put(taskId, task);
        }
        task.addOutputBuffers(outputBuffers);

        return task.getTaskInfo();
    }

    @Override
    public BufferResult getTaskResults(TaskId taskId, String outputId, long startingSequenceId, DataSize maxSize, Duration maxWaitTime)
            throws InterruptedException
    {
        checkNotNull(taskId, "taskId is null");
        checkNotNull(outputId, "outputId is null");

        MockTask task;
        synchronized (this) {
            task = tasks.get(taskId);
        }

        if (task == null) {
            throw new NoSuchElementException();
        }
        return task.getResults(outputId, startingSequenceId, maxSize, maxWaitTime);
    }

    @Override
    public synchronized TaskInfo abortTaskResults(TaskId taskId, String outputId)
    {
        checkNotNull(taskId, "taskId is null");
        checkNotNull(outputId, "outputId is null");

        MockTask task = tasks.get(taskId);
        if (task == null) {
            throw new NoSuchElementException();
        }
        task.abortResults(outputId);
        return task.getTaskInfo();
    }

    @Override
    public synchronized TaskInfo cancelTask(TaskId taskId)
    {
        checkNotNull(taskId, "taskId is null");

        MockTask task = tasks.get(taskId);
        if (task == null) {
            return null;
        }

        task.cancel();
        return task.getTaskInfo();
    }

    public static class MockTask
    {
        private final AtomicLong nextTaskInfoVersion = new AtomicLong(TaskInfo.STARTING_VERSION);

        private final URI location;
        private final TaskStateMachine taskStateMachine;
        private final TaskContext taskContext;
        private final SharedBuffer sharedBuffer;

        public MockTask(Session session,
                TaskId taskId,
                URI location,
                OutputBuffers outputBuffers,
                DataSize maxBufferSize,
                int initialPages,
                Executor executor)
        {
            this.taskStateMachine = new TaskStateMachine(checkNotNull(taskId, "taskId is null"), checkNotNull(executor, "executor is null"));
            this.taskContext = new TaskContext(taskStateMachine, executor, session, new DataSize(256, MEGABYTE), new DataSize(1, MEGABYTE), true);

            this.location = checkNotNull(location, "location is null");

            this.sharedBuffer = new SharedBuffer(taskId, executor, checkNotNull(maxBufferSize, "maxBufferSize is null"), outputBuffers);

            List<String> data = ImmutableList.of("apple", "banana", "cherry", "date");

            // load initial pages
            for (int i = 0; i < initialPages; i++) {
                checkState(sharedBuffer.enqueue(new Page(createStringsBlock(Iterables.concat(Collections.nCopies(i + 1, data))))).isDone(), "Unable to add page to buffer");
            }
            sharedBuffer.finish();
        }

        public void abortResults(String outputId)
        {
            sharedBuffer.abort(outputId);
        }

        public void addOutputBuffers(OutputBuffers outputBuffers)
        {
            sharedBuffer.setOutputBuffers(outputBuffers);
        }

        public void cancel()
        {
            taskStateMachine.cancel();
        }

        public BufferResult getResults(String outputId, long startingSequenceId, DataSize maxSize, Duration maxWaitTime)
                throws InterruptedException
        {
            return sharedBuffer.get(outputId, startingSequenceId, maxSize, maxWaitTime);
        }

        public TaskInfo getTaskInfo()
        {
            TaskState state = taskStateMachine.getState();
            List<FailureInfo> failures = ImmutableList.of();
            if (state == TaskState.FAILED) {
                failures = toFailures(taskStateMachine.getFailureCauses());
            }

            return new TaskInfo(
                    taskStateMachine.getTaskId(),
                    nextTaskInfoVersion.getAndIncrement(),
                    state,
                    location,
                    DateTime.now(),
                    sharedBuffer.getInfo(),
                    ImmutableSet.<PlanNodeId>of(),
                    taskContext.getTaskStats(),
                    failures,
                    taskContext.getOutputItems());
        }
    }
}
