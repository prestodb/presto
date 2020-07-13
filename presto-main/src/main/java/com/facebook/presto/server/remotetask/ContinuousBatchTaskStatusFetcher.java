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
package com.facebook.presto.server.remotetask;

import com.facebook.airlift.concurrent.SetThreadName;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.server.smile.BaseResponse;
import com.facebook.presto.server.smile.Codec;
import com.facebook.presto.server.smile.SmileCodec;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import javax.annotation.PostConstruct;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CURRENT_STATE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_WAIT;
import static com.facebook.presto.server.RequestHelpers.setContentTypeHeaders;
import static com.facebook.presto.server.smile.AdaptingJsonResponseHandler.createAdaptingJsonResponseHandler;
import static com.facebook.presto.server.smile.FullSmileResponseHandler.createFullSmileResponseHandler;
import static com.facebook.presto.server.smile.JsonCodecWrapper.unwrapJsonCodec;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_MISMATCH;
import static com.facebook.presto.util.Failures.REMOTE_TASK_MISMATCH_ERROR;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ContinuousBatchTaskStatusFetcher
{
    public class Task {
        TaskId taskId;
        Consumer<Throwable> onFail;
        StateMachine<TaskStatus> taskStatus;
        Codec<TaskStatus> taskStatusCodec;
        Duration refreshMaxWait;
        Executor executor;
        HttpClient httpClient;
        RequestErrorTracker errorTracker;
        RemoteTaskStats stats;
        boolean isBinaryTransportEnabled;

        AtomicLong currentRequestStartNanos = new AtomicLong();
    }

    // TaskId -> WorkerId (String(URI(TaskStatus.getSelf() (get worker id)))
    private final ConcurrentHashMap<TaskId, String> idWorkerMap;
    // String(URI(TaskStatus.getSelf() (get worker id))) -> WorkerTaskStatusFetcher (List<Task>)
    private final ConcurrentHashMap<String, WorkerTaskStatusFetcher> workerTaskMap;

    private static final Logger log = Logger.get(ContinuousBatchTaskStatusFetcher.class);

    private final AtomicLong currentRequestStartNanos = new AtomicLong();

    @GuardedBy("this")
    private boolean running;

    @GuardedBy("this")
    private ListenableFuture<BaseResponse<TaskStatus>> future;

    public ContinuousBatchTaskStatusFetcher() {
        idWorkerMap = new ConcurrentHashMap<>();
        workerTaskMap = new ConcurrentHashMap<>();
    }

    @PostConstruct
    public void addTask(
            Consumer<Throwable> onFail,
            TaskId taskId,
            TaskStatus initialTaskStatus,
            Duration refreshMaxWait,
            Codec<TaskStatus> taskStatusCodec,
            Executor executor,
            HttpClient httpClient,
            Duration maxErrorDuration,
            ScheduledExecutorService errorScheduledExecutor,
            RemoteTaskStats stats,
            boolean isBinaryTransportEnabled)
    {
        Task newTask = new Task();
        requireNonNull(initialTaskStatus, "initialTaskStatus is null");

        newTask.taskId = requireNonNull(taskId, "taskId is null");
        newTask.onFail = requireNonNull(onFail, "onFail is null");
        newTask.taskStatus = new StateMachine<>("task-" + taskId, executor, initialTaskStatus);

        newTask.refreshMaxWait = requireNonNull(refreshMaxWait, "refreshMaxWait is null");
        newTask.taskStatusCodec = requireNonNull(taskStatusCodec, "taskStatusCodec is null");

        newTask.executor = requireNonNull(executor, "executor is null");
        newTask.httpClient = requireNonNull(httpClient, "httpClient is null");

        newTask.errorTracker = new RequestErrorTracker(taskId, initialTaskStatus.getSelf(),
                maxErrorDuration, errorScheduledExecutor, "getting task status");
        newTask.stats = requireNonNull(stats, "stats is null");
        newTask.isBinaryTransportEnabled = isBinaryTransportEnabled;

        String worker = initialTaskStatus.getSelf().getHost();
        if (!workerTaskMap.containsKey(worker)) {
            workerTaskMap.put(worker, new WorkerTaskStatusFetcher());
        }
        workerTaskMap.get(worker).addTask(taskId, newTask);
    }

    @PostConstruct
    public synchronized void start()
    {
        if (running) { // We have already called start
            // already running
            return;
        }
        running = true;
        scheduleNextRequest();
    }

    public synchronized void stop()
    {
        running = false;
        if (future != null) {
            // do not terminate if the request is already running to avoid closing pooled connections
            future.cancel(false);
            future = null;
        }
    }

    private synchronized void scheduleNextRequest()
    {
        for (WorkerTaskStatusFetcher workerTaskStatusFetcher: workerTaskMap.values()) {
            workerTaskStatusFetcher.scheduleNextRequest();
        }
    }

    TaskStatus getTaskStatus(TaskId taskId)
    {
        WorkerTaskStatusFetcher worker = workerTaskMap.get(idWorkerMap.get(taskId));
        return worker.getTaskStatus(taskId);
    }

    public synchronized boolean isRunning()
    {
        // [OLD] return running;
        return workerTaskMap.size() > 0;  // There is at least one worker running a task
    }

    /**
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    public void addStateChangeListener(StateMachine.StateChangeListener<List<TaskStatus>> stateChangeListener)
    {
        // [NEW] worker.addStateChangeListener(stateChangeListener);
        // [OLD] taskStatus.addStateChangeListener(stateChangeListener);
    }
}
