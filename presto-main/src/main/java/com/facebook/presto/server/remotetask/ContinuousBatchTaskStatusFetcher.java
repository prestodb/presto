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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.server.smile.BaseResponse;
import com.facebook.presto.server.smile.Codec;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import javax.annotation.PostConstruct;
import javax.annotation.concurrent.GuardedBy;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class ContinuousBatchTaskStatusFetcher
{
    public class Task {
        TaskId taskId;
        Consumer<Throwable> onFail;
        StateMachine<TaskStatus> taskStatus;
        Codec<TaskStatus> taskStatusCodec;
        Executor executor;
        RequestErrorTracker errorTracker;
        RemoteTaskStats stats;
        boolean isBinaryTransportEnabled;
    }

    // TaskId -> WorkerId (String(URI(TaskStatus.getSelf() (get worker id)))
    private final ConcurrentHashMap<TaskId, String> idWorkerMap;
    // String(URI(TaskStatus.getSelf() (get worker id))) -> WorkerTaskStatusFetcher (List<Task>)
    private final ConcurrentHashMap<String, WorkerTaskStatusFetcher> workerTaskMap;

    private static final Logger log = Logger.get(ContinuousBatchTaskStatusFetcher.class);

    private final Duration refreshMaxWait;
    private final HttpClient httpClient;

    @GuardedBy("this")
    private ListenableFuture<BaseResponse<TaskStatus>> future;

    public ContinuousBatchTaskStatusFetcher(
            Duration refreshMaxWait,
            HttpClient httpClient) {
        idWorkerMap = new ConcurrentHashMap<>();
        workerTaskMap = new ConcurrentHashMap<>();

        this.refreshMaxWait = requireNonNull(refreshMaxWait, "refreshMaxWait is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    public void addTask(
            Consumer<Throwable> onFail,
            TaskId taskId,
            TaskStatus initialTaskStatus,
            Codec<TaskStatus> taskStatusCodec,
            Executor executor,
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

        newTask.taskStatusCodec = requireNonNull(taskStatusCodec, "taskStatusCodec is null");
        newTask.executor = requireNonNull(executor, "executor is null");

        newTask.errorTracker = new RequestErrorTracker(taskId, initialTaskStatus.getSelf(),
                maxErrorDuration, errorScheduledExecutor, "getting task status");
        newTask.stats = requireNonNull(stats, "stats is null");
        newTask.isBinaryTransportEnabled = isBinaryTransportEnabled;


        String worker = initialTaskStatus.getSelf().getHost();
        idWorkerMap.put(taskId, worker);
        if (!workerTaskMap.containsKey(worker)) {
            workerTaskMap.put(worker, new WorkerTaskStatusFetcher(worker, refreshMaxWait, httpClient));
        }
        workerTaskMap.get(worker).addTask(newTask);
    }

    @PostConstruct
    public synchronized void start()
    {
        if (isRunning()) { // We have already called start
            return;
        }
        scheduleNextRequest();
    }

    public synchronized void stop()
    {
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
        for (WorkerTaskStatusFetcher workerTaskStatusFetcher: workerTaskMap.values()) {
            if (workerTaskStatusFetcher.isRunning()) return true;
        }
        return false;
    }
}
