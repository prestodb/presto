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

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.concurrent.SetThreadName;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.operator.ForScheduler;
import com.facebook.presto.server.InternalCommunicationConfig;
import com.facebook.presto.server.smile.BaseResponse;
import com.facebook.presto.server.smile.Codec;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.units.Duration;

import javax.annotation.PostConstruct;
import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class ContinuousBatchTaskStatusFetcher
{
    public class Task
    {
        TaskId taskId;
        StateMachine<TaskStatus> taskStatus;
        Codec<Map<TaskId, TaskStatus>> taskListStatusCodec;
    }

    // TaskId -> WorkerId (String(URI(TaskStatus.getSelf() (get worker id)))
    private final ConcurrentHashMap<TaskId, String> idWorkerMap;
    // String(URI(TaskStatus.getSelf() (get worker id))) -> WorkerTaskStatusFetcher (List<Task>)
    private final ConcurrentHashMap<String, WorkerTaskStatusFetcher> workerTaskMap;

    private static final Logger log = Logger.get(ContinuousBatchTaskStatusFetcher.class);

    private final Consumer<Throwable> onFail;
    private final Duration refreshMaxWait;
    private final Executor executor;
    private final HttpClient httpClient;
    private final Duration maxErrorDuration;
    // private final ScheduledExecutorService errorScheduledExecutor;
    private final RemoteTaskStats stats;
    private final boolean isBinaryTransportEnabled;

    @GuardedBy("this")
    private ListenableFuture<BaseResponse<TaskStatus>> future;

    @Inject
    public ContinuousBatchTaskStatusFetcher(
            @ForScheduler HttpClient httpClient,
            @ForScheduler ScheduledExecutorService errorScheduledExecutor,
            RemoteTaskStats stats,
            TaskManagerConfig taskConfig,
            QueryManagerConfig config,
            InternalCommunicationConfig communicationConfig)
    {
        idWorkerMap = new ConcurrentHashMap<>();
        workerTaskMap = new ConcurrentHashMap<>();

        this.onFail = this::failTask;
        this.httpClient = requireNonNull(httpClient, "httpClient is null");

        ExecutorService coreExecutor = newCachedThreadPool(daemonThreadsNamed("remote-task-callback-%s"));
        this.executor = new BoundedExecutor(coreExecutor, config.getRemoteTaskMaxCallbackThreads());

        Duration refreshMaxWait = taskConfig.getInfoRefreshMaxWait();
        this.refreshMaxWait = requireNonNull(refreshMaxWait, "refreshMaxWait is null");

        this.maxErrorDuration = config.getRemoteTaskMaxErrorDuration();
        // this.errorScheduledExecutor = requireNonNull(errorScheduledExecutor, "errorScheduledExecutor is null");
        this.stats = requireNonNull(stats, "stats is null");
        isBinaryTransportEnabled = requireNonNull(communicationConfig, "communicationConfig is null").isBinaryTransportEnabled();
    }

    private void failTask(Throwable cause)
    {
        // Dummy method for onFail
    }

    public void addTask(
            TaskId taskId,
            TaskStatus initialTaskStatus,
            Codec<Map<TaskId, TaskStatus>> taskListStatusCodec)
    {
        Task newTask = new Task();
        requireNonNull(initialTaskStatus, "initialTaskStatus is null");

        newTask.taskId = requireNonNull(taskId, "taskId is null");
        newTask.taskStatus = new StateMachine<>("task-" + taskId, executor, initialTaskStatus);
        newTask.taskListStatusCodec = requireNonNull(taskListStatusCodec, "taskListStatusCodec is null");

        URI workerURI = initialTaskStatus.getSelf();
        String worker = initialTaskStatus.getSelf().getHost();
        idWorkerMap.put(taskId, worker);
        if (!workerTaskMap.containsKey(worker)) {
            workerTaskMap.put(
                    worker,
                    new WorkerTaskStatusFetcher(
                            worker,
                            workerURI,
                            onFail,
                            refreshMaxWait,
                            taskListStatusCodec,
                            executor,
                            httpClient,
                            maxErrorDuration,
                            // errorScheduledExecutor,
                            stats,
                            isBinaryTransportEnabled,
                            Integer.toString(worker.hashCode())));
            scheduleNextRequest(workerTaskMap.get(worker));
        }
        workerTaskMap.get(worker).addTask(newTask);
        workerTaskMap.get(worker).addStateChangeListener(newTask.taskId, newStatus -> {
            TaskState state = newStatus.getState();
            if (state.isDone()) { // We can worry about the details later
                // cleanUpTask();
            }
            else {
                // partitionedSplitCountTracker.setPartitionedSplitCount(getPartitionedSplitCount());
                // updateSplitQueueSpace();
            }
        });
    }

    @PostConstruct
    public synchronized void start() // We probably don't need this method, since all logic is in addTask
    {
        if (isRunning()) { // We have already called start
            return;
        }
    }

    public synchronized void stop()
    {
        if (future != null) {
            // do not terminate if the request is already running to avoid closing pooled connections
            future.cancel(false);
            future = null;
        }
    }

    private synchronized void scheduleNextRequest(WorkerTaskStatusFetcher workerTaskStatusFetcher)
    {
        try (SetThreadName ignored = new SetThreadName("WorkerTaskStatusFetcher-%s", workerTaskStatusFetcher)) {
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
        for (WorkerTaskStatusFetcher workerTaskStatusFetcher : workerTaskMap.values()) {
            if (workerTaskStatusFetcher.isRunning()) {
                return true;
            }
        }
        return false;
    }

    public void addStateChangeListener(TaskId taskId, StateMachine.StateChangeListener<TaskStatus> stateChangeListener)
    {
        //workerTaskMap.get(idWorkerMap.get(taskId)).getTaskStateMachine(taskId).addStateChangeListener(stateChangeListener);
        workerTaskMap.get(idWorkerMap.get(taskId)).addStateChangeListener(taskId, stateChangeListener);
    }
}
