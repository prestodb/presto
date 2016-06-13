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

import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskStatus;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TaskInfoFetcher
        implements SimpleHttpResponseCallback<TaskInfo>
{
    private static final Logger log = Logger.get(TaskInfoFetcher.class);

    private final TaskId taskId;
    private final Consumer<Throwable> onFail;
    private final StateMachine<TaskInfo> taskInfo;
    private final JsonCodec<TaskInfo> taskInfoCodec;

    private final long updateIntervalMillis;
    private final AtomicLong lastUpdateNanos = new AtomicLong();
    private final ScheduledExecutorService updateScheduledExecutor;

    private final Executor executor;
    private final HttpClient httpClient;
    private final RequestErrorTracker errorTracker;

    @GuardedBy("this")
    private final AtomicLong currentRequestStartNanos = new AtomicLong();

    @GuardedBy("this")
    private final AtomicReference<TaskStatus> taskStatusDone = new AtomicReference();

    @GuardedBy("this")
    private final AtomicBoolean lastRequest = new AtomicBoolean();

    private final RemoteTaskStats stats;

    @GuardedBy("this")
    private boolean running;

    @GuardedBy("this")
    private ScheduledFuture<?> scheduledFuture;

    @GuardedBy("this")
    private ListenableFuture<FullJsonResponseHandler.JsonResponse<TaskInfo>> future;

    public TaskInfoFetcher(
            Consumer<Throwable> onFail,
            TaskInfo initialTask,
            HttpClient httpClient,
            Duration updateInterval,
            JsonCodec<TaskInfo> taskInfoCodec,
            Duration minErrorDuration,
            Executor executor,
            ScheduledExecutorService updateScheduledExecutor,
            ScheduledExecutorService errorScheduledExecutor,
            RemoteTaskStats stats)
    {
        requireNonNull(initialTask, "initialTask is null");
        requireNonNull(minErrorDuration, "minErrorDuration is null");
        requireNonNull(errorScheduledExecutor, "errorScheduledExecutor is null");

        this.taskId = initialTask.getTaskStatus().getTaskId();
        this.onFail = requireNonNull(onFail, "onFail is null");
        this.taskInfo = new StateMachine<>("task " + taskId, executor, initialTask);
        this.taskInfoCodec = requireNonNull(taskInfoCodec, "taskInfoCodec is null");

        this.updateIntervalMillis = requireNonNull(updateInterval, "updateInterval is null").toMillis();
        this.updateScheduledExecutor = requireNonNull(updateScheduledExecutor, "updateScheduledExecutor is null");
        this.errorTracker = new RequestErrorTracker(taskId, initialTask.getTaskStatus().getSelf(), minErrorDuration, errorScheduledExecutor, "getting info for task");

        this.executor = requireNonNull(executor, "executor is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    public TaskInfo getTaskInfo()
    {
        return taskInfo.get();
    }

    public synchronized void start()
    {
        if (running) {
            // already running
            return;
        }
        running = true;
        scheduleUpdate();
    }

    public synchronized void abort(TaskStatus taskStatus)
    {
        updateTaskInfo(taskInfo.get().withTaskStatus(taskStatus));
        // For aborted tasks we do not care about the final task info, so stop the info fetcher
        stop();
    }

    public synchronized void taskStatusDone(TaskStatus taskStatus)
    {
        if (running && !isDone(getTaskInfo())) {
            // try to get the last task status from the remote task
            taskStatusDone.set(taskStatus);
        }
        else {
            getTaskInfo().withTaskStatus(taskStatus);
        }
    }

    private synchronized void stop()
    {
        running = false;
        if (future != null) {
            future.cancel(true);
            future = null;
        }
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
    }

    private synchronized void scheduleUpdate()
    {
        scheduledFuture = updateScheduledExecutor.scheduleWithFixedDelay(() -> {
            synchronized (this) {
                // if the previous request still running, don't schedule a new request
                if (future != null && !future.isDone()) {
                    return;
                }
            }
            if (nanosSince(lastUpdateNanos.get()).toMillis() >= updateIntervalMillis) {
                sendNextRequest();
            }
        }, 0, 100, MILLISECONDS);
    }

    private synchronized void sendNextRequest()
    {
        TaskStatus taskStatus = getTaskInfo().getTaskStatus();

        if (!running) {
            return;
        }

        // we already have the final task info
        if (isDone(getTaskInfo())) {
            stop();
            return;
        }

        // if we have an outstanding request
        if (future != null && !future.isDone()) {
            return;
        }

        if (taskStatusDone.get() != null) {
            lastRequest.set(true);
        }

        // if throttled due to error, asynchronously wait for timeout and try again
        ListenableFuture<?> errorRateLimit = errorTracker.acquireRequestPermit();
        if (!errorRateLimit.isDone()) {
            errorRateLimit.addListener(this::sendNextRequest, executor);
            return;
        }

        HttpUriBuilder httpUriBuilder = uriBuilderFrom(taskStatus.getSelf());
        // if this is the last request, don't summarize, get the complete taskInfo
        URI uri = lastRequest.get() ? httpUriBuilder.build() : httpUriBuilder.addParameter("summarize").build();

        Request request = prepareGet()
                .setUri(uri)
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .build();

        errorTracker.startRequest();
        future = httpClient.executeAsync(request, createFullJsonResponseHandler(taskInfoCodec));
        currentRequestStartNanos.set(System.nanoTime());
        Futures.addCallback(future, new SimpleHttpResponseHandler<>(this, request.getUri(), stats), executor);
    }

    synchronized void updateTaskInfo(TaskInfo newValue)
    {
        taskInfo.setIf(newValue, oldValue -> {
            TaskStatus oldTaskStatus = oldValue.getTaskStatus();
            TaskStatus newTaskStatus = newValue.getTaskStatus();
            if (oldTaskStatus.getState().isDone()) {
                // never update if the task has reached a terminal state
                return false;
            }
            // don't update to an older version (same version is ok)
            return newTaskStatus.getVersion() >= oldTaskStatus.getVersion();
        });
    }

    @Override
    public void success(TaskInfo newValue)
    {
        try (SetThreadName ignored = new SetThreadName("TaskInfoFetcher-%s", taskId)) {
            lastUpdateNanos.set(System.nanoTime());
            updateStats(currentRequestStartNanos.get());

            errorTracker.requestSucceeded();
            updateTaskInfo(newValue);

            // if taskStatus is done, and the newValue that we got is also done,
            // we have the final task info so we can stop
            if (lastRequest.get() && !newValue.getTaskStatus().getState().isDone()) {
                log.error("%s taskStatus done but taskInfo is not", taskId);

                // Since the task is already done, update the task status
                updateTaskInfo(taskInfo.get().withTaskStatus(taskStatusDone.get()));
            }

            if (lastRequest.get()) {
                stop();
            }
        }
    }

    @Override
    public void failed(Throwable cause)
    {
        try (SetThreadName ignored = new SetThreadName("TaskInfoFetcher-%s", taskId)) {
            lastUpdateNanos.set(System.nanoTime());
            updateStats(currentRequestStartNanos.get());

            try {
                // if task not already done, record error
                if (!isDone(getTaskInfo())) {
                    errorTracker.requestFailed(cause);
                }
            }
            catch (Error e) {
                // if task status is already done, ignore this failure, update the task status and stop fetching
                if (taskStatusDone.get() != null) {
                    updateTaskInfo(taskInfo.get().withTaskStatus(taskStatusDone.get()));
                    stop();
                }
                onFail.accept(e);
                throw e;
            }
            catch (RuntimeException e) {
                onFail.accept(e);
            }
        }
    }

    @Override
    public void fatal(Throwable cause)
    {
        try (SetThreadName ignored = new SetThreadName("TaskInfoFetcher-%s", taskId)) {
            // if task status is already done, ignore this failure, update the task status and stop fetching
            if (taskStatusDone.get() != null) {
                updateTaskInfo(taskInfo.get().withTaskStatus(taskStatusDone.get()));
                stop();
            }
            updateStats(currentRequestStartNanos.get());
            onFail.accept(cause);
        }
    }

    private void updateStats(long currentRequestStartNanos)
    {
        stats.infoRoundTripMillis(nanosSince(currentRequestStartNanos).toMillis());
    }

    private static boolean isDone(TaskInfo taskInfo)
    {
        return taskInfo.getTaskStatus().getState().isDone();
    }
}
