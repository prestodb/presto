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
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_CURRENT_STATE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_WAIT;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_MISMATCH;
import static com.facebook.presto.util.Failures.REMOTE_TASK_MISMATCH_ERROR;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class ContinuousTaskStatusFetcher
        implements SimpleHttpResponseCallback<TaskStatus>
{
    private static final Logger log = Logger.get(ContinuousTaskStatusFetcher.class);

    private final TaskId taskId;
    private final Consumer<Throwable> onFail;
    private final StateMachine<TaskStatus> taskStatus;
    private final JsonCodec<TaskStatus> taskStatusCodec;

    private final Duration refreshMaxWait;
    private final Executor executor;
    private final HttpClient httpClient;
    private final RequestErrorTracker errorTracker;
    private final RemoteTaskStats stats;

    private final AtomicLong currentRequestStartNanos = new AtomicLong();

    @GuardedBy("this")
    private boolean running;

    @GuardedBy("this")
    private ListenableFuture<FullJsonResponseHandler.JsonResponse<TaskStatus>> future;

    public ContinuousTaskStatusFetcher(
            Consumer<Throwable> onFail,
            TaskStatus initialTaskStatus,
            Duration refreshMaxWait,
            JsonCodec<TaskStatus> taskStatusCodec,
            Executor executor,
            HttpClient httpClient,
            Duration minErrorDuration,
            Duration maxErrorDuration,
            ScheduledExecutorService errorScheduledExecutor,
            RemoteTaskStats stats)
    {
        requireNonNull(initialTaskStatus, "initialTaskStatus is null");

        this.taskId = initialTaskStatus.getTaskId();
        this.onFail = requireNonNull(onFail, "onFail is null");
        this.taskStatus = new StateMachine<>("task-" + taskId, executor, initialTaskStatus);

        this.refreshMaxWait = requireNonNull(refreshMaxWait, "refreshMaxWait is null");
        this.taskStatusCodec = requireNonNull(taskStatusCodec, "taskStatusCodec is null");

        this.executor = requireNonNull(executor, "executor is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");

        this.errorTracker = new RequestErrorTracker(taskId, initialTaskStatus.getSelf(), minErrorDuration, maxErrorDuration, errorScheduledExecutor, "getting task status");
        this.stats = requireNonNull(stats, "stats is null");
    }

    public synchronized void start()
    {
        if (running) {
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
            future.cancel(true);
            future = null;
        }
    }

    private synchronized void scheduleNextRequest()
    {
        // stopped or done?
        TaskStatus taskStatus = getTaskStatus();
        if (!running || taskStatus.getState().isDone()) {
            return;
        }

        // outstanding request?
        if (future != null && !future.isDone()) {
            // this should never happen
            log.error("Can not reschedule update because an update is already running");
            return;
        }

        // if throttled due to error, asynchronously wait for timeout and try again
        ListenableFuture<?> errorRateLimit = errorTracker.acquireRequestPermit();
        if (!errorRateLimit.isDone()) {
            errorRateLimit.addListener(this::scheduleNextRequest, executor);
            return;
        }

        Request request = prepareGet()
                .setUri(uriBuilderFrom(taskStatus.getSelf()).appendPath("status").build())
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setHeader(PRESTO_CURRENT_STATE, taskStatus.getState().toString())
                .setHeader(PRESTO_MAX_WAIT, refreshMaxWait.toString())
                .build();

        errorTracker.startRequest();
        future = httpClient.executeAsync(request, createFullJsonResponseHandler(taskStatusCodec));
        currentRequestStartNanos.set(System.nanoTime());
        Futures.addCallback(future, new SimpleHttpResponseHandler<>(this, request.getUri(), stats), executor);
    }

    TaskStatus getTaskStatus()
    {
        return taskStatus.get();
    }

    @Override
    public void success(TaskStatus value)
    {
        try (SetThreadName ignored = new SetThreadName("ContinuousTaskStatusFetcher-%s", taskId)) {
            updateStats(currentRequestStartNanos.get());
            try {
                updateTaskStatus(value);
                errorTracker.requestSucceeded();
            }
            finally {
                scheduleNextRequest();
            }
        }
    }

    @Override
    public void failed(Throwable cause)
    {
        try (SetThreadName ignored = new SetThreadName("ContinuousTaskStatusFetcher-%s", taskId)) {
            updateStats(currentRequestStartNanos.get());
            try {
                // if task not already done, record error
                TaskStatus taskStatus = getTaskStatus();
                if (!taskStatus.getState().isDone()) {
                    errorTracker.requestFailed(cause);
                }
            }
            catch (Error e) {
                onFail.accept(e);
                throw e;
            }
            catch (RuntimeException e) {
                onFail.accept(e);
            }
            finally {
                scheduleNextRequest();
            }
        }
    }

    @Override
    public void fatal(Throwable cause)
    {
        try (SetThreadName ignored = new SetThreadName("ContinuousTaskStatusFetcher-%s", taskId)) {
            updateStats(currentRequestStartNanos.get());
            onFail.accept(cause);
        }
    }

    void updateTaskStatus(TaskStatus newValue)
    {
        // change to new value if old value is not changed and new value has a newer version
        AtomicBoolean taskMismatch = new AtomicBoolean();
        taskStatus.setIf(newValue, oldValue -> {
            // did the task instance id change
            if (!isNullOrEmpty(oldValue.getTaskInstanceId()) && !oldValue.getTaskInstanceId().equals(newValue.getTaskInstanceId())) {
                taskMismatch.set(true);
                return false;
            }

            if (oldValue.getState().isDone()) {
                // never update if the task has reached a terminal state
                return false;
            }
            if (newValue.getVersion() < oldValue.getVersion()) {
                // don't update to an older version (same version is ok)
                return false;
            }
            return true;
        });

        if (taskMismatch.get()) {
            // This will also set the task status to FAILED state directly.
            // Additionally, this will issue a DELETE for the task to the worker.
            // While sending the DELETE is not required, it is preferred because a task was created by the previous request.
            onFail.accept(new PrestoException(REMOTE_TASK_MISMATCH, format("%s (%s)", REMOTE_TASK_MISMATCH_ERROR, HostAddress.fromUri(getTaskStatus().getSelf()))));
        }
    }

    public synchronized boolean isRunning()
    {
        return running;
    }

    public void addStateChangeListener(StateMachine.StateChangeListener<TaskStatus> stateChangeListener)
    {
        taskStatus.addStateChangeListener(stateChangeListener);
    }

    public CompletableFuture<TaskStatus> getStateChange(TaskStatus taskStatus)
    {
        return this.taskStatus.getStateChange(taskStatus);
    }

    private void updateStats(long currentRequestStartNanos)
    {
        stats.statusRoundTripMillis(nanosSince(currentRequestStartNanos).toMillis());
    }
}
