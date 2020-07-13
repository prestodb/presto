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

class WorkerTaskStatusFetcher
        implements SimpleHttpResponseCallback<List<TaskStatus>>
{
    private final ConcurrentHashMap<TaskId, ContinuousBatchTaskStatusFetcher.Task> idTaskMap;

    private static final Logger log = Logger.get(ContinuousTaskStatusFetcher.class);

    private final TaskId taskId; // TODO: Remove after removing references
    private final StateMachine<TaskStatus> taskStatus; // TODO: Remove after removing references

    private final Consumer<Throwable> onFail;
    private final Codec<TaskStatus> taskStatusCodec;
    private final Duration refreshMaxWait;
    private final Executor executor;
    private final HttpClient httpClient;
    private final RequestErrorTracker errorTracker;
    private final RemoteTaskStats stats;
    private final boolean isBinaryTransportEnabled;

    private final AtomicLong currentRequestStartNanos = new AtomicLong();

    @GuardedBy("this")
    private boolean running;

    @GuardedBy("this")
    private ListenableFuture<BaseResponse<TaskStatus>> future;

    public WorkerTaskStatusFetcher() {
        this(null, null, null, null, null, null, null, null, null, null, false);
    }

    // TODO: We don't need all the parameters here and the logic should be in CBTSF
    public WorkerTaskStatusFetcher(
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
        idTaskMap = new ConcurrentHashMap<>();

        //requireNonNull(initialTaskStatus, "initialTaskStatus is null");

        this.taskId = requireNonNull(taskId, "taskId is null");
        this.taskStatus = new StateMachine<>("task-" + taskId, executor, initialTaskStatus);
        this.onFail = requireNonNull(onFail, "onFail is null");

        this.refreshMaxWait = requireNonNull(refreshMaxWait, "refreshMaxWait is null");
        this.taskStatusCodec = requireNonNull(taskStatusCodec, "taskStatusCodec is null");

        this.executor = requireNonNull(executor, "executor is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");

        this.errorTracker = new RequestErrorTracker(taskId, initialTaskStatus.getSelf(), maxErrorDuration, errorScheduledExecutor, "getting task status");
        this.stats = requireNonNull(stats, "stats is null");
        this.isBinaryTransportEnabled = isBinaryTransportEnabled;
    }

    public void addTask(TaskId taskId, ContinuousBatchTaskStatusFetcher.Task task) {
        idTaskMap.put(taskId, task);
    }

    TaskStatus getTaskStatus(TaskId taskId)
    {
        return idTaskMap.get(taskId).taskStatus.get();
    }

    // TODO: Schedule request per worker
    public synchronized void scheduleNextRequest()
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

        Request request = setContentTypeHeaders(isBinaryTransportEnabled, prepareGet())
                .setUri(uriBuilderFrom(taskStatus.getSelf()).appendPath("status").build())
                .setHeader(PRESTO_CURRENT_STATE, taskStatus.getState().toString())
                .setHeader(PRESTO_MAX_WAIT, refreshMaxWait.toString())
                .build();

        ResponseHandler responseHandler;
        if (isBinaryTransportEnabled) {
            responseHandler = createFullSmileResponseHandler((SmileCodec<TaskStatus>) taskStatusCodec);
        }
        else {
            responseHandler = createAdaptingJsonResponseHandler(unwrapJsonCodec(taskStatusCodec));
        }

        errorTracker.startRequest();
        future = httpClient.executeAsync(request, responseHandler);
        currentRequestStartNanos.set(System.nanoTime());
        Futures.addCallback(future, new SimpleHttpResponseHandler<>(this, request.getUri(), stats), executor);
    }

    @Override
    public void success(List<TaskStatus> value)
    {
        try (SetThreadName ignored = new SetThreadName("ContinuousBatchTaskStatusFetcher-%s", taskId)) {
            updateStats(currentRequestStartNanos.get());
            try {
                updateAllTaskStatus(value);
                errorTracker.requestSucceeded();
            }
            finally {
                // Signal to CBTSF we're done
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
                TaskStatus taskStatus = getTaskStatus(taskId); // TODO: taskId isn't a thing. Fix.
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
                // Signal to CBTSF we're done
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

    void updateAllTaskStatus(List<TaskStatus> newValues)
    {
        for (TaskStatus newValue: newValues) {
            updateTaskStatus(newValue);
        }
    }

    // TODO: Update logic to handle response list instead of individual TaskStatus
    void updateTaskStatus(TaskStatus newValue)
    {
        // change to new value if old value is not changed and new value has a newer version
        AtomicBoolean taskMismatch = new AtomicBoolean();
        taskStatus.setIf(newValue, oldValue -> {
            // did the task instance id change
            boolean isEmpty = oldValue.getTaskInstanceIdLeastSignificantBits() == 0 && oldValue.getTaskInstanceIdMostSignificantBits() == 0;
            if (!isEmpty &&
                    !(oldValue.getTaskInstanceIdLeastSignificantBits() == newValue.getTaskInstanceIdLeastSignificantBits() &&
                            oldValue.getTaskInstanceIdMostSignificantBits() == newValue.getTaskInstanceIdMostSignificantBits())) {
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
            onFail.accept(new PrestoException(REMOTE_TASK_MISMATCH, format("%s (%s)", REMOTE_TASK_MISMATCH_ERROR, HostAddress.fromUri(getTaskStatus(taskId).getSelf()))));
        }
    }

    public synchronized boolean isRunning()
    {
        return running;
    }

    /**
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    public void addStateChangeListener(StateMachine.StateChangeListener<TaskStatus> stateChangeListener)
    {
        taskStatus.addStateChangeListener(stateChangeListener);
    }

    private void updateStats(long currentRequestStartNanos)
    {
        stats.statusRoundTripMillis(nanosSince(currentRequestStartNanos).toMillis());
    }
}
