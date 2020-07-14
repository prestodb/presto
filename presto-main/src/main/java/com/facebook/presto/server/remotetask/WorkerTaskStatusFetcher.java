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
import com.facebook.presto.execution.TaskState;
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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
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

    private final String worker;
    private final Consumer<Throwable> onFail;
    private final Codec<List<TaskStatus>> taskListStatusCodec;
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
    private ListenableFuture<BaseResponse<List<TaskStatus>>> future;

    public WorkerTaskStatusFetcher(
            String worker,
            Consumer<Throwable> onFail,
            Duration refreshMaxWait,
            Codec<List<TaskStatus>> taskListStatusCodec,
            Executor executor,
            HttpClient httpClient,
            RemoteTaskStats stats,
            boolean isBinaryTransportEnabled)
    {
        this.worker = worker;
        idTaskMap = new ConcurrentHashMap<>();

        //requireNonNull(initialTaskStatus, "initialTaskStatus is null");

        this.onFail = requireNonNull(onFail, "onFail is null");

        this.refreshMaxWait = requireNonNull(refreshMaxWait, "refreshMaxWait is null");
        this.taskListStatusCodec = requireNonNull(taskListStatusCodec, "taskStatusCodec is null");

        this.executor = requireNonNull(executor, "executor is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");

        // TODO: Create new ErrorTracker
        this.errorTracker = new RequestErrorTracker(taskId, initialTaskStatus.getSelf(), maxErrorDuration, errorScheduledExecutor, "getting task status");
        this.stats = requireNonNull(stats, "stats is null");
        this.isBinaryTransportEnabled = isBinaryTransportEnabled;
    }

    public void addTask(ContinuousBatchTaskStatusFetcher.Task task) {
        idTaskMap.put(task.taskId, task);
    }

    TaskStatus getTaskStatus(TaskId taskId)
    {
        return idTaskMap.get(taskId).taskStatus.get();
    }

    // TODO: Schedule request per worker
    public synchronized void scheduleNextRequest()
    {
        // stopped or done?
        if (!running) {
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

        URI uri;
        try {
            uri = uriBuilderFrom(new URI(worker)).appendPath("/tasks/status").build();
        } catch (URISyntaxException e) {
            uri = null;
        }

        HashMap<TaskId, TaskState> idStateMap = new HashMap<>(); // Maybe we need to make this JSON instead?
        for (ContinuousBatchTaskStatusFetcher.Task task: idTaskMap.values()) {
            idStateMap.put(task.taskId, task.taskStatus.get().getState());
        }

        Request request = setContentTypeHeaders(isBinaryTransportEnabled, prepareGet())
                .setUri(uri)
                .setHeader(PRESTO_CURRENT_STATE, idStateMap.toString())
                .setHeader(PRESTO_MAX_WAIT, refreshMaxWait.toString())
                .build();

        ResponseHandler responseHandler;
        if (isBinaryTransportEnabled) {
            responseHandler = createFullSmileResponseHandler((SmileCodec<List<TaskStatus>>) taskListStatusCodec);
        }
        else {
            responseHandler = createAdaptingJsonResponseHandler(unwrapJsonCodec(taskListStatusCodec));
        }

        errorTracker.startRequest();
        future = httpClient.executeAsync(request, responseHandler);
        currentRequestStartNanos.set(System.nanoTime());
        Futures.addCallback(future, new SimpleHttpResponseHandler<>(this, request.getUri(), stats), executor);
    }

    @Override
    public void success(List<TaskStatus> value)
    {
        try (SetThreadName ignored = new SetThreadName("ContinuousBatchTaskStatusFetcher-%s", idTaskMap)) {
            updateStats(currentRequestStartNanos.get());
            try {
                updateAllTaskStatus(value);
                errorTracker.requestSucceeded();
            }
            finally {
                // Signal to CBTSF we're done
                scheduleNextRequest();
            }
        }
    }

    @Override
    public void failed(Throwable cause)
    {
        try (SetThreadName ignored = new SetThreadName("ContinuousTaskStatusFetcher-%s", idTaskMap)) {
            updateStats(currentRequestStartNanos.get());
            try {
                for (ContinuousBatchTaskStatusFetcher.Task task: idTaskMap.values()) {
                    // if task not already done, record error
                    TaskStatus taskStatus = task.taskStatus.get();
                    if (!taskStatus.getState().isDone()) {
                        errorTracker.requestFailed(cause);
                    }
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
                scheduleNextRequest();
            }
        }
    }

    @Override
    public void fatal(Throwable cause)
    {
        try (SetThreadName ignored = new SetThreadName("ContinuousTaskStatusFetcher-%s", idTaskMap)) {
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
            onFail.accept(new PrestoException(REMOTE_TASK_MISMATCH, format("%s (%s)", REMOTE_TASK_MISMATCH_ERROR, HostAddress.fromUri(newValue.getSelf()))));
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
    public void addStateChangeListener(StateMachine.StateChangeListener<List<TaskStatus>> stateChangeListener)
    {
        // taskStatus.addStateChangeListener(stateChangeListener);
    }

    private void updateStats(long currentRequestStartNanos)
    {
        stats.statusRoundTripMillis(nanosSince(currentRequestStartNanos).toMillis());
    }
}
