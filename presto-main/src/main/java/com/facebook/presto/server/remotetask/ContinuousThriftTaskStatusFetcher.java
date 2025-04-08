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
import com.facebook.airlift.json.Codec;
import com.facebook.airlift.log.Logger;
import com.facebook.drift.transport.netty.codec.Protocol;
import com.facebook.presto.common.experimental.ExperimentalThriftResponseHandler;
import com.facebook.presto.common.experimental.auto_gen.ThriftTaskStatus;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.server.RequestErrorTracker;
import com.facebook.presto.server.smile.BaseResponse;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.thrift.ThriftRequestUtils.APPLICATION_THRIFT_BINARY;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_EXPERIMENTAL;
import static com.facebook.presto.server.RequestErrorTracker.taskRequestErrorTracker;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_MISMATCH;
import static com.facebook.presto.util.Failures.REMOTE_TASK_MISMATCH_ERROR;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class ContinuousThriftTaskStatusFetcher
{
    private static final Logger log = Logger.get(ContinuousThriftTaskStatusFetcher.class);

    private final TaskId taskId;
    private final Consumer<Throwable> onFail;
    private final StateMachine<ThriftTaskStatus> taskStatus;
    private final Codec<TaskStatus> taskStatusCodec;

    private final Duration refreshMaxWait;
    private final Executor executor;
    private final HttpClient httpClient;
    private final RequestErrorTracker errorTracker;
    private final RemoteTaskStats stats;
    private final boolean binaryTransportEnabled;
    private final boolean thriftTransportEnabled;
    private final Protocol thriftProtocol;
    private final boolean experimentalThriftEnabled;

    private final AtomicLong currentRequestStartNanos = new AtomicLong();

    @GuardedBy("this")
    private boolean running;

    @GuardedBy("this")
    private ListenableFuture<BaseResponse<ThriftTaskStatus>> future;

    public ContinuousThriftTaskStatusFetcher(
            Consumer<Throwable> onFail,
            TaskId taskId,
            ThriftTaskStatus initialTaskStatus,
            Duration refreshMaxWait,
            Codec<TaskStatus> taskStatusCodec,
            Executor executor,
            HttpClient httpClient,
            Duration maxErrorDuration,
            ScheduledExecutorService errorScheduledExecutor,
            RemoteTaskStats stats,
            boolean binaryTransportEnabled,
            boolean thriftTransportEnabled,
            Protocol thriftProtocol,
            boolean experimentalThriftEnabled)
    {
        requireNonNull(initialTaskStatus, "initialTaskStatus is null");

        this.taskId = requireNonNull(taskId, "taskId is null");
        this.onFail = requireNonNull(onFail, "onFail is null");
        this.taskStatus = new StateMachine<>("task-" + taskId, executor, initialTaskStatus);

        this.refreshMaxWait = requireNonNull(refreshMaxWait, "refreshMaxWait is null");
        this.taskStatusCodec = requireNonNull(taskStatusCodec, "taskStatusCodec is null");

        this.executor = requireNonNull(executor, "executor is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");

        this.errorTracker = taskRequestErrorTracker(taskId, URI.create(initialTaskStatus.getSelf()), maxErrorDuration, errorScheduledExecutor, "getting task status");
        this.stats = requireNonNull(stats, "stats is null");
        this.binaryTransportEnabled = binaryTransportEnabled;
        this.thriftTransportEnabled = thriftTransportEnabled;
        this.thriftProtocol = requireNonNull(thriftProtocol, "thriftProtocol is null");
        this.experimentalThriftEnabled = experimentalThriftEnabled;
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
            // do not terminate if the request is already running to avoid closing pooled connections
            future.cancel(false);
            future = null;
        }
    }

    private synchronized void scheduleNextRequest()
    {
        // stopped or done?
        ThriftTaskStatus taskStatus = getTaskStatus();
        if (!running || TaskState.isDone(taskStatus.getState())) {
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
        Request.Builder requestBuilder;
        ResponseHandler responseHandler;

        requestBuilder = prepareGet()
                .setHeader(CONTENT_TYPE, APPLICATION_THRIFT_BINARY)
                .setHeader(ACCEPT, APPLICATION_THRIFT_BINARY);
        responseHandler = new ExperimentalThriftResponseHandler(ThriftTaskStatus.class);

        Request request = requestBuilder.setUri(uriBuilderFrom(URI.create(taskStatus.getSelf())).appendPath("status").build())
                .setHeader(PRESTO_EXPERIMENTAL, String.valueOf(experimentalThriftEnabled))
                .build();

        errorTracker.startRequest();
        future = httpClient.executeAsync(request, responseHandler);
        currentRequestStartNanos.set(System.nanoTime());
        FutureCallback callback = new FutureCallback<ThriftTaskStatus>()
        {
            @Override
            public void onSuccess(ThriftTaskStatus value)
            {
                try (SetThreadName ignored = new SetThreadName("ContinuousThriftTaskStatusFetcher-%s", taskId)) {
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
            public void onFailure(Throwable cause)
            {
                try (SetThreadName ignored = new SetThreadName("ContinuousThriftTaskStatusFetcher-%s", taskId)) {
                    updateStats(currentRequestStartNanos.get());
                    try {
                        // if task not already done, record error
                        ThriftTaskStatus taskStatus = getTaskStatus();
                        if (!TaskState.isDone(taskStatus.getState())) {
                            errorTracker.requestFailed(cause);
                        }
                    }
                    catch (Error e) {
                        log.error(e);
                    }
                    finally {
                        scheduleNextRequest();
                    }
                }
            }
        };
        Futures.addCallback(
                future,
                callback,
                executor);
    }

    ThriftTaskStatus getTaskStatus()
    {
        return taskStatus.get();
    }

    void updateTaskStatus(ThriftTaskStatus newValue)
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

            if (TaskState.isDone(oldValue.getState())) {
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
            onFail.accept(new PrestoException(REMOTE_TASK_MISMATCH, format("%s (%s)", REMOTE_TASK_MISMATCH_ERROR, HostAddress.fromUri(URI.create(getTaskStatus().getSelf())))));
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
    public void addStateChangeListener(StateMachine.StateChangeListener<ThriftTaskStatus> stateChangeListener)
    {
        taskStatus.addStateChangeListener(stateChangeListener);
    }

    private void updateStats(long currentRequestStartNanos)
    {
        stats.statusRoundTripMillis(nanosSince(currentRequestStartNanos).toMillis());
    }
}
