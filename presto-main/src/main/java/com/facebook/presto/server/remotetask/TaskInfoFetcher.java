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
import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.Response;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.json.Codec;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.smile.SmileCodec;
import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.MetadataUpdates;
import com.facebook.presto.server.RequestErrorTracker;
import com.facebook.presto.server.SimpleHttpResponseCallback;
import com.facebook.presto.server.SimpleHttpResponseHandler;
import com.facebook.presto.server.smile.BaseResponse;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.ResponseHandlerUtils.propagate;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CURRENT_STATE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_WAIT;
import static com.facebook.presto.server.RequestErrorTracker.taskRequestErrorTracker;
import static com.facebook.presto.server.RequestHelpers.setContentTypeHeaders;
import static com.facebook.presto.server.smile.AdaptingJsonResponseHandler.createAdaptingJsonResponseHandler;
import static com.facebook.presto.server.smile.FullSmileResponseHandler.createFullSmileResponseHandler;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TaskInfoFetcher
        implements SimpleHttpResponseCallback<TaskInfo>
{
    private final TaskId taskId;
    private final Consumer<Throwable> onFail;
    private final StateMachine<TaskInfo> taskInfo;
    private final StateMachine<Optional<TaskInfo>> finalTaskInfo;
    private final Codec<TaskInfo> taskInfoCodec;
    private final Codec<MetadataUpdates> metadataUpdatesCodec;

    private final long updateIntervalMillis;
    private final Duration taskInfoRefreshMaxWait;
    private final AtomicLong lastUpdateNanos = new AtomicLong();

    private final ScheduledExecutorService updateScheduledExecutor;

    private final Executor executor;
    private final HttpClient httpClient;
    private final RequestErrorTracker errorTracker;

    private final boolean summarizeTaskInfo;

    @GuardedBy("this")
    private final AtomicLong currentRequestStartNanos = new AtomicLong();

    private final RemoteTaskStats stats;

    @GuardedBy("this")
    private boolean running;

    @GuardedBy("this")
    private ScheduledFuture<?> scheduledFuture;

    @GuardedBy("this")
    private ListenableFuture<BaseResponse<TaskInfo>> future;

    @GuardedBy("this")
    private ListenableFuture<?> metadataUpdateFuture;

    private final boolean isBinaryTransportEnabled;
    private final Session session;
    private final MetadataManager metadataManager;
    private final QueryManager queryManager;

    public TaskInfoFetcher(
            Consumer<Throwable> onFail,
            TaskInfo initialTask,
            HttpClient httpClient,
            Duration updateInterval,
            Duration taskInfoRefreshMaxWait,
            Codec<TaskInfo> taskInfoCodec,
            Codec<MetadataUpdates> metadataUpdatesCodec,
            Duration maxErrorDuration,
            boolean summarizeTaskInfo,
            Executor executor,
            ScheduledExecutorService updateScheduledExecutor,
            ScheduledExecutorService errorScheduledExecutor,
            RemoteTaskStats stats,
            boolean isBinaryTransportEnabled,
            Session session,
            MetadataManager metadataManager,
            QueryManager queryManager)
    {
        requireNonNull(initialTask, "initialTask is null");
        requireNonNull(errorScheduledExecutor, "errorScheduledExecutor is null");

        this.taskId = initialTask.getTaskId();
        this.onFail = requireNonNull(onFail, "onFail is null");
        this.taskInfo = new StateMachine<>("task " + taskId, executor, initialTask);
        this.finalTaskInfo = new StateMachine<>("task-" + taskId, executor, Optional.empty());
        this.taskInfoCodec = requireNonNull(taskInfoCodec, "taskInfoCodec is null");

        this.metadataUpdatesCodec = requireNonNull(metadataUpdatesCodec, "metadataUpdatesCodec is null");

        this.updateIntervalMillis = requireNonNull(updateInterval, "updateInterval is null").toMillis();
        this.taskInfoRefreshMaxWait = requireNonNull(taskInfoRefreshMaxWait, "taskInfoRefreshMaxWait is null");
        this.updateScheduledExecutor = requireNonNull(updateScheduledExecutor, "updateScheduledExecutor is null");
        this.errorTracker = taskRequestErrorTracker(taskId, initialTask.getTaskStatus().getSelf(), maxErrorDuration, errorScheduledExecutor, "getting info for task");

        this.summarizeTaskInfo = summarizeTaskInfo;

        this.executor = requireNonNull(executor, "executor is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.isBinaryTransportEnabled = isBinaryTransportEnabled;
        this.session = requireNonNull(session, "session is null");
        this.metadataManager = requireNonNull(metadataManager, "metadataManager is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
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

    private synchronized void stop()
    {
        running = false;
        if (future != null) {
            // do not terminate if the request is already running to avoid closing pooled connections
            future.cancel(false);
            future = null;
        }
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
    }

    /**
     * Add a listener for the final task info.  This notification is guaranteed to be fired only once.
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    public void addFinalTaskInfoListener(StateChangeListener<TaskInfo> stateChangeListener)
    {
        AtomicBoolean done = new AtomicBoolean();
        StateChangeListener<Optional<TaskInfo>> fireOnceStateChangeListener = finalTaskInfo -> {
            if (finalTaskInfo.isPresent() && done.compareAndSet(false, true)) {
                stateChangeListener.stateChanged(finalTaskInfo.get());
            }
        };
        finalTaskInfo.addStateChangeListener(fireOnceStateChangeListener);
        fireOnceStateChangeListener.stateChanged(finalTaskInfo.get());
    }

    private synchronized void scheduleUpdate()
    {
        scheduledFuture = updateScheduledExecutor.scheduleWithFixedDelay(() -> {
            try {
                synchronized (this) {
                    // if the previous request still running, don't schedule a new request
                    if (future != null && !future.isDone()) {
                        return;
                    }
                }
                if (nanosSince(lastUpdateNanos.get()).toMillis() >= updateIntervalMillis) {
                    sendNextRequest();
                }
            }
            catch (Throwable t) {
                fatal(t);
                throw t;
            }
        }, 0, 100, MILLISECONDS);
    }

    private synchronized void sendNextRequest()
    {
        TaskInfo taskInfo = getTaskInfo();
        TaskStatus taskStatus = taskInfo.getTaskStatus();

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

        // if throttled due to error, asynchronously wait for timeout and try again
        ListenableFuture<?> errorRateLimit = errorTracker.acquireRequestPermit();
        if (!errorRateLimit.isDone()) {
            errorRateLimit.addListener(this::sendNextRequest, executor);
            return;
        }

        MetadataUpdates metadataUpdateRequests = taskInfo.getMetadataUpdates();
        if (!metadataUpdateRequests.getMetadataUpdates().isEmpty()) {
            scheduleMetadataUpdates(metadataUpdateRequests);
        }

        HttpUriBuilder httpUriBuilder = uriBuilderFrom(taskStatus.getSelf());
        URI uri = summarizeTaskInfo ? httpUriBuilder.addParameter("summarize").build() : httpUriBuilder.build();
        Request.Builder uriBuilder = setContentTypeHeaders(isBinaryTransportEnabled, prepareGet());

        if (taskInfoRefreshMaxWait.toMillis() != 0L) {
            uriBuilder.setHeader(PRESTO_CURRENT_STATE, taskStatus.getState().toString())
                    .setHeader(PRESTO_MAX_WAIT, taskInfoRefreshMaxWait.toString());
        }

        Request request = uriBuilder.setUri(uri).build();

        ResponseHandler responseHandler;
        if (isBinaryTransportEnabled) {
            responseHandler = createFullSmileResponseHandler((SmileCodec<TaskInfo>) taskInfoCodec);
        }
        else {
            responseHandler = createAdaptingJsonResponseHandler((JsonCodec<TaskInfo>) taskInfoCodec);
        }

        errorTracker.startRequest();
        future = httpClient.executeAsync(request, responseHandler);
        currentRequestStartNanos.set(System.nanoTime());
        Futures.addCallback(future, new SimpleHttpResponseHandler<>(this, request.getUri(), stats.getHttpResponseStats(), REMOTE_TASK_ERROR), executor);
    }

    synchronized void updateTaskInfo(TaskInfo newValue)
    {
        boolean updated = taskInfo.setIf(newValue, oldValue -> {
            TaskStatus oldTaskStatus = oldValue.getTaskStatus();
            TaskStatus newTaskStatus = newValue.getTaskStatus();
            if (oldTaskStatus.getState().isDone()) {
                // never update if the task has reached a terminal state
                return false;
            }
            // don't update to an older version (same version is ok)
            return newTaskStatus.getVersion() >= oldTaskStatus.getVersion();
        });

        if (updated && newValue.getTaskStatus().getState().isDone()) {
            finalTaskInfo.compareAndSet(Optional.empty(), Optional.of(newValue));
            stop();
        }
    }

    @Override
    public void success(TaskInfo newValue)
    {
        try (SetThreadName ignored = new SetThreadName("TaskInfoFetcher-%s", taskId)) {
            lastUpdateNanos.set(System.nanoTime());

            long startNanos;
            synchronized (this) {
                startNanos = this.currentRequestStartNanos.get();
            }
            updateStats(startNanos);
            errorTracker.requestSucceeded();
            updateTaskInfo(newValue);
        }
    }

    @Override
    public void failed(Throwable cause)
    {
        try (SetThreadName ignored = new SetThreadName("TaskInfoFetcher-%s", taskId)) {
            lastUpdateNanos.set(System.nanoTime());

            try {
                // if task not already done, record error
                if (!isDone(getTaskInfo())) {
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
        }
    }

    @Override
    public void fatal(Throwable cause)
    {
        try (SetThreadName ignored = new SetThreadName("TaskInfoFetcher-%s", taskId)) {
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

    private void scheduleMetadataUpdates(MetadataUpdates metadataUpdateRequests)
    {
        MetadataUpdates results = metadataManager.getMetadataUpdateResults(session, queryManager, metadataUpdateRequests, taskId.getQueryId());
        executor.execute(() -> sendMetadataUpdates(results));
    }

    private synchronized void sendMetadataUpdates(MetadataUpdates results)
    {
        TaskStatus taskStatus = getTaskInfo().getTaskStatus();

        // we already have the final task info
        if (isDone(getTaskInfo())) {
            stop();
            return;
        }

        // outstanding request?
        if (metadataUpdateFuture != null && !metadataUpdateFuture.isDone()) {
            // this should never happen
            return;
        }

        byte[] metadataUpdatesJson = metadataUpdatesCodec.toBytes(results);
        Request request = setContentTypeHeaders(isBinaryTransportEnabled, preparePost())
                .setUri(uriBuilderFrom(taskStatus.getSelf()).appendPath("metadataresults").build())
                .setBodyGenerator(createStaticBodyGenerator(metadataUpdatesJson))
                .build();

        errorTracker.startRequest();
        metadataUpdateFuture = httpClient.executeAsync(request, new ResponseHandler<Response, RuntimeException>()
        {
            @Override
            public Response handleException(Request request, Exception exception)
            {
                throw propagate(request, exception);
            }

            @Override
            public Response handle(Request request, Response response)
            {
                return response;
            }
        });
        currentRequestStartNanos.set(System.nanoTime());
    }
}
