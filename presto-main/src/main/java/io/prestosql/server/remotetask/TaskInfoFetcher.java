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
package io.prestosql.server.remotetask;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.prestosql.execution.StateMachine;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.TaskInfo;
import io.prestosql.execution.TaskStatus;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
    private final TaskId taskId;
    private final Consumer<Throwable> onFail;
    private final StateMachine<TaskInfo> taskInfo;
    private final StateMachine<Optional<TaskInfo>> finalTaskInfo;
    private final JsonCodec<TaskInfo> taskInfoCodec;

    private final long updateIntervalMillis;
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
    private ListenableFuture<FullJsonResponseHandler.JsonResponse<TaskInfo>> future;

    public TaskInfoFetcher(
            Consumer<Throwable> onFail,
            TaskInfo initialTask,
            HttpClient httpClient,
            Duration updateInterval,
            JsonCodec<TaskInfo> taskInfoCodec,
            Duration maxErrorDuration,
            boolean summarizeTaskInfo,
            Executor executor,
            ScheduledExecutorService updateScheduledExecutor,
            ScheduledExecutorService errorScheduledExecutor,
            RemoteTaskStats stats)
    {
        requireNonNull(initialTask, "initialTask is null");
        requireNonNull(errorScheduledExecutor, "errorScheduledExecutor is null");

        this.taskId = initialTask.getTaskStatus().getTaskId();
        this.onFail = requireNonNull(onFail, "onFail is null");
        this.taskInfo = new StateMachine<>("task " + taskId, executor, initialTask);
        this.finalTaskInfo = new StateMachine<>("task-" + taskId, executor, Optional.empty());
        this.taskInfoCodec = requireNonNull(taskInfoCodec, "taskInfoCodec is null");

        this.updateIntervalMillis = requireNonNull(updateInterval, "updateInterval is null").toMillis();
        this.updateScheduledExecutor = requireNonNull(updateScheduledExecutor, "updateScheduledExecutor is null");
        this.errorTracker = new RequestErrorTracker(taskId, initialTask.getTaskStatus().getSelf(), maxErrorDuration, errorScheduledExecutor, "getting info for task");

        this.summarizeTaskInfo = summarizeTaskInfo;

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

        // if throttled due to error, asynchronously wait for timeout and try again
        ListenableFuture<?> errorRateLimit = errorTracker.acquireRequestPermit();
        if (!errorRateLimit.isDone()) {
            errorRateLimit.addListener(this::sendNextRequest, executor);
            return;
        }

        HttpUriBuilder httpUriBuilder = uriBuilderFrom(taskStatus.getSelf());
        URI uri = summarizeTaskInfo ? httpUriBuilder.addParameter("summarize").build() : httpUriBuilder.build();
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
}
