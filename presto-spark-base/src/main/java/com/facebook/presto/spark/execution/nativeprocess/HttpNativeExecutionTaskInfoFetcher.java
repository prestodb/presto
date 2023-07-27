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
package com.facebook.presto.spark.execution.nativeprocess;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.server.RequestErrorTracker;
import com.facebook.presto.server.smile.BaseResponse;
import com.facebook.presto.spark.execution.http.PrestoSparkHttpTaskClient;
import com.facebook.presto.spi.PrestoException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.spi.StandardErrorCode.NATIVE_EXECUTION_TASK_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * This class helps to fetch {@link TaskInfo} for a native task through HTTP communications with a Presto worker. Upon calling start(), object of this class will continuously poll
 * {@link TaskInfo} from Presto worker and update its internal {@link TaskInfo} buffer. Caller is responsible for retrieving updated {@link TaskInfo} by calling getTaskInfo()
 * method.
 * Caller is also responsible for calling stop() to release resource when this fetcher is no longer needed.
 */
public class HttpNativeExecutionTaskInfoFetcher
{
    private static final Logger log = Logger.get(HttpNativeExecutionTaskInfoFetcher.class);
    private static final String TASK_ERROR_MESSAGE = "TaskInfoFetcher encountered too many errors talking to native process.";

    private final PrestoSparkHttpTaskClient workerClient;
    private final ScheduledExecutorService updateScheduledExecutor;
    private final AtomicReference<TaskInfo> taskInfo = new AtomicReference<>();
    private final Executor executor;
    private final Duration infoFetchInterval;
    private final RequestErrorTracker errorTracker;
    private final AtomicReference<RuntimeException> lastException = new AtomicReference<>();
    private final Duration maxErrorDuration;
    private final Object taskFinished;

    @GuardedBy("this")
    private ScheduledFuture<?> scheduledFuture;

    public HttpNativeExecutionTaskInfoFetcher(
            ScheduledExecutorService updateScheduledExecutor,
            ScheduledExecutorService errorRetryScheduledExecutor,
            PrestoSparkHttpTaskClient workerClient,
            Executor executor,
            Duration infoFetchInterval,
            Duration maxErrorDuration,
            Object taskFinished)
    {
        this.workerClient = requireNonNull(workerClient, "workerClient is null");
        this.updateScheduledExecutor = requireNonNull(updateScheduledExecutor, "updateScheduledExecutor is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.infoFetchInterval = requireNonNull(infoFetchInterval, "infoFetchInterval is null");
        this.maxErrorDuration = requireNonNull(maxErrorDuration, "maxErrorDuration is null");
        this.errorTracker = new RequestErrorTracker(
                "NativeExecution",
                workerClient.getLocation(),
                NATIVE_EXECUTION_TASK_ERROR,
                TASK_ERROR_MESSAGE,
                maxErrorDuration,
                requireNonNull(errorRetryScheduledExecutor, "errorRetryScheduledExecutor is null"),
                "getting taskInfo from native process");
        this.taskFinished = requireNonNull(taskFinished, "taskFinished is null");
    }

    public void start()
    {
        scheduledFuture = updateScheduledExecutor.scheduleWithFixedDelay(
                this::doGetTaskInfo, 0, (long) infoFetchInterval.getValue(), infoFetchInterval.getUnit());
    }

    public void stop()
    {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
    }

    @VisibleForTesting
    void doGetTaskInfo()
    {
        try {
            BaseResponse<TaskInfo> response = workerClient.getTaskInfo().get();
            onSuccess(response);
        }
        catch (Throwable t) {
            onFailure(t);
        }
    }

    private void onSuccess(BaseResponse<TaskInfo> result)
    {
        log.debug("TaskInfoCallback success %s", result.getValue().getTaskId());
        taskInfo.set(result.getValue());

        errorTracker.requestSucceeded();

        if (result.getValue().getTaskStatus().getState().isDone()) {
            synchronized (taskFinished) {
                taskFinished.notifyAll();
            }
        }
    }

    private void onFailure(Throwable t)
    {
        // record failure
        try {
            errorTracker.requestFailed(t);
        }
        catch (PrestoException e) {
            // Entering here means that we are unable
            // to get any task info from the CPP process
            // likely because process has crashed
            stop();
            lastException.set(e);
            synchronized (taskFinished) {
                taskFinished.notifyAll();
            }
            return;
        }
        ListenableFuture<?> errorRateLimit = errorTracker.acquireRequestPermit();
        try {
            // synchronously wait on throttling
            errorRateLimit.get(maxErrorDuration.toMillis(), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e) {
            // throttling error is not fatal, just log the error.
            log.debug(e.getMessage());
        }
    }

    public Optional<TaskInfo> getTaskInfo()
            throws RuntimeException
    {
        if (scheduledFuture != null && scheduledFuture.isCancelled() && lastException.get() != null) {
            throw lastException.get();
        }
        TaskInfo info = taskInfo.get();
        return info == null ? Optional.empty() : Optional.of(info);
    }

    public AtomicReference<RuntimeException> getLastException()
    {
        return lastException;
    }
}
