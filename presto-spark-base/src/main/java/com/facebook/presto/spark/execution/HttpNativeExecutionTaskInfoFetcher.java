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
package com.facebook.presto.spark.execution;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.server.RequestErrorTracker;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.server.smile.BaseResponse;
import com.facebook.presto.spark.execution.http.PrestoSparkHttpTaskClient;
import com.facebook.presto.spi.PrestoException;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.spi.StandardErrorCode.NATIVE_EXECUTION_TASK_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
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
    private final ScheduledExecutorService errorRetryScheduledExecutor;
    private final AtomicReference<RuntimeException> lastException = new AtomicReference<>();
    private final Duration maxErrorDuration;

    @GuardedBy("this")
    private ScheduledFuture<?> scheduledFuture;

    public HttpNativeExecutionTaskInfoFetcher(
            ScheduledExecutorService updateScheduledExecutor,
            ScheduledExecutorService errorRetryScheduledExecutor,
            PrestoSparkHttpTaskClient workerClient,
            Executor executor,
            Duration infoFetchInterval,
            Duration maxErrorDuration)
    {
        this.workerClient = requireNonNull(workerClient, "workerClient is null");
        this.updateScheduledExecutor = requireNonNull(updateScheduledExecutor, "updateScheduledExecutor is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.infoFetchInterval = requireNonNull(infoFetchInterval, "infoFetchInterval is null");
        this.errorRetryScheduledExecutor = requireNonNull(errorRetryScheduledExecutor, "errorRetryScheduledExecutor is null");
        this.maxErrorDuration = requireNonNull(maxErrorDuration, "maxErrorDuration is null");
        this.errorTracker = new RequestErrorTracker(
                "NativeExecution",
                workerClient.getLocation(),
                NATIVE_EXECUTION_TASK_ERROR,
                TASK_ERROR_MESSAGE,
                maxErrorDuration,
                errorRetryScheduledExecutor,
                "getting taskInfo from native process");
    }

    public void start()
    {
        scheduledFuture = updateScheduledExecutor.scheduleWithFixedDelay(() ->
        {
            try {
                ListenableFuture<BaseResponse<TaskInfo>> taskInfoFuture = workerClient.getTaskInfo();
                Futures.addCallback(
                        taskInfoFuture,
                        new FutureCallback<BaseResponse<TaskInfo>>()
                        {
                            @Override
                            public void onSuccess(BaseResponse<TaskInfo> result)
                            {
                                log.debug("TaskInfoCallback success %s", result.getValue().getTaskId());
                                taskInfo.set(result.getValue());
                            }

                            @Override
                            public void onFailure(Throwable t)
                            {
                                // record failure
                                try {
                                    errorTracker.requestFailed(t);
                                }
                                catch (PrestoException e) {
                                    stop();
                                    lastException.set(e);
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
                        },
                        executor);
            }
            catch (Throwable t) {
                throw t;
            }
        }, 0, (long) infoFetchInterval.getValue(), infoFetchInterval.getUnit());
    }

    public CompletableFuture<TaskInfo> newStart()
    {
        CompletableFuture<TaskInfo> taskCompletionFuture = new CompletableFuture<>();
        scheduledFuture = updateScheduledExecutor.scheduleWithFixedDelay(() ->
        {
            try {
                ListenableFuture<BaseResponse<TaskInfo>> future = workerClient.getTaskInfo();

                BaseResponse<TaskInfo> result = future.get();

                taskInfo.set(result.getValue());

                if (!taskInfo.get().getTaskStatus().getState().isDone()) {
                    log.info("Task %s not done yet. taskState=%s", taskInfo.get().getTaskId(), taskInfo.get().getTaskStatus().getState());
                    return;
                }

                if (taskInfo.get().getTaskStatus().getState() != TaskState.FINISHED) {
                    // task failed with errors
                    RuntimeException failure = taskInfo.get().getTaskStatus().getFailures().stream()
                                   .findFirst()
                                   .map(ExecutionFailureInfo::toException)
                                   .orElse(new PrestoException(GENERIC_INTERNAL_ERROR, "Native task failed for an unknown reason"));
                    taskCompletionFuture.completeExceptionally(failure);
                    return;
                }

                taskCompletionFuture.complete(taskInfo.get());
            }
            catch (InterruptedException | ExecutionException ex) {
                // We don't complete future here because
                // we want to assume that task might still be running
                // and that we will get a successfull response on
                // next call.
                // If the task has really aborted/died, the error tracker will
                // catch that and propagate
                log.error("Error getting task Info..", ex);
            }
            catch (Throwable t) {
                throw t;
            }
        }, 0, (long) infoFetchInterval.getValue(), infoFetchInterval.getUnit());

        return taskCompletionFuture;
    }

    public void stop()
    {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
    }

    public Optional<TaskInfo> getTaskInfo()
            throws RuntimeException
    {
        if (scheduledFuture != null && scheduledFuture.isCancelled()) {
            throw lastException.get();
        }
        TaskInfo info = taskInfo.get();
        return info == null ? Optional.empty() : Optional.of(info);
    }
}
