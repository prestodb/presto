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
import com.facebook.presto.spark.execution.http.PrestoSparkHttpTaskClient;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Throwables.throwIfUnchecked;
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

    private final PrestoSparkHttpTaskClient workerClient;
    private final ScheduledExecutorService updateScheduledExecutor;
    private final AtomicReference<TaskInfo> taskInfo = new AtomicReference<>();
    private final Duration infoFetchInterval;
    private final AtomicReference<Throwable> lastException = new AtomicReference<>();
    private final Object taskFinished;

    @GuardedBy("this")
    private ScheduledFuture<?> scheduledFuture;

    public HttpNativeExecutionTaskInfoFetcher(
            ScheduledExecutorService updateScheduledExecutor,
            PrestoSparkHttpTaskClient workerClient,
            Duration infoFetchInterval,
            Object taskFinished)
    {
        this.workerClient = requireNonNull(workerClient, "workerClient is null");
        this.updateScheduledExecutor = requireNonNull(updateScheduledExecutor, "updateScheduledExecutor is null");
        this.infoFetchInterval = requireNonNull(infoFetchInterval, "infoFetchInterval is null");
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
            TaskInfo result = workerClient.getTaskInfo();
            onSuccess(result);
        }
        catch (Throwable t) {
            onFailure(t);
        }
    }

    private void onSuccess(TaskInfo result)
    {
        log.debug("TaskInfoCallback success %s", result.getTaskId());
        taskInfo.set(result);
        if (result.getTaskStatus().getState().isDone()) {
            synchronized (taskFinished) {
                taskFinished.notifyAll();
            }
        }
    }

    private void onFailure(Throwable failure)
    {
        stop();
        lastException.set(failure);
        synchronized (taskFinished) {
            taskFinished.notifyAll();
        }
    }

    public Optional<TaskInfo> getTaskInfo()
            throws RuntimeException
    {
        if (scheduledFuture != null && scheduledFuture.isCancelled() && lastException.get() != null) {
            Throwable failure = lastException.get();
            throwIfUnchecked(failure);
            throw new RuntimeException(failure);
        }
        TaskInfo info = taskInfo.get();
        return info == null ? Optional.empty() : Optional.of(info);
    }

    public AtomicReference<Throwable> getLastException()
    {
        return lastException;
    }
}
