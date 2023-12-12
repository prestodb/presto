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
package com.facebook.presto.spark.execution.task;

import com.facebook.airlift.http.client.HttpStatus;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.server.RequestErrorTracker;
import com.facebook.presto.server.smile.BaseResponse;
import com.facebook.presto.spark.execution.http.PrestoSparkHttpTaskClient;
import com.facebook.presto.spark.execution.nativeprocess.HttpNativeExecutionTaskInfoFetcher;
import com.facebook.presto.spark.execution.nativeprocess.HttpNativeExecutionTaskResultFetcher;
import com.facebook.presto.spi.PrestoTransportException;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.spi.security.TokenAuthenticator;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.execution.TaskState.ABORTED;
import static com.facebook.presto.execution.TaskState.CANCELED;
import static com.facebook.presto.execution.TaskState.FAILED;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.spi.StandardErrorCode.NATIVE_EXECUTION_TASK_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * NativeExecutionTask provide the abstraction of executing tasks via c++ worker.
 * It is used for native execution of Presto on Spark. The plan and splits is provided at creation
 * of NativeExecutionTask, it doesn't support adding more splits during execution.
 * <p>
 * Caller should manage the lifecycle of the task by exposed APIs. The general workflow will look like:
 * 1. Caller shall start() to start the task.
 * 2. Caller shall call getTaskInfo() any time to get the current task info. Until the caller calls stop(), the task info fetcher will not stop fetching task info.
 * 3. Caller shall call pollResult() continuously to poll result page from the internal buffer. The result fetcher will stop fetching more results if buffer hits its memory cap,
 * until pages are fetched by caller to reduce the buffer under its memory cap.
 * 4. Caller must call stop() to release resource.
 */
public class NativeExecutionTask
{
    private static final Logger log = Logger.get(NativeExecutionTask.class);

    private final Session session;
    private final PlanFragment planFragment;
    private final OutputBuffers outputBuffers;
    private final PrestoSparkHttpTaskClient workerClient;
    private final TableWriteInfo tableWriteInfo;
    private final Optional<String> shuffleWriteInfo;
    private final Optional<String> broadcastBasePath;
    private final List<TaskSource> sources;
    private final Executor executor;

    private final ScheduledExecutorService errorRetryScheduledExecutor;
    private final Duration remoteTaskMaxErrorDuration;
    private final HttpNativeExecutionTaskInfoFetcher taskInfoFetcher;
    // Results will be fetched only if not written to shuffle.
    private final Optional<HttpNativeExecutionTaskResultFetcher> taskResultFetcher;
    private final Object taskFinishedOrHasResult = new Object();
    private Optional<TokenAuthenticator> tokenAuthenticator;

    public NativeExecutionTask(
            Session session,
            PrestoSparkHttpTaskClient workerClient,
            PlanFragment planFragment,
            List<TaskSource> sources,
            TableWriteInfo tableWriteInfo,
            Optional<String> shuffleWriteInfo,
            Optional<String> broadcastBasePath,
            Executor executor,
            ScheduledExecutorService updateScheduledExecutor,
            ScheduledExecutorService errorRetryScheduledExecutor,
            TaskManagerConfig taskManagerConfig,
            QueryManagerConfig queryManagerConfig)
    {
        this.session = requireNonNull(session, "session is null");
        this.planFragment = requireNonNull(planFragment, "planFragment is null");
        this.tableWriteInfo = requireNonNull(tableWriteInfo, "tableWriteInfo is null");
        this.shuffleWriteInfo = requireNonNull(shuffleWriteInfo, "shuffleWriteInfo is null");
        this.broadcastBasePath = requireNonNull(broadcastBasePath, "broadcastBasePath is null");
        this.sources = requireNonNull(sources, "sources is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.errorRetryScheduledExecutor = requireNonNull(errorRetryScheduledExecutor, "errorRetryScheduledExecutor is null");
        this.workerClient = requireNonNull(workerClient, "workerClient is null");
        this.outputBuffers = createInitialEmptyOutputBuffers(planFragment.getPartitioningScheme().getPartitioning().getHandle()).withNoMoreBufferIds();
        requireNonNull(taskManagerConfig, "taskManagerConfig is null");
        requireNonNull(updateScheduledExecutor, "updateScheduledExecutor is null");
        requireNonNull(errorRetryScheduledExecutor, "errorRetryScheduledExecutor is null");
        this.remoteTaskMaxErrorDuration = queryManagerConfig.getRemoteTaskMaxErrorDuration();
        this.taskInfoFetcher = new HttpNativeExecutionTaskInfoFetcher(
                updateScheduledExecutor,
                errorRetryScheduledExecutor,
                this.workerClient,
                this.executor,
                taskManagerConfig.getInfoUpdateInterval(),
                remoteTaskMaxErrorDuration,
                taskFinishedOrHasResult);
        if (!shuffleWriteInfo.isPresent()) {
            this.taskResultFetcher = Optional.of(new HttpNativeExecutionTaskResultFetcher(
                    updateScheduledExecutor,
                    errorRetryScheduledExecutor,
                    this.workerClient,
                    this.executor,
                    remoteTaskMaxErrorDuration,
                    taskFinishedOrHasResult));
        }
        else {
            this.taskResultFetcher = Optional.empty();
        }
    }

    /**
     * Gets the most updated {@link TaskInfo} of the task of the native task.
     *
     * @return an {@link Optional} of most updated {@link TaskInfo}, empty {@link Optional} if {@link HttpNativeExecutionTaskInfoFetcher} has not yet retrieved the very first
     * TaskInfo.
     */
    public Optional<TaskInfo> getTaskInfo()
            throws RuntimeException
    {
        return taskInfoFetcher.getTaskInfo();
    }

    public boolean isTaskDone()
    {
        Optional<TaskInfo> taskInfo = getTaskInfo();
        return taskInfo.isPresent() && taskInfo.get().getTaskStatus().getState().isDone();
    }

    public Object getTaskFinishedOrHasResult()
    {
        return taskFinishedOrHasResult;
    }

    /**
     * Blocking call to poll from result fetcher buffer. Blocks until content becomes available in the buffer, or until timeout is hit.
     *
     * @return an Optional of the first {@link SerializedPage} result fetcher buffer contains, an empty Optional if no result is in the buffer.
     */
    public Optional<SerializedPage> pollResult()
            throws InterruptedException
    {
        if (!taskResultFetcher.isPresent()) {
            return Optional.empty();
        }
        return taskResultFetcher.get().pollPage();
    }

    public boolean hasResult()
    {
        return taskResultFetcher.isPresent() && taskResultFetcher.get().hasPage();
    }

    /**
     * Blocking call to create and start native task.
     * <p>
     * Starts background threads to fetch results and updated info.
     */
    public TaskInfo start()
    {
        TaskInfo taskInfo = sendUpdateRequest();

        // We do not start taskInfo fetcher for failed tasks
        if (!ImmutableList.of(CANCELED, FAILED, ABORTED).contains(taskInfo.getTaskStatus().getState())) {
            log.info("Starting TaskInfoFetcher and TaskResultFetcher.");
            taskResultFetcher.ifPresent(fetcher -> fetcher.start());
            taskInfoFetcher.start();
        }

        return taskInfo;
    }

    /**
     * Releases all resources, and kills all schedulers. It is caller's responsibility to call this method when NativeExecutionTask is no longer needed.
     */
    public void stop(boolean success)
    {
        taskInfoFetcher.stop();
        taskResultFetcher.ifPresent(fetcher -> fetcher.stop(success));
        workerClient.abortResults();
    }

    private TaskInfo sendUpdateRequest()
    {
        RequestErrorTracker errorTracker = new RequestErrorTracker(
                "NativeExecution",
                workerClient.getLocation(),
                NATIVE_EXECUTION_TASK_ERROR,
                "sendUpdateRequest encountered too many errors talking to native process",
                remoteTaskMaxErrorDuration,
                errorRetryScheduledExecutor,
                "sending update request to native process");

        while (true) {
            getFutureValue(errorTracker.acquireRequestPermit());
            try {
                errorTracker.startRequest();
                BaseResponse<TaskInfo> response = doSendUpdateRequest();
                if (response.hasValue()) {
                    errorTracker.requestSucceeded();
                    return response.getValue();
                }
                else {
                    String message = String.format("Create-or-update task request didn't return a result. %s: %s",
                            HttpStatus.fromStatusCode(response.getStatusCode()),
                            response.getStatusMessage());
                    throw new IllegalStateException(message);
                }
            }
            catch (RuntimeException e) {
                errorTracker.requestFailed(e);
            }
        }
    }

    private BaseResponse<TaskInfo> doSendUpdateRequest()
    {
        return workerClient.updateTask(
                sources,
                planFragment,
                tableWriteInfo,
                shuffleWriteInfo,
                broadcastBasePath,
                session,
                outputBuffers);
    }

    public static boolean isNativeExecutionTaskError(RuntimeException ex)
    {
        return ex instanceof PrestoTransportException;
    }
}
