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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.server.smile.BaseResponse;
import com.facebook.presto.spark.execution.http.PrestoSparkHttpWorkerClient;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static java.util.Objects.requireNonNull;

/**
 * NativeExecutionTask provide the abstraction of executing tasks via c++ worker. It is used for native execution of Presto on Spark. The plan and splits is provided at creation
 * of NativeExecutionTask, it doesn't support adding more splits during execution.
 * <p>
 * Caller should manage the lifecycle of the task by exposed APIs. The general workflow will look like:
 * 1. Caller shall start() to start the task. The task finish signal can be told by the returned future.
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
    private final PrestoSparkHttpWorkerClient workerClient;
    private final TableWriteInfo tableWriteInfo;
    private final List<TaskSource> sources;
    private final Executor executor;
    private final HttpNativeExecutionTaskInfoFetcher taskInfoFetcher;
    private final HttpNativeExecutionTaskResultFetcher taskResultFetcher;

    public NativeExecutionTask(
            Session session,
            URI location,
            TaskId taskId,
            PlanFragment planFragment,
            List<TaskSource> sources,
            HttpClient httpClient,
            TableWriteInfo tableWriteInfo,
            Executor executor,
            ScheduledExecutorService updateScheduledExecutor,
            JsonCodec<TaskInfo> taskInfoCodec,
            JsonCodec<PlanFragment> planFragmentCodec,
            JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec)
    {
        this.session = requireNonNull(session, "session is null");
        this.planFragment = requireNonNull(planFragment, "planFragment is null");
        this.tableWriteInfo = requireNonNull(tableWriteInfo, "tableWriteInfo is null");
        this.sources = requireNonNull(sources, "sources is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.outputBuffers = createInitialEmptyOutputBuffers(PARTITIONED);
        this.workerClient = new PrestoSparkHttpWorkerClient(
                requireNonNull(httpClient, "httpClient is null"),
                taskId,
                location,
                taskInfoCodec,
                planFragmentCodec,
                taskUpdateRequestCodec);
        requireNonNull(updateScheduledExecutor, "updateScheduledExecutor is null");
        this.taskInfoFetcher = new HttpNativeExecutionTaskInfoFetcher(
                updateScheduledExecutor,
                this.workerClient,
                this.executor);
        this.taskResultFetcher = new HttpNativeExecutionTaskResultFetcher(
                updateScheduledExecutor,
                this.workerClient,
                Optional.empty());
    }

    /**
     * Gets the most updated {@link TaskInfo} of the task of the native task.
     *
     * @return an {@link Optional} of most updated {@link TaskInfo}, empty {@link Optional} if {@link HttpNativeExecutionTaskInfoFetcher} has not yet retrieved the very first
     * TaskInfo.
     */
    public Optional<TaskInfo> getTaskInfo()
    {
        return taskInfoFetcher.getTaskInfo();
    }

    /**
     * Blocking call to poll from result fetcher buffer. Blocks until content becomes available in the buffer, or until timeout is hit.
     *
     * @return an Optional of the first {@link SerializedPage} result fetcher buffer contains, an empty Optional if no result is in the buffer.
     */
    public Optional<SerializedPage> pollResult()
            throws InterruptedException
    {
        return taskResultFetcher.pollPage();
    }

    /**
     * Starts the execution of the NativeExecutionTask. Any exceptional cases should be captured by the exception handling mechanism of CompletableFuture.
     *
     * @return a CompletableFuture of no content to indicate the successful finish of the task.
     */
    public CompletableFuture<Void> start()
    {
        CompletableFuture<Void> updateFuture = sendUpdateRequest().handle((Void result, Throwable t) ->
        {
            if (t != null) {
                throw new CompletionException(t.getCause());
            }
            taskInfoFetcher.start();
            return null;
        });

        return updateFuture.thenCombine(taskResultFetcher.start(), (r1, r2) -> null);
    }

    /**
     * Releases all resources, and kills all schedulers. It is caller's responsibility to call this method when NativeExecutionTask is no longer needed.
     */
    public void stop()
    {
        taskInfoFetcher.stop();
        taskResultFetcher.stop();
    }

    private CompletableFuture<Void> sendUpdateRequest()
    {
        CompletableFuture<Void> future = new CompletableFuture<>();
        Futures.addCallback(
                workerClient.updateTask(
                        sources,
                        planFragment,
                        tableWriteInfo,
                        session,
                        outputBuffers),
                new UpdateResponseHandler(future),
                executor);
        return future;
    }

    private class UpdateResponseHandler
            implements FutureCallback<BaseResponse<TaskInfo>>
    {
        private final CompletableFuture<Void> future;

        public UpdateResponseHandler(CompletableFuture<Void> future)
        {
            this.future = requireNonNull(future, "future is null");
        }

        @Override
        public void onSuccess(BaseResponse<TaskInfo> result)
        {
            TaskInfo value = result.getValue();
            log.debug("success %s", value.getTaskId());
            future.complete(null);
        }

        @Override
        public void onFailure(Throwable t)
        {
            log.error("failed %s", t);
            future.completeExceptionally(t);
        }
    }
}
