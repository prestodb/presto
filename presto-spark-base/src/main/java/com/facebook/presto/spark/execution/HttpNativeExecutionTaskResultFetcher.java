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

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.operator.PageBufferClient;
import com.facebook.presto.spark.execution.http.PrestoSparkHttpWorkerClient;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.page.SerializedPage;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.SERIALIZED_PAGE_CHECKSUM_ERROR;
import static com.facebook.presto.spi.page.PagesSerdeUtil.isChecksumValid;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HttpNativeExecutionTaskResultFetcher {
    private static final Duration FETCH_INTERVAL = new Duration(
            200,
            TimeUnit.MILLISECONDS);

    private final ScheduledExecutorService scheduler;
    private final PrestoSparkHttpWorkerClient workerClient;
    // Timeout for each fetching request
    private final Duration requestTimeout;
    private final TaskId taskId;

    private ScheduledFuture<?> schedulerFuture;
    private boolean started;

    /**
     * The instance of this class helps to fetch results from remote Presto
     * worker. When start() is called, the fetcher polls the remote result
     * endpoint in a fixed interval until all results fetched or exceptions
     * occur. A CompletableFuture is returned for optional async handling.
     * Exceptions will be wrapped inside of the CompletableFuture in case any
     * occur.
     */
    public HttpNativeExecutionTaskResultFetcher(
            ScheduledExecutorService scheduler,
            PrestoSparkHttpWorkerClient workerClient,
            TaskId taskId,
            Duration requestTimeout) {
        this.scheduler = requireNonNull(scheduler, "scheduler is null");
        this.workerClient = requireNonNull(workerClient, "workerClient is null");
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.requestTimeout = requireNonNull(requestTimeout, "requestTimeout is null");
    }

    public CompletableFuture<List<SerializedPage>> start() {
        if (started) {
            throw new PrestoException(
                    GENERIC_INTERNAL_ERROR,
                    "trying to start an already started TaskResultFetcher for '" + taskId + "'");
        }
        CompletableFuture<List<SerializedPage>> future = new CompletableFuture<>();
        schedulerFuture = scheduler.scheduleAtFixedRate(
                new HttpNativeExecutionTaskResultFetcherRunner(
                        workerClient,
                        taskId,
                        future,
                        requestTimeout),
                0,
                (long) FETCH_INTERVAL.getValue(),
                FETCH_INTERVAL.getUnit());
        started = true;
        return future.handle(
                (List<SerializedPage> pages, Throwable throwable) -> {
                    schedulerFuture.cancel(false);
                    if (throwable != null) {
                        throw new CompletionException(throwable.getCause());
                    }
                    return pages;
                });
    }

    private static class HttpNativeExecutionTaskResultFetcherRunner
            implements Runnable {
        private static final DataSize MAX_RESPONSE_SIZE = new DataSize(32,
                DataSize.Unit.MEGABYTE);
        private static final int MAX_HTTP_TIMEOUT_RETRIES = 3;

        private final Duration requestTimeout;
        private final TaskId taskId;
        private final PrestoSparkHttpWorkerClient client;

        private int timeoutRetries;
        private long token;
        private CompletableFuture<List<SerializedPage>> future;
        private List<SerializedPage> serializedPages;

        public HttpNativeExecutionTaskResultFetcherRunner(
                PrestoSparkHttpWorkerClient client, TaskId taskId,
                CompletableFuture<List<SerializedPage>> future,
                Duration requestTimeout) {
            this.timeoutRetries = 0;
            this.token = 0;
            this.taskId = requireNonNull(taskId, "taskId is null");
            this.client = requireNonNull(client, "client is null");
            this.future = requireNonNull(future, "future is null");
            this.serializedPages = new ArrayList<>();
            this.requestTimeout = requireNonNull(requestTimeout,
                    "requestTimeout is null");
        }

        @Override
        public void run() {
            try {
                PageBufferClient.PagesResponse pagesResponse = client.getResults(
                        token,
                        MAX_RESPONSE_SIZE)
                        .get((long) requestTimeout.getValue(),
                                requestTimeout.getUnit());

                List<SerializedPage> pages = pagesResponse.getPages();
                for (SerializedPage page : pages) {
                    if (!isChecksumValid(page)) {
                        throw new PrestoException(
                                SERIALIZED_PAGE_CHECKSUM_ERROR,
                                format("Received corrupted serialized page from host %s",
                                        HostAddress.fromUri(
                                                client.getLocation())));
                    }
                }
                serializedPages.addAll(pages);
                long nextToken = pagesResponse.getNextToken();
                if (pages.size() > 0) {
                    client.acknowledgeResultsAsync(nextToken);
                }
                this.token = nextToken;
                if (pagesResponse.isClientComplete()) {
                    client.abortResults();
                    future.complete(serializedPages);
                }
            } catch (InterruptedException e) {
                if (!future.isDone()) {
                    // This should never happen, but as a sanity check we set exception to future.
                    client.abortResults();
                    future.completeExceptionally(e);
                }
            } catch (ExecutionException | PrestoException e) {
                client.abortResults();
                future.completeExceptionally(e);
            } catch (TimeoutException e) {
                if (++timeoutRetries >= MAX_HTTP_TIMEOUT_RETRIES) {
                    client.abortResults();
                    future.completeExceptionally(e);
                }
            }
        }
    }
}
