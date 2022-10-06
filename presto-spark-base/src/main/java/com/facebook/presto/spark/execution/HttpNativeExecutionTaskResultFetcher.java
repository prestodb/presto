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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.SERIALIZED_PAGE_CHECKSUM_ERROR;
import static com.facebook.presto.spi.page.PagesSerdeUtil.isChecksumValid;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * This class helps to fetch results for a native task through HTTP communications with a Presto worker. The object of this class will give back a {@link CompletableFuture} to the
 * caller upon start(). This future will be completed when retrievals of all results by the fetcher is completed. Results are retrieved and stored in an internal buffer, which is
 * supposed to be polled by the caller. Note that the completion of the future does not mean all results have been consumed by the caller. The caller is responsible for making sure
 * all results be consumed after future completion.
 * There is a capacity cap (MAX_BUFFER_SIZE) for internal buffer managed by {@link HttpNativeExecutionTaskResultFetcher}. The fetcher will stop fetching results when buffer limit
 * is hit and resume fetching after some of the buffer has been consumed, bringing buffer size down below the limit.
 * <p>
 * The fetcher specifically serves to fetch table write commit metadata results from Presto worker so currently no shuffle result fetching is supported.
 */
public class HttpNativeExecutionTaskResultFetcher
{
    private static final Duration FETCH_INTERVAL = new Duration(200, TimeUnit.MILLISECONDS);
    private static final Duration POLL_TIMEOUT = new Duration(100, TimeUnit.MILLISECONDS);
    private static final Duration REQUEST_TIMEOUT = new Duration(200, TimeUnit.MILLISECONDS);
    private static final DataSize MAX_BUFFER_SIZE = new DataSize(128, DataSize.Unit.MEGABYTE);

    private final ScheduledExecutorService scheduler;
    private final PrestoSparkHttpWorkerClient workerClient;
    // Timeout for each fetching request
    private final Duration requestTimeout;
    private final TaskId taskId;
    private final LinkedBlockingDeque<SerializedPage> pageBuffer = new LinkedBlockingDeque<>();
    private final AtomicLong bufferMemoryBytes;

    private ScheduledFuture<?> schedulerFuture;
    private boolean started;

    public HttpNativeExecutionTaskResultFetcher(
            ScheduledExecutorService scheduler,
            PrestoSparkHttpWorkerClient workerClient,
            TaskId taskId,
            Optional<Duration> requestTimeout)
    {
        this.scheduler = requireNonNull(scheduler, "scheduler is null");
        this.workerClient = requireNonNull(workerClient, "workerClient is null");
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.requestTimeout = requestTimeout.orElse(REQUEST_TIMEOUT);
        this.bufferMemoryBytes = new AtomicLong();
    }

    public CompletableFuture<Void> start()
    {
        if (started) {
            throw new PrestoException(
                    GENERIC_INTERNAL_ERROR,
                    "trying to start an already started TaskResultFetcher for '" + taskId + "'");
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        schedulerFuture = scheduler.scheduleAtFixedRate(
                new HttpNativeExecutionTaskResultFetcherRunner(
                        workerClient,
                        future,
                        pageBuffer,
                        requestTimeout,
                        bufferMemoryBytes),
                0,
                (long) FETCH_INTERVAL.getValue(),
                FETCH_INTERVAL.getUnit());
        started = true;
        return future.handle(
                (Void result, Throwable throwable) ->
                {
                    schedulerFuture.cancel(false);
                    if (throwable != null) {
                        throw new CompletionException(throwable.getCause());
                    }
                    return result;
                });
    }

    public void stop()
    {
        if (schedulerFuture != null) {
            schedulerFuture.cancel(false);
        }
    }

    /**
     * Blocking call to poll from result buffer. Blocks until content becomes
     * available in the buffer, or until timeout is hit.
     *
     * @return the first {@link SerializedPage} result buffer contains.
     */
    public Optional<SerializedPage> pollPage()
            throws InterruptedException
    {
        SerializedPage page = pageBuffer.poll((long) POLL_TIMEOUT.getValue(), POLL_TIMEOUT.getUnit());
        if (page != null) {
            bufferMemoryBytes.addAndGet(-page.getSizeInBytes());
            return Optional.of(page);
        }
        return Optional.empty();
    }

    private static class HttpNativeExecutionTaskResultFetcherRunner
            implements Runnable
    {
        private static final DataSize MAX_RESPONSE_SIZE = new DataSize(32, DataSize.Unit.MEGABYTE);
        private static final int MAX_HTTP_TIMEOUT_RETRIES = 3;

        private final Duration requestTimeout;
        private final PrestoSparkHttpWorkerClient client;
        private final LinkedBlockingDeque<SerializedPage> pageBuffer;
        private final AtomicLong bufferMemoryBytes;
        private final CompletableFuture<Void> future;

        private int timeoutRetries;
        private long token;

        public HttpNativeExecutionTaskResultFetcherRunner(
                PrestoSparkHttpWorkerClient client,
                CompletableFuture<Void> future,
                LinkedBlockingDeque<SerializedPage> pageBuffer,
                Duration requestTimeout,
                AtomicLong bufferMemoryBytes)
        {
            this.timeoutRetries = 0;
            this.token = 0;
            this.client = requireNonNull(client, "client is null");
            this.future = requireNonNull(future, "future is null");
            this.pageBuffer = requireNonNull(pageBuffer, "pageBuffer is null");
            this.requestTimeout = requireNonNull(
                    requestTimeout,
                    "requestTimeout is null");
            this.bufferMemoryBytes = requireNonNull(
                    bufferMemoryBytes,
                    "bufferMemoryBytes is null");
        }

        @Override
        public void run()
        {
            try {
                if (bufferMemoryBytes.longValue() >= MAX_BUFFER_SIZE.toBytes()) {
                    return;
                }
                PageBufferClient.PagesResponse pagesResponse = client.getResults(
                        token,
                        MAX_RESPONSE_SIZE)
                        .get((long) requestTimeout.getValue(), requestTimeout.getUnit());

                List<SerializedPage> pages = pagesResponse.getPages();
                long bytes = 0;
                for (SerializedPage page : pages) {
                    if (!isChecksumValid(page)) {
                        throw new PrestoException(
                                SERIALIZED_PAGE_CHECKSUM_ERROR,
                                format("Received corrupted serialized page from host %s",
                                        HostAddress.fromUri(client.getLocation())));
                    }
                    bytes += page.getSizeInBytes();
                }
                pageBuffer.addAll(pages);
                bufferMemoryBytes.addAndGet(bytes);
                long nextToken = pagesResponse.getNextToken();
                if (pages.size() > 0) {
                    client.acknowledgeResultsAsync(nextToken);
                }
                this.token = nextToken;
                if (pagesResponse.isClientComplete()) {
                    client.abortResults();
                    future.complete(null);
                }
            }
            catch (InterruptedException e) {
                if (!future.isDone()) {
                    // This should never happen, but as a sanity check we set exception to future.
                    client.abortResults();
                    future.completeExceptionally(e);
                }
            }
            catch (ExecutionException | PrestoException e) {
                client.abortResults();
                future.completeExceptionally(e);
            }
            catch (TimeoutException e) {
                if (++timeoutRetries >= MAX_HTTP_TIMEOUT_RETRIES) {
                    client.abortResults();
                    future.completeExceptionally(e);
                }
            }
        }
    }
}
