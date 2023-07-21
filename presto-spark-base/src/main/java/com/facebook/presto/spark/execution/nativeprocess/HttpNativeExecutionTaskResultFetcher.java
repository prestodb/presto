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
import com.facebook.presto.operator.PageBufferClient;
import com.facebook.presto.server.RequestErrorTracker;
import com.facebook.presto.spark.execution.http.PrestoSparkHttpTaskClient;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NATIVE_EXECUTION_TASK_ERROR;
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
    private static final Logger log = Logger.get(HttpNativeExecutionTaskResultFetcher.class);
    private static final Duration FETCH_INTERVAL = new Duration(200, TimeUnit.MILLISECONDS);
    private static final Duration POLL_TIMEOUT = new Duration(100, TimeUnit.MILLISECONDS);
    private static final DataSize MAX_RESPONSE_SIZE = new DataSize(32, DataSize.Unit.MEGABYTE);
    private static final DataSize MAX_BUFFER_SIZE = new DataSize(128, DataSize.Unit.MEGABYTE);
    private static final String TASK_ERROR_MESSAGE = "TaskResultsFetcher encountered too many errors talking to native process.";

    private final Executor executor;
    private final ScheduledExecutorService scheduler;
    private final PrestoSparkHttpTaskClient workerClient;
    private final LinkedBlockingDeque<SerializedPage> pageBuffer = new LinkedBlockingDeque<>();
    private final AtomicLong bufferMemoryBytes;
    private final Object taskHasResult;
    private final Duration maxErrorDuration;
    private final RequestErrorTracker errorTracker;
    private final AtomicReference<RuntimeException> lastException = new AtomicReference<>();

    private ScheduledFuture<?> scheduledFuture;

    private long token;

    public HttpNativeExecutionTaskResultFetcher(
            ScheduledExecutorService scheduler,
            ScheduledExecutorService errorRetryScheduledExecutor,
            PrestoSparkHttpTaskClient workerClient,
            Executor executor,
            Duration maxErrorDuration,
            Object taskHasResult)
    {
        this.executor = requireNonNull(executor, "executor is null");
        this.scheduler = requireNonNull(scheduler, "scheduler is null");
        this.workerClient = requireNonNull(workerClient, "workerClient is null");
        this.bufferMemoryBytes = new AtomicLong();
        this.taskHasResult = requireNonNull(taskHasResult, "taskHasResult is null");
        this.maxErrorDuration = requireNonNull(maxErrorDuration, "maxErrorDuration is null");
        this.errorTracker = new RequestErrorTracker(
                "NativeExecution",
                workerClient.getLocation(),
                NATIVE_EXECUTION_TASK_ERROR,
                TASK_ERROR_MESSAGE,
                maxErrorDuration,
                requireNonNull(errorRetryScheduledExecutor, "errorRetryScheduledExecutor is null"),
                "getting results from native process");
    }

    public void start()
    {
        scheduledFuture = scheduler.scheduleAtFixedRate(this::doGetResults,
                0,
                (long) FETCH_INTERVAL.getValue(),
                FETCH_INTERVAL.getUnit());
    }

    public void stop(boolean success)
    {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }

        if (success && !pageBuffer.isEmpty()) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("TaskResultFetcher is closed with %s pages left in the buffer", pageBuffer.size()));
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
        if (scheduledFuture != null && scheduledFuture.isCancelled() && lastException.get() != null) {
            throw lastException.get();
        }

        SerializedPage page = pageBuffer.poll((long) POLL_TIMEOUT.getValue(), POLL_TIMEOUT.getUnit());
        if (page != null) {
            bufferMemoryBytes.addAndGet(-page.getSizeInBytes());
            return Optional.of(page);
        }
        return Optional.empty();
    }

    public boolean hasPage()
    {
        if (scheduledFuture != null && scheduledFuture.isCancelled() && lastException.get() != null) {
            throw lastException.get();
        }

        return !pageBuffer.isEmpty();
    }

    private void doGetResults()
    {
        if (bufferMemoryBytes.longValue() >= MAX_BUFFER_SIZE.toBytes()) {
            return;
        }

        try {
            PageBufferClient.PagesResponse pagesResponse = workerClient.getResults(token, MAX_RESPONSE_SIZE).get();
            onSuccess(pagesResponse);
        }
        catch (Throwable t) {
            onFailure(t);
        }
    }

    private void onSuccess(PageBufferClient.PagesResponse pagesResponse)
    {
        errorTracker.requestSucceeded();

        List<SerializedPage> pages = pagesResponse.getPages();
        long bytes = 0;
        long positionCount = 0;
        for (SerializedPage page : pages) {
            if (!isChecksumValid(page)) {
                throw new PrestoException(
                        SERIALIZED_PAGE_CHECKSUM_ERROR,
                        format("Received corrupted serialized page from host %s",
                                HostAddress.fromUri(workerClient.getLocation())));
            }
            bytes += page.getSizeInBytes();
            positionCount += page.getPositionCount();
        }
        log.info("Received %s rows in %s pages from %s", positionCount, pages.size(), workerClient.getTaskUri());

        pageBuffer.addAll(pages);
        bufferMemoryBytes.addAndGet(bytes);
        long nextToken = pagesResponse.getNextToken();
        if (pages.size() > 0) {
            workerClient.acknowledgeResultsAsync(nextToken);
        }
        token = nextToken;
        if (pagesResponse.isClientComplete()) {
            workerClient.abortResults();
            scheduledFuture.cancel(false);
        }
        if (!pages.isEmpty()) {
            synchronized (taskHasResult) {
                taskHasResult.notifyAll();
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
            // Entering here means that we are unable to get any results from the CPP process
            // likely because process has crashed.
            workerClient.abortResults();
            stop(false);
            lastException.set(e);
            synchronized (taskHasResult) {
                taskHasResult.notifyAll();
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
}
