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
package com.facebook.presto.operator;

import com.facebook.presto.execution.buffer.BufferSummary;
import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.server.remotetask.Backoff;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.net.MediaType;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClient.HttpResponseFuture;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.ResponseTooLargeException;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.SliceInput;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.PrestoMediaTypes.PRESTO_PAGES_TYPE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_BUFFER_COMPLETE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_SIZE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_NEXT_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TASK_INSTANCE_ID;
import static com.facebook.presto.execution.buffer.PageCompression.COMPRESSED;
import static com.facebook.presto.execution.buffer.PagesSerdeUtil.readSerializedPages;
import static com.facebook.presto.operator.HttpPageBufferClient.PagesResponse.createEmptyPagesResponse;
import static com.facebook.presto.operator.HttpPageBufferClient.PagesResponse.createPagesResponse;
import static com.facebook.presto.server.TaskResource.PATH_BUFFER_DATA;
import static com.facebook.presto.server.TaskResource.PATH_BUFFER_SUMMARY;
import static com.facebook.presto.spi.HostAddress.fromUri;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_BUFFER_CLOSE_FAILED;
import static com.facebook.presto.util.Failures.WORKER_NODE_ERROR;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.ResponseHandlerUtils.propagate;
import static io.airlift.http.client.StatusResponseHandler.StatusResponse;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@ThreadSafe
public final class HttpPageBufferClient
        implements Closeable
{
    private static final Logger log = Logger.get(HttpPageBufferClient.class);

    /**
     * For each request, the addPage method will be called zero or more times,
     * followed by either requestComplete or clientFinished (if buffer complete).  If the client is
     * closed, requestComplete or bufferFinished may never be called.
     * <p/>
     * <b>NOTE:</b> Implementations of this interface are not allowed to perform
     * blocking operations.
     */
    public interface ClientCallback
    {
        boolean addPages(HttpPageBufferClient client, List<SerializedPage> pages);

        void requestComplete(HttpPageBufferClient client);

        void clientFinished(HttpPageBufferClient client);

        void clientFailed(HttpPageBufferClient client, Throwable cause);

        void releaseQuota(long bytes);
    }

    private final HttpClient httpClient;
    private final DataSize maxResponseSize;
    private final URI location;
    private final ClientCallback clientCallback;
    private final ScheduledExecutorService scheduler;
    private final Backoff backoff;
    private final JsonCodec<BufferSummary> bufferSummaryCodec;

    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private HttpResponseFuture<?> future;
    @GuardedBy("this")
    private DateTime lastUpdate = DateTime.now();
    @GuardedBy("this")
    private long token;
    @GuardedBy("this")
    private boolean scheduled;
    @GuardedBy("this")
    private boolean completed;
    @GuardedBy("this")
    private String taskInstanceId;
    @GuardedBy("this")
    private final List<Long> pendingPageSizesInBytes = new LinkedList<>();

    private final AtomicLong rowsReceived = new AtomicLong();
    private final AtomicInteger pagesReceived = new AtomicInteger();

    private final AtomicLong rowsRejected = new AtomicLong();
    private final AtomicInteger pagesRejected = new AtomicInteger();

    private final AtomicInteger requestsScheduled = new AtomicInteger();
    private final AtomicInteger requestsCompleted = new AtomicInteger();
    private final AtomicInteger requestsFailed = new AtomicInteger();

    private final Executor pageBufferClientCallbackExecutor;

    // reserved bytes and released bytes should be exactly the same (eventually)
    // record both of them for debugging (e.g., reservedBytes > releasedBytes for a long time denotes a potential deadlock or unhealthy network delay)
    @GuardedBy("this")
    private long reservedBytes;
    @GuardedBy("this")
    private long releasedBytes;
    @GuardedBy("this")
    private long receivedBytes;
    @GuardedBy("this")
    private long backoffNanos;

    public HttpPageBufferClient(
            HttpClient httpClient,
            DataSize maxResponseSize,
            Duration maxErrorDuration,
            URI location,
            ClientCallback clientCallback,
            ScheduledExecutorService scheduler,
            Executor pageBufferClientCallbackExecutor,
            JsonCodec<BufferSummary> bufferSummaryCodec)
    {
        this(httpClient, maxResponseSize, maxErrorDuration, location, clientCallback, scheduler, Ticker.systemTicker(), pageBufferClientCallbackExecutor, bufferSummaryCodec);
    }

    public HttpPageBufferClient(
            HttpClient httpClient,
            DataSize maxResponseSize,
            Duration maxErrorDuration,
            URI location,
            ClientCallback clientCallback,
            ScheduledExecutorService scheduler,
            Ticker ticker,
            Executor pageBufferClientCallbackExecutor,
            JsonCodec<BufferSummary> bufferSummaryCodec)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.maxResponseSize = requireNonNull(maxResponseSize, "maxResponseSize is null");
        this.location = requireNonNull(location, "location is null");
        this.clientCallback = requireNonNull(clientCallback, "clientCallback is null");
        this.scheduler = requireNonNull(scheduler, "scheduler is null");
        this.pageBufferClientCallbackExecutor = requireNonNull(pageBufferClientCallbackExecutor, "pageBufferClientCallbackExecutor is null");
        requireNonNull(maxErrorDuration, "maxErrorDuration is null");
        requireNonNull(ticker, "ticker is null");
        this.backoff = new Backoff(maxErrorDuration, ticker);
        this.bufferSummaryCodec = requireNonNull(bufferSummaryCodec, "bufferSummaryCodec is null");
    }

    public synchronized PageBufferClientStatus getStatus()
    {
        String state;
        if (closed) {
            state = "closed";
        }
        else if (future != null) {
            state = "running";
        }
        else if (scheduled) {
            state = "scheduled";
        }
        else if (completed) {
            state = "completed";
        }
        else {
            state = "queued";
        }
        String httpRequestState = "not scheduled";
        if (future != null) {
            httpRequestState = future.getState();
        }

        long rejectedRows = rowsRejected.get();
        int rejectedPages = pagesRejected.get();

        return new PageBufferClientStatus(
                location,
                state,
                lastUpdate,
                rowsReceived.get(),
                pagesReceived.get(),
                rejectedRows == 0 ? OptionalLong.empty() : OptionalLong.of(rejectedRows),
                rejectedPages == 0 ? OptionalInt.empty() : OptionalInt.of(rejectedPages),
                requestsScheduled.get(),
                requestsCompleted.get(),
                requestsFailed.get(),
                httpRequestState,
                reservedBytes,
                releasedBytes,
                receivedBytes,
                backoffNanos);
    }

    public synchronized boolean isRunning()
    {
        return future != null;
    }

    @Override
    public void close()
    {
        boolean shouldSendDelete;
        Future<?> future;
        synchronized (this) {
            shouldSendDelete = !closed;

            closed = true;

            future = this.future;

            this.future = null;

            lastUpdate = DateTime.now();
        }

        if (future != null && !future.isDone()) {
            future.cancel(true);
        }

        // abort the output buffer on the remote node; response of delete is ignored
        if (shouldSendDelete) {
            sendDelete();
        }
    }

    public synchronized long getTotalPendingSize()
    {
        return pendingPageSizesInBytes.stream().mapToLong(Long::longValue).sum();
    }

    public synchronized void scheduleRequest(long maxBytes)
    {
        reservedBytes += maxBytes;
        if (closed || (future != null) || scheduled) {
            releaseQuota(maxBytes);
            return;
        }
        scheduled = true;

        // start before scheduling to include error delay
        backoff.startRequest();

        long delayNanos = backoff.getBackoffDelayNanos();
        backoffNanos += delayNanos;

        // TODO: backoff is usually short (< 500ms); but still a problem reserving the memory but not using it
        scheduler.schedule(() -> {
            try {
                initiateRequest(maxBytes);
            }
            catch (Throwable t) {
                // should not happen, but be safe and fail the operator
                clientCallback.clientFailed(HttpPageBufferClient.this, t);
            }
        }, delayNanos, NANOSECONDS);

        lastUpdate = DateTime.now();
        requestsScheduled.incrementAndGet();
    }

    @GuardedBy("this")
    private void releaseQuota(long bytes)
    {
        releasedBytes += bytes;
        clientCallback.releaseQuota(bytes);
    }

    private synchronized void initiateRequest(long maxBytes)
    {
        scheduled = false;
        if (closed || (future != null)) {
            releaseQuota(maxBytes);
            return;
        }

        if (completed) {
            releaseQuota(maxBytes);
            sendDelete();
        }
        else if (pendingPageSizesInBytes.isEmpty() || maxBytes == 0) {
            // There could be a race when the exchange client found there was no pending pages when scheduling
            // till this point some other delayed request has come back with some page summary
            releaseQuota(maxBytes);
            sendGetSummary();
        }
        else {
            sendGetData(maxBytes);
        }

        lastUpdate = DateTime.now();
    }

    @GuardedBy("this")
    private void sendGetSummary()
    {
        URI uri = HttpUriBuilder.uriBuilderFrom(location).appendPath(PATH_BUFFER_SUMMARY).appendPath(String.valueOf(token)).build();
        HttpResponseFuture<BufferSummary> resultFuture = httpClient.executeAsync(
                prepareGet()
                        .setHeader(PRESTO_MAX_SIZE, maxResponseSize.toString())
                        .setUri(uri).build(),
                new PageSizeResponseHandler(bufferSummaryCodec));

        future = resultFuture;
        Futures.addCallback(resultFuture, new FutureCallback<BufferSummary>()
        {
            @Override
            public void onSuccess(BufferSummary bufferSummary)
            {
                checkNotHoldsLock(this);

                backoff.success();

                List<Long> pageSizesInBytes;
                try {
                    synchronized (HttpPageBufferClient.this) {
                        if (taskInstanceId == null) {
                            taskInstanceId = bufferSummary.getTaskInstanceId();
                        }

                        if (!isNullOrEmpty(taskInstanceId) && !bufferSummary.getTaskInstanceId().equals(taskInstanceId)) {
                            throw new PageTransportErrorException(format("expected task id [%s]; found [%s]", taskInstanceId, bufferSummary.getTaskInstanceId()));
                        }

                        if (bufferSummary.getToken() == token) {
                            pageSizesInBytes = bufferSummary.getPageSizesInBytes();
                        }
                        else {
                            pageSizesInBytes = ImmutableList.of();
                        }

                        // client is complete; acknowledge it by sending it a delete in the next request
                        if (bufferSummary.isBufferComplete()) {
                            if (!pageSizesInBytes.isEmpty()) {
                                throw new PageTransportErrorException(format("buffer completes with %s pages", pageSizesInBytes.size()));
                            }
                            completed = true;
                        }
                        if (future == resultFuture) {
                            future = null;
                        }

                        if (token == bufferSummary.getToken() && pendingPageSizesInBytes.isEmpty()) {
                            pendingPageSizesInBytes.addAll(bufferSummary.getPageSizesInBytes());
                        }

                        lastUpdate = DateTime.now();
                    }
                }
                catch (PrestoException e) {
                    handleFailure(new PageTransportErrorException(format("Error fetching %s: %s", uri.toASCIIString(), e.getMessage()), e), resultFuture);
                    return;
                }

                requestsCompleted.incrementAndGet();
                clientCallback.requestComplete(HttpPageBufferClient.this);
            }

            @Override
            public void onFailure(Throwable t)
            {
                onRequestFailure(uri, resultFuture, t);
            }
        }, scheduler);
    }

    @GuardedBy("this")
    private void sendGetData(long maxBytes)
    {
        // for the normal case, token should be the same as this.token
        // but that is not always guaranteed given there could be duplicated requested
        long token = this.token;

        URI uri = HttpUriBuilder.uriBuilderFrom(location).appendPath(PATH_BUFFER_DATA).appendPath(String.valueOf(token)).build();
        HttpResponseFuture<PagesResponse> resultFuture = httpClient.executeAsync(
                prepareGet()
                        .setHeader(PRESTO_MAX_SIZE, new DataSize(maxBytes, BYTE).toString())
                        .setUri(uri).build(),
                new PageResponseHandler());

        future = resultFuture;
        resultFuture.addListener(() -> releaseQuota(maxBytes), scheduler);
        Futures.addCallback(resultFuture, new FutureCallback<PagesResponse>()
        {
            @Override
            public void onSuccess(PagesResponse result)
            {
                checkNotHoldsLock(this);

                backoff.success();

                List<SerializedPage> pages;
                try {
                    synchronized (HttpPageBufferClient.this) {
                        if (taskInstanceId == null) {
                            taskInstanceId = result.getTaskInstanceId();
                        }

                        if (!isNullOrEmpty(taskInstanceId) && !result.getTaskInstanceId().equals(taskInstanceId)) {
                            throw new PageTransportErrorException(format("expected task id [%s]; found [%s]", taskInstanceId, result.getTaskInstanceId()));
                        }

                        if (result.getToken() != token) {
                            throw new PageTransportErrorException(format("expected token [%s]; found [%s]", result.getToken(), token));
                        }

                        if (token == HttpPageBufferClient.this.token) {
                            pages = result.getPages();
                            if (pages.size() > pendingPageSizesInBytes.size()) {
                                // exchange client should only provide quota no more than the size of all pending pages
                                // it is impossible for the current client to fetch more pages than expected
                                throw new PageTransportErrorException(format("expected max number of pages to fetch: %s; found: %s", pendingPageSizesInBytes.size(), pages.size()));
                            }
                            for (int i = 0; i < pages.size(); i++) {
                                SerializedPage page = pages.get(i);
                                long pendingPageSize = pendingPageSizesInBytes.get(i);
                                // the size of a serialized page on both server and client should be the same but that is not true for the retained size
                                // the server uses a dynamic slice to serialize a page
                                // if the page is uncompressed, the retained size could be larger than the actual size
                                // if it is compressed, the slice will be copied with the exact same length as the actual one so that the retained sizes on both server and client are the same
                                if (page.getRetainedSizeInBytes() > pendingPageSize) {
                                    throw new PageTransportErrorException(format("expected page size: %s; found: %s", page.getRetainedSizeInBytes(), pendingPageSize));
                                }
                                else if (page.getRetainedSizeInBytes() < pendingPageSize && page.getCompression().equals(COMPRESSED)) {
                                    throw new PageTransportErrorException(format("expected compressed page size: %s; found: %s", page.getRetainedSizeInBytes(), pendingPageSize));
                                }
                                receivedBytes += page.getRetainedSizeInBytes();
                            }
                            // add bytes in batch in order to match the distribution of reservedBytes
                            pendingPageSizesInBytes.subList(0, pages.size()).clear();
                            HttpPageBufferClient.this.token = result.getNextToken();
                        }
                        else {
                            pages = ImmutableList.of();
                        }
                    }
                }
                catch (PrestoException e) {
                    handleFailure(new PageTransportErrorException(format("Error fetching %s: %s", uri.toASCIIString(), e.getMessage()), e), resultFuture);
                    return;
                }

                // add pages
                if (!pages.isEmpty()) {
                    if (clientCallback.addPages(HttpPageBufferClient.this, pages)) {
                        pagesReceived.addAndGet(pages.size());
                        rowsReceived.addAndGet(pages.stream().mapToLong(SerializedPage::getPositionCount).sum());
                    }
                    else {
                        pagesRejected.addAndGet(pages.size());
                        rowsRejected.addAndGet(pages.stream().mapToLong(SerializedPage::getPositionCount).sum());
                    }
                }

                synchronized (HttpPageBufferClient.this) {
                    // client is complete, acknowledge it by sending it a delete in the next request
                    if (result.isClientComplete()) {
                        completed = true;
                    }
                    if (future == resultFuture) {
                        future = null;
                    }
                    lastUpdate = DateTime.now();
                }
                requestsCompleted.incrementAndGet();
                clientCallback.requestComplete(HttpPageBufferClient.this);
            }

            @Override
            public void onFailure(Throwable t)
            {
                onRequestFailure(uri, resultFuture, t);
            }
        }, pageBufferClientCallbackExecutor);
    }

    private void onRequestFailure(URI uri, HttpResponseFuture<?> resultFuture, Throwable t)
    {
        log.debug("Request to %s failed %s", uri, t);
        checkNotHoldsLock(this);

        t = rewriteException(t);
        if (!(t instanceof PrestoException) && backoff.failure()) {
            String message = format("%s (%s - %s failures, failure duration %s, total failed request time %s)",
                    WORKER_NODE_ERROR,
                    uri,
                    backoff.getFailureCount(),
                    backoff.getFailureDuration().convertTo(SECONDS),
                    backoff.getFailureRequestTimeTotal().convertTo(SECONDS));
            t = new PageTransportTimeoutException(fromUri(uri), message, t);
        }
        handleFailure(t, resultFuture);
    }

    private synchronized void sendDelete()
    {
        HttpResponseFuture<StatusResponse> resultFuture = httpClient.executeAsync(prepareDelete().setUri(location).build(), createStatusResponseHandler());
        future = resultFuture;
        Futures.addCallback(resultFuture, new FutureCallback<StatusResponse>()
        {
            @Override
            public void onSuccess(@Nullable StatusResponse result)
            {
                checkNotHoldsLock(this);
                backoff.success();
                synchronized (HttpPageBufferClient.this) {
                    closed = true;
                    if (future == resultFuture) {
                        future = null;
                    }
                    lastUpdate = DateTime.now();
                }
                requestsCompleted.incrementAndGet();
                clientCallback.clientFinished(HttpPageBufferClient.this);
            }

            @Override
            public void onFailure(Throwable t)
            {
                checkNotHoldsLock(this);

                log.error("Request to delete %s failed %s", location, t);
                if (!(t instanceof PrestoException) && backoff.failure()) {
                    String message = format("Error closing remote buffer (%s - %s failures, failure duration %s, total failed request time %s)",
                            location,
                            backoff.getFailureCount(),
                            backoff.getFailureDuration().convertTo(SECONDS),
                            backoff.getFailureRequestTimeTotal().convertTo(SECONDS));
                    t = new PrestoException(REMOTE_BUFFER_CLOSE_FAILED, message, t);
                }
                handleFailure(t, resultFuture);
            }
        }, pageBufferClientCallbackExecutor);
    }

    private static void checkNotHoldsLock(Object lock)
    {
        checkState(!Thread.holdsLock(lock), "Cannot execute this method while holding a lock");
    }

    private void handleFailure(Throwable t, HttpResponseFuture<?> expectedFuture)
    {
        // Can not delegate to other callback while holding a lock on this
        checkNotHoldsLock(this);

        requestsFailed.incrementAndGet();
        requestsCompleted.incrementAndGet();

        if (t instanceof PrestoException) {
            clientCallback.clientFailed(HttpPageBufferClient.this, t);
        }

        synchronized (HttpPageBufferClient.this) {
            if (future == expectedFuture) {
                future = null;
            }
            lastUpdate = DateTime.now();
        }
        clientCallback.requestComplete(HttpPageBufferClient.this);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HttpPageBufferClient that = (HttpPageBufferClient) o;

        if (!location.equals(that.location)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return location.hashCode();
    }

    @Override
    public String toString()
    {
        String state;
        synchronized (this) {
            if (closed) {
                state = "CLOSED";
            }
            else if (future != null) {
                state = "RUNNING";
            }
            else {
                state = "QUEUED";
            }
        }
        return toStringHelper(this)
                .add("location", location)
                .addValue(state)
                .toString();
    }

    private static Throwable rewriteException(Throwable t)
    {
        if (t instanceof ResponseTooLargeException) {
            return new PageTooLargeException();
        }
        return t;
    }

    private static class PageSizeResponseHandler
            implements ResponseHandler<BufferSummary, RuntimeException>
    {
        private final FullJsonResponseHandler<BufferSummary> handler;

        public PageSizeResponseHandler(JsonCodec<BufferSummary> codec)
        {
            handler = createFullJsonResponseHandler(codec);
        }

        @Override
        public BufferSummary handleException(Request request, Exception exception)
        {
            throw propagate(request, exception);
        }

        @Override
        public BufferSummary handle(Request request, Response response)
        {
            JsonResponse<BufferSummary> jsonResponse;
            try {
                jsonResponse = handler.handle(request, response);
            }
            catch (RuntimeException e) {
                throw new PageTransportErrorException(format("Error fetching %s: %s", request.getUri().toASCIIString(), e.getMessage()), e);
            }

            if (response.getStatusCode() != HttpStatus.OK.code()) {
                throw new PageTransportErrorException(format(
                        "Expected response code to be 200, but was %s %s:%n%s",
                        response.getStatusCode(),
                        response.getStatusMessage(),
                        response.toString()));
            }

            // invalid content type can happen when an error page is returned, but is unlikely given the above 200
            String contentType = response.getHeader(CONTENT_TYPE);
            if (contentType == null) {
                throw new PageTransportErrorException(format("%s header is not set: %s", CONTENT_TYPE, response));
            }
            if (!contentType.equals(APPLICATION_JSON)) {
                throw new PageTransportErrorException(format("Expected %s response from server but got %s", APPLICATION_JSON, contentType));
            }

            return jsonResponse.getValue();
        }
    }

    public static class PageResponseHandler
            implements ResponseHandler<PagesResponse, RuntimeException>
    {
        @Override
        public PagesResponse handleException(Request request, Exception exception)
        {
            throw propagate(request, exception);
        }

        @Override
        public PagesResponse handle(Request request, Response response)
        {
            try {
                // no content means no content was created within the wait period, but query is still ok
                // if job is finished, complete is set in the response
                if (response.getStatusCode() == HttpStatus.NO_CONTENT.code()) {
                    return createEmptyPagesResponse(getTaskInstanceId(response), getToken(response), getNextToken(response), getComplete(response));
                }

                // otherwise we must have gotten an OK response, everything else is considered fatal
                if (response.getStatusCode() != HttpStatus.OK.code()) {
                    StringBuilder body = new StringBuilder();
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getInputStream(), UTF_8))) {
                        // Get up to 1000 lines for debugging
                        for (int i = 0; i < 1000; i++) {
                            String line = reader.readLine();
                            // Don't output more than 100KB
                            if (line == null || body.length() + line.length() > 100 * 1024) {
                                break;
                            }
                            body.append(line + "\n");
                        }
                    }
                    catch (RuntimeException | IOException e) {
                        // Ignored. Just return whatever message we were able to decode
                    }
                    throw new PageTransportErrorException(format("Expected response code to be 200, but was %s %s:%n%s", response.getStatusCode(), response.getStatusMessage(), body.toString()));
                }

                // invalid content type can happen when an error page is returned, but is unlikely given the above 200
                String contentType = response.getHeader(CONTENT_TYPE);
                if (contentType == null) {
                    throw new PageTransportErrorException(format("%s header is not set: %s", CONTENT_TYPE, response));
                }
                if (!mediaTypeMatches(contentType, PRESTO_PAGES_TYPE)) {
                    throw new PageTransportErrorException(format("Expected %s response from server but got %s", PRESTO_PAGES_TYPE, contentType));
                }

                String taskInstanceId = getTaskInstanceId(response);
                long token = getToken(response);
                long nextToken = getNextToken(response);
                boolean complete = getComplete(response);

                try (SliceInput input = new InputStreamSliceInput(response.getInputStream())) {
                    List<SerializedPage> pages = ImmutableList.copyOf(readSerializedPages(input));
                    return createPagesResponse(taskInstanceId, token, nextToken, pages, complete);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            catch (PageTransportErrorException e) {
                throw new PageTransportErrorException(format("Error fetching %s: %s", request.getUri().toASCIIString(), e.getMessage()), e);
            }
        }

        private static String getTaskInstanceId(Response response)
        {
            String taskInstanceId = response.getHeader(PRESTO_TASK_INSTANCE_ID);
            if (taskInstanceId == null) {
                throw new PageTransportErrorException(format("Expected %s header", PRESTO_TASK_INSTANCE_ID));
            }
            return taskInstanceId;
        }

        private static long getToken(Response response)
        {
            String tokenHeader = response.getHeader(PRESTO_PAGE_TOKEN);
            if (tokenHeader == null) {
                throw new PageTransportErrorException(format("Expected %s header", PRESTO_PAGE_TOKEN));
            }
            return Long.parseLong(tokenHeader);
        }

        private static long getNextToken(Response response)
        {
            String nextTokenHeader = response.getHeader(PRESTO_PAGE_NEXT_TOKEN);
            if (nextTokenHeader == null) {
                throw new PageTransportErrorException(format("Expected %s header", PRESTO_PAGE_NEXT_TOKEN));
            }
            return Long.parseLong(nextTokenHeader);
        }

        private static boolean getComplete(Response response)
        {
            String bufferComplete = response.getHeader(PRESTO_BUFFER_COMPLETE);
            if (bufferComplete == null) {
                throw new PageTransportErrorException(format("Expected %s header", PRESTO_BUFFER_COMPLETE));
            }
            return Boolean.parseBoolean(bufferComplete);
        }

        private static boolean mediaTypeMatches(String value, MediaType range)
        {
            try {
                return MediaType.parse(value).is(range);
            }
            catch (IllegalArgumentException | IllegalStateException e) {
                return false;
            }
        }
    }

    public static class PagesResponse
    {
        public static PagesResponse createPagesResponse(String taskInstanceId, long token, long nextToken, Iterable<SerializedPage> pages, boolean complete)
        {
            return new PagesResponse(taskInstanceId, token, nextToken, pages, complete);
        }

        public static PagesResponse createEmptyPagesResponse(String taskInstanceId, long token, long nextToken, boolean complete)
        {
            return new PagesResponse(taskInstanceId, token, nextToken, ImmutableList.of(), complete);
        }

        private final String taskInstanceId;
        private final long token;
        private final long nextToken;
        private final List<SerializedPage> pages;
        private final boolean clientComplete;

        private PagesResponse(String taskInstanceId, long token, long nextToken, Iterable<SerializedPage> pages, boolean clientComplete)
        {
            this.taskInstanceId = taskInstanceId;
            this.token = token;
            this.nextToken = nextToken;
            this.pages = ImmutableList.copyOf(pages);
            this.clientComplete = clientComplete;
        }

        public long getToken()
        {
            return token;
        }

        public long getNextToken()
        {
            return nextToken;
        }

        public List<SerializedPage> getPages()
        {
            return pages;
        }

        public boolean isClientComplete()
        {
            return clientComplete;
        }

        public String getTaskInstanceId()
        {
            return taskInstanceId;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("token", token)
                    .add("nextToken", nextToken)
                    .add("pagesSize", pages.size())
                    .add("clientComplete", clientComplete)
                    .toString();
        }
    }
}
