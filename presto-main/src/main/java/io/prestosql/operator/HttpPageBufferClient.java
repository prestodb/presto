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
package io.prestosql.operator;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.net.MediaType;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClient.HttpResponseFuture;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.ResponseTooLargeException;
import io.airlift.log.Logger;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.SliceInput;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.execution.buffer.SerializedPage;
import io.prestosql.server.remotetask.Backoff;
import io.prestosql.spi.PrestoException;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.airlift.http.client.HttpStatus.familyForStatusCode;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.ResponseHandlerUtils.propagate;
import static io.airlift.http.client.StatusResponseHandler.StatusResponse;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.prestosql.PrestoMediaTypes.PRESTO_PAGES_TYPE;
import static io.prestosql.client.PrestoHeaders.PRESTO_BUFFER_COMPLETE;
import static io.prestosql.client.PrestoHeaders.PRESTO_MAX_SIZE;
import static io.prestosql.client.PrestoHeaders.PRESTO_PAGE_NEXT_TOKEN;
import static io.prestosql.client.PrestoHeaders.PRESTO_PAGE_TOKEN;
import static io.prestosql.client.PrestoHeaders.PRESTO_TASK_INSTANCE_ID;
import static io.prestosql.execution.buffer.PagesSerdeUtil.readSerializedPages;
import static io.prestosql.operator.HttpPageBufferClient.PagesResponse.createEmptyPagesResponse;
import static io.prestosql.operator.HttpPageBufferClient.PagesResponse.createPagesResponse;
import static io.prestosql.spi.HostAddress.fromUri;
import static io.prestosql.spi.StandardErrorCode.REMOTE_BUFFER_CLOSE_FAILED;
import static io.prestosql.spi.StandardErrorCode.REMOTE_TASK_MISMATCH;
import static io.prestosql.util.Failures.REMOTE_TASK_MISMATCH_ERROR;
import static io.prestosql.util.Failures.WORKER_NODE_ERROR;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

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
    }

    private final HttpClient httpClient;
    private final DataSize maxResponseSize;
    private final boolean acknowledgePages;
    private final URI location;
    private final ClientCallback clientCallback;
    private final ScheduledExecutorService scheduler;
    private final Backoff backoff;

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

    private final AtomicLong rowsReceived = new AtomicLong();
    private final AtomicInteger pagesReceived = new AtomicInteger();

    private final AtomicLong rowsRejected = new AtomicLong();
    private final AtomicInteger pagesRejected = new AtomicInteger();

    private final AtomicInteger requestsScheduled = new AtomicInteger();
    private final AtomicInteger requestsCompleted = new AtomicInteger();
    private final AtomicInteger requestsFailed = new AtomicInteger();

    private final Executor pageBufferClientCallbackExecutor;

    public HttpPageBufferClient(
            HttpClient httpClient,
            DataSize maxResponseSize,
            Duration maxErrorDuration,
            boolean acknowledgePages,
            URI location,
            ClientCallback clientCallback,
            ScheduledExecutorService scheduler,
            Executor pageBufferClientCallbackExecutor)
    {
        this(httpClient, maxResponseSize, maxErrorDuration, acknowledgePages, location, clientCallback, scheduler, Ticker.systemTicker(), pageBufferClientCallbackExecutor);
    }

    public HttpPageBufferClient(
            HttpClient httpClient,
            DataSize maxResponseSize,
            Duration maxErrorDuration,
            boolean acknowledgePages,
            URI location,
            ClientCallback clientCallback,
            ScheduledExecutorService scheduler,
            Ticker ticker,
            Executor pageBufferClientCallbackExecutor)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.maxResponseSize = requireNonNull(maxResponseSize, "maxResponseSize is null");
        this.acknowledgePages = acknowledgePages;
        this.location = requireNonNull(location, "location is null");
        this.clientCallback = requireNonNull(clientCallback, "clientCallback is null");
        this.scheduler = requireNonNull(scheduler, "scheduler is null");
        this.pageBufferClientCallbackExecutor = requireNonNull(pageBufferClientCallbackExecutor, "pageBufferClientCallbackExecutor is null");
        requireNonNull(maxErrorDuration, "maxErrorDuration is null");
        requireNonNull(ticker, "ticker is null");
        this.backoff = new Backoff(maxErrorDuration, ticker);
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
                httpRequestState);
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

    public synchronized void scheduleRequest()
    {
        if (closed || (future != null) || scheduled) {
            return;
        }
        scheduled = true;

        // start before scheduling to include error delay
        backoff.startRequest();

        long delayNanos = backoff.getBackoffDelayNanos();
        scheduler.schedule(() -> {
            try {
                initiateRequest();
            }
            catch (Throwable t) {
                // should not happen, but be safe and fail the operator
                clientCallback.clientFailed(HttpPageBufferClient.this, t);
            }
        }, delayNanos, NANOSECONDS);

        lastUpdate = DateTime.now();
        requestsScheduled.incrementAndGet();
    }

    private synchronized void initiateRequest()
    {
        scheduled = false;
        if (closed || (future != null)) {
            return;
        }

        if (completed) {
            sendDelete();
        }
        else {
            sendGetResults();
        }

        lastUpdate = DateTime.now();
    }

    private synchronized void sendGetResults()
    {
        URI uri = HttpUriBuilder.uriBuilderFrom(location).appendPath(String.valueOf(token)).build();
        HttpResponseFuture<PagesResponse> resultFuture = httpClient.executeAsync(
                prepareGet()
                        .setHeader(PRESTO_MAX_SIZE, maxResponseSize.toString())
                        .setUri(uri).build(),
                new PageResponseHandler());

        future = resultFuture;
        Futures.addCallback(resultFuture, new FutureCallback<PagesResponse>()
        {
            @Override
            public void onSuccess(PagesResponse result)
            {
                checkNotHoldsLock(this);

                backoff.success();

                List<SerializedPage> pages;
                try {
                    boolean shouldAcknowledge = false;
                    synchronized (HttpPageBufferClient.this) {
                        if (taskInstanceId == null) {
                            taskInstanceId = result.getTaskInstanceId();
                        }

                        if (!isNullOrEmpty(taskInstanceId) && !result.getTaskInstanceId().equals(taskInstanceId)) {
                            // TODO: update error message
                            throw new PrestoException(REMOTE_TASK_MISMATCH, format("%s (%s)", REMOTE_TASK_MISMATCH_ERROR, fromUri(uri)));
                        }

                        if (result.getToken() == token) {
                            pages = result.getPages();
                            token = result.getNextToken();
                            shouldAcknowledge = pages.size() > 0;
                        }
                        else {
                            pages = ImmutableList.of();
                        }
                    }

                    if (shouldAcknowledge && acknowledgePages) {
                        // Acknowledge token without handling the response.
                        // The next request will also make sure the token is acknowledged.
                        // This is to fast release the pages on the buffer side.
                        URI uri = HttpUriBuilder.uriBuilderFrom(location).appendPath(String.valueOf(result.getNextToken())).appendPath("acknowledge").build();
                        httpClient.executeAsync(prepareGet().setUri(uri).build(), new ResponseHandler<Void, RuntimeException>()
                        {
                            @Override
                            public Void handleException(Request request, Exception exception)
                            {
                                log.debug(exception, "Acknowledge request failed: %s", uri);
                                return null;
                            }

                            @Override
                            public Void handle(Request request, Response response)
                            {
                                if (familyForStatusCode(response.getStatusCode()) != HttpStatus.Family.SUCCESSFUL) {
                                    log.debug("Unexpected acknowledge response code: %s", response.getStatusCode());
                                }
                                return null;
                            }
                        });
                    }
                }
                catch (PrestoException e) {
                    handleFailure(e, resultFuture);
                    return;
                }

                // add pages:
                // addPages must be called regardless of whether pages is an empty list because
                // clientCallback can keep stats of requests and responses. For example, it may
                // keep track of how often a client returns empty response and adjust request
                // frequency or buffer size.
                if (clientCallback.addPages(HttpPageBufferClient.this, pages)) {
                    pagesReceived.addAndGet(pages.size());
                    rowsReceived.addAndGet(pages.stream().mapToLong(SerializedPage::getPositionCount).sum());
                }
                else {
                    pagesRejected.addAndGet(pages.size());
                    rowsRejected.addAndGet(pages.stream().mapToLong(SerializedPage::getPositionCount).sum());
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
        }, pageBufferClientCallbackExecutor);
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
