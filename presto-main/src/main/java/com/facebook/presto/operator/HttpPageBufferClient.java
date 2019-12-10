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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpClient.HttpResponseFuture;
import com.facebook.airlift.http.client.HttpStatus;
import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.Response;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.http.client.ResponseTooLargeException;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.execution.buffer.BufferResult;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.server.remotetask.Backoff;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Splitter;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.net.MediaType;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
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
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.airlift.concurrent.MoreFutures.addTimeout;
import static com.facebook.airlift.http.client.HttpStatus.familyForStatusCode;
import static com.facebook.airlift.http.client.Request.Builder.prepareDelete;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.ResponseHandlerUtils.propagate;
import static com.facebook.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static com.facebook.presto.PrestoMediaTypes.PRESTO_PAGES_TYPE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_BUFFER_COMPLETE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_SIZE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_NEXT_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TASK_INSTANCE_ID;
import static com.facebook.presto.execution.buffer.PagesSerdeUtil.readSerializedPages;
import static com.facebook.presto.operator.HttpPageBufferClient.PagesResponse.createEmptyPagesResponse;
import static com.facebook.presto.operator.HttpPageBufferClient.PagesResponse.createPagesResponse;
import static com.facebook.presto.spi.HostAddress.fromUri;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_BUFFER_CLOSE_FAILED;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_MISMATCH;
import static com.facebook.presto.util.Failures.REMOTE_TASK_MISMATCH_ERROR;
import static com.facebook.presto.util.Failures.WORKER_NODE_ERROR;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public final class HttpPageBufferClient
        implements Closeable
{
    private static final Logger log = Logger.get(HttpPageBufferClient.class);
    private static final Duration DEFAULT_MAX_WAIT_TIME = new Duration(2, SECONDS);

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
    private final boolean acknowledgePages;
    private final URI location;
    private final ClientCallback clientCallback;
    private final ScheduledExecutorService scheduler;
    private final Backoff backoff;
    private final TaskManager taskManager;
    private final ListeningScheduledExecutorService taskManagerScheduler;
    private final ScheduledExecutorService timeoutExecutor;
    private final TaskId taskId;
    private final OutputBuffers.OutputBufferId outputBufferId;

    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private ListenableFuture<?> future;
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
            Duration maxErrorDuration,
            boolean acknowledgePages,
            URI location,
            ClientCallback clientCallback,
            ScheduledExecutorService scheduler,
            Executor pageBufferClientCallbackExecutor)
    {
        this(httpClient, maxErrorDuration, acknowledgePages, location, clientCallback, scheduler, Ticker.systemTicker(), pageBufferClientCallbackExecutor, null, null);
    }

    public HttpPageBufferClient(
            HttpClient httpClient,
            Duration maxErrorDuration,
            boolean acknowledgePages,
            URI location,
            ClientCallback clientCallback,
            ScheduledExecutorService scheduler,
            Ticker ticker,
            Executor pageBufferClientCallbackExecutor)
    {
        this(httpClient, maxErrorDuration, acknowledgePages, location, clientCallback, scheduler, ticker, pageBufferClientCallbackExecutor, null, null);
    }

    public HttpPageBufferClient(
            HttpClient httpClient,
            Duration maxErrorDuration,
            boolean acknowledgePages,
            URI location,
            ClientCallback clientCallback,
            ScheduledExecutorService scheduler,
            Executor pageBufferClientCallbackExecutor,
            TaskManager taskManager,
            ScheduledExecutorService timeoutExecutor)
    {
        this(httpClient, maxErrorDuration, acknowledgePages, location, clientCallback, scheduler, Ticker.systemTicker(), pageBufferClientCallbackExecutor, taskManager, timeoutExecutor);
    }

    public HttpPageBufferClient(
            HttpClient httpClient,
            Duration maxErrorDuration,
            boolean acknowledgePages,
            URI location,
            ClientCallback clientCallback,
            ScheduledExecutorService scheduler,
            Ticker ticker,
            Executor pageBufferClientCallbackExecutor,
            TaskManager taskManager,
            ScheduledExecutorService timeoutExecutor)
    {
        this.acknowledgePages = acknowledgePages;
        this.location = requireNonNull(location, "location is null");
        this.clientCallback = requireNonNull(clientCallback, "clientCallback is null");
        this.scheduler = requireNonNull(scheduler, "scheduler is null");
        taskManagerScheduler = MoreExecutors.listeningDecorator(scheduler);
        this.pageBufferClientCallbackExecutor = requireNonNull(pageBufferClientCallbackExecutor, "pageBufferClientCallbackExecutor is null");
        requireNonNull(maxErrorDuration, "maxErrorDuration is null");
        requireNonNull(ticker, "ticker is null");
        this.timeoutExecutor = timeoutExecutor;
        List<String> strings = Splitter.on("/").omitEmptyStrings().splitToList(location.getPath());
        if (taskManager != null && strings.size() == 5 && "v1".equals(strings.get(0)) && "task".equals(strings.get(1)) && "results".equals(strings.get(3))) {
            this.taskId = TaskId.valueOf(strings.get(2));
            this.outputBufferId = OutputBuffers.OutputBufferId.fromString(strings.get(4));
            this.taskManager = taskManager;
            this.httpClient = null;
            this.backoff = Backoff.createNeverBackingOff();
        }
        else {
            this.taskId = null;
            this.outputBufferId = null;
            this.taskManager = null;
            this.httpClient = requireNonNull(httpClient, "httpClient is null");
            this.backoff = new Backoff(maxErrorDuration, ticker);
        }
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
        if (future != null && future instanceof HttpResponseFuture) {
            httpRequestState = ((HttpResponseFuture) future).getState();
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
        ListenableFuture<?> future;
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

    public synchronized void scheduleRequest(DataSize maxResponseSize)
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
                initiateRequest(maxResponseSize);
            }
            catch (Throwable t) {
                // should not happen, but be safe and fail the operator
                clientCallback.clientFailed(HttpPageBufferClient.this, t);
            }
        }, delayNanos, NANOSECONDS);

        lastUpdate = DateTime.now();
        requestsScheduled.incrementAndGet();
    }

    private synchronized void initiateRequest(DataSize maxResponseSize)
    {
        scheduled = false;
        if (closed || (future != null)) {
            return;
        }

        if (completed) {
            sendDelete();
        }
        else {
            sendGetResults(maxResponseSize);
        }

        lastUpdate = DateTime.now();
    }

    private synchronized void sendAcknowledgeRaw(PagesResponse result)
    {
        Futures.addCallback(
                taskManagerScheduler.submit(() ->
                        taskManager.acknowledgeTaskResults(taskId, outputBufferId, result.getNextToken())),
                new FutureCallback<Object>()
                {
                    @Override
                    public void onSuccess(Object result)
                    {
                    }

                    @Override
                    public void onFailure(Throwable t)
                    {
                        log.debug(t, "Acknowledge request failed: %s", taskId);
                    }
                }, directExecutor());
    }

    private synchronized void sendAcknowledgeHttp(PagesResponse result)
    {
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

    private synchronized void sendAcknowledge(PagesResponse response)
    {
        if (taskManager != null) {
            sendAcknowledgeRaw(response);
        }
        else {
            sendAcknowledgeHttp(response);
        }
    }

    private synchronized void sendGetResults(DataSize maxResponseSize)
    {
        Optional<URI> uri;
        ListenableFuture<PagesResponse> resultFuture;
        if (taskManager != null) {
            resultFuture = sendGetResultsRaw(maxResponseSize);
            uri = Optional.empty();
        }
        else {
            uri = Optional.of(HttpUriBuilder.uriBuilderFrom(location).appendPath(String.valueOf(token)).build());
            resultFuture = sendGetResultsHttp(uri.get(), maxResponseSize);
        }
        future = resultFuture;
        Futures.addCallback(resultFuture, new GetResponseHandler(resultFuture, uri), pageBufferClientCallbackExecutor);
    }

    private static Duration randomizeWaitTime(Duration waitTime)
    {
        // Randomize in [T/2, T], so wait is not near zero and the client-supplied max wait time is respected
        long halfWaitMillis = waitTime.toMillis() / 2;
        return new Duration(halfWaitMillis + ThreadLocalRandom.current().nextLong(halfWaitMillis), MILLISECONDS);
    }

    private synchronized ListenableFuture<PagesResponse> sendGetResultsRaw(DataSize maxResponseSize)
    {
        // Potentially different from the this.taskInstanceId
        String localTaskInstanceId = taskManager.getTaskInstanceId(taskId);
        ListenableFuture<BufferResult> bufferResultFuture = taskManager.getTaskResults(taskId, outputBufferId, token, maxResponseSize);
        Duration waitTime = randomizeWaitTime(DEFAULT_MAX_WAIT_TIME);
        bufferResultFuture = addTimeout(
                bufferResultFuture,
                () -> BufferResult.emptyResults(localTaskInstanceId, token, false),
                waitTime,
                timeoutExecutor);

        return Futures.transform(bufferResultFuture, result -> {
            List<SerializedPage> serializedPages = result.getSerializedPages();
            if (serializedPages.isEmpty()) {
                return createEmptyPagesResponse(localTaskInstanceId, token, result.getNextToken(), result.isBufferComplete());
            }
            else {
                return createPagesResponse(localTaskInstanceId, token, result.getNextToken(), serializedPages, result.isBufferComplete());
            }
        }, directExecutor());
    }

    private class GetResponseHandler
            implements FutureCallback<PagesResponse>
    {
        private final ListenableFuture<PagesResponse> resultFuture;
        private final Optional<URI> uri;

        public GetResponseHandler(ListenableFuture<PagesResponse> resultFuture, Optional<URI> uri)
        {
            this.resultFuture = resultFuture;
            this.uri = uri;
        }

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
                        throw new PrestoException(REMOTE_TASK_MISMATCH, format("%s (%s)", REMOTE_TASK_MISMATCH_ERROR, uri.isPresent() ? fromUri(uri.get()) : taskId));
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
                    sendAcknowledge(result);
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
                        uri.isPresent() ? uri.get() : (taskId != null) ? taskId : null,
                        backoff.getFailureCount(),
                        backoff.getFailureDuration().convertTo(SECONDS),
                        backoff.getFailureRequestTimeTotal().convertTo(SECONDS));
                if (uri.isPresent()) {
                    t = new PageTransportTimeoutException(fromUri(uri.get()), message, t);
                }
            }
            handleFailure(t, resultFuture);
        }
    }

    private synchronized ListenableFuture<PagesResponse> sendGetResultsHttp(URI uri, DataSize maxResponseSize)
    {
        return httpClient.executeAsync(
                prepareGet()
                        .setHeader(PRESTO_MAX_SIZE, maxResponseSize.toString())
                        .setUri(uri).build(),
                new PageResponseHandler());
    }

    private synchronized void sendDelete()
    {
        if (taskManager != null) {
            future = sendDeleteRaw();
        }
        else {
            future = sendDeleteHttp();
        }
        Futures.addCallback(future, new DeleteResponseHandler(future), pageBufferClientCallbackExecutor);
    }

    private class DeleteResponseHandler
            implements FutureCallback
    {
        private final ListenableFuture<?> resultFuture;

        public DeleteResponseHandler(ListenableFuture<?> resultFuture)
        {
            this.resultFuture = resultFuture;
        }

        @Override
        public void onSuccess(@Nullable Object result)
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
    }

    private synchronized ListenableFuture<?> sendDeleteRaw()
    {
        return taskManagerScheduler.submit(() -> taskManager.abortTaskResults(taskId, outputBufferId));
    }

    private synchronized ListenableFuture<?> sendDeleteHttp()
    {
        return httpClient.executeAsync(prepareDelete().setUri(location).build(), createStatusResponseHandler());
    }

    private static void checkNotHoldsLock(Object lock)
    {
        checkState(!Thread.holdsLock(lock), "Cannot execute this method while holding a lock");
    }

    private void handleFailure(Throwable t, ListenableFuture<?> expectedFuture)
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
                throw new PageTransportErrorException("Error fetching " + request.getUri().toASCIIString(), e);
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
