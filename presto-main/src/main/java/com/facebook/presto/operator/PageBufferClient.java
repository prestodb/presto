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

import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.server.remotetask.Backoff;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.spi.HostAddress.fromUri;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_BUFFER_CLOSE_FAILED;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_MISMATCH;
import static com.facebook.presto.spi.StandardErrorCode.SERIALIZED_PAGE_CHECKSUM_ERROR;
import static com.facebook.presto.spi.page.PagesSerdeUtil.isChecksumValid;
import static com.facebook.presto.util.Failures.REMOTE_TASK_MISMATCH_ERROR;
import static com.facebook.presto.util.Failures.WORKER_NODE_ERROR;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public final class PageBufferClient
        implements Closeable
{
    private static final Logger log = Logger.get(PageBufferClient.class);

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
        boolean addPages(PageBufferClient client, List<SerializedPage> pages);

        void requestComplete(PageBufferClient client);

        void clientFinished(PageBufferClient client);

        void clientFailed(PageBufferClient client, Throwable cause);
    }

    private final RpcShuffleClient resultClient;
    private final boolean acknowledgePages;
    private final URI location;
    private final Optional<URI> asyncPageTransportLocation;
    private final ClientCallback clientCallback;
    private final ScheduledExecutorService scheduler;
    private final Backoff backoff;

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

    public PageBufferClient(
            RpcShuffleClient resultClient,
            Duration maxErrorDuration,
            boolean acknowledgePages,
            URI location,
            Optional<URI> asyncPageTransportLocation,
            ClientCallback clientCallback,
            ScheduledExecutorService scheduler,
            Executor pageBufferClientCallbackExecutor)
    {
        this(resultClient, maxErrorDuration, acknowledgePages, location, asyncPageTransportLocation, clientCallback, scheduler, Ticker.systemTicker(), pageBufferClientCallbackExecutor);
    }

    public PageBufferClient(
            RpcShuffleClient resultClient,
            Duration maxErrorDuration,
            boolean acknowledgePages,
            URI location,
            Optional<URI> asyncPageTransportLocation,
            ClientCallback clientCallback,
            ScheduledExecutorService scheduler,
            Ticker ticker,
            Executor pageBufferClientCallbackExecutor)
    {
        this.resultClient = requireNonNull(resultClient, "resultClient is null");
        this.acknowledgePages = acknowledgePages;
        this.location = requireNonNull(location, "location is null");
        this.asyncPageTransportLocation = requireNonNull(asyncPageTransportLocation, "asyncPageTransportLocation is null");
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
                future == null ? "not scheduled" : "processing request");
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
            // do not terminate if the request is already running to avoid closing pooled connections
            future.cancel(false);
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
                clientCallback.clientFailed(PageBufferClient.this, t);
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

    private synchronized void sendGetResults(DataSize maxResponseSize)
    {
        URI uriBase = asyncPageTransportLocation.orElse(location);
        URI uri = HttpUriBuilder.uriBuilderFrom(uriBase).appendPath(String.valueOf(token)).build();

        ListenableFuture<PagesResponse> resultFuture = resultClient.getResults(token, maxResponseSize);

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
                    synchronized (PageBufferClient.this) {
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
                        resultClient.acknowledgeResultsAsync(result.getNextToken());
                    }

                    for (SerializedPage page : pages) {
                        if (!isChecksumValid(page)) {
                            throw new PrestoException(SERIALIZED_PAGE_CHECKSUM_ERROR, format("Received corrupted serialized page from host %s", HostAddress.fromUri(uri)));
                        }
                    }

                    // add pages:
                    // addPages must be called regardless of whether pages is an empty list because
                    // clientCallback can keep stats of requests and responses. For example, it may
                    // keep track of how often a client returns empty response and adjust request
                    // frequency or buffer size.
                    if (clientCallback.addPages(PageBufferClient.this, pages)) {
                        pagesReceived.addAndGet(pages.size());
                        rowsReceived.addAndGet(pages.stream().mapToLong(SerializedPage::getPositionCount).sum());
                    }
                    else {
                        pagesRejected.addAndGet(pages.size());
                        rowsRejected.addAndGet(pages.stream().mapToLong(SerializedPage::getPositionCount).sum());
                    }
                }
                catch (PrestoException e) {
                    handleFailure(e, resultFuture);
                    return;
                }

                synchronized (PageBufferClient.this) {
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
                clientCallback.requestComplete(PageBufferClient.this);
            }

            @Override
            public void onFailure(Throwable t)
            {
                log.debug("Request to %s failed %s", uri, t);
                checkNotHoldsLock(this);

                t = resultClient.rewriteException(t);
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
        ListenableFuture<?> resultFuture = resultClient.abortResults();
        future = resultFuture;
        Futures.addCallback(resultFuture, new FutureCallback<Object>()
        {
            @Override
            public void onSuccess(@Nullable Object result)
            {
                checkNotHoldsLock(this);
                backoff.success();
                synchronized (PageBufferClient.this) {
                    closed = true;
                    if (future == resultFuture) {
                        future = null;
                    }
                    lastUpdate = DateTime.now();
                }
                requestsCompleted.incrementAndGet();
                clientCallback.clientFinished(PageBufferClient.this);
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

    private void handleFailure(Throwable t, ListenableFuture<?> expectedFuture)
    {
        // Can not delegate to other callback while holding a lock on this
        checkNotHoldsLock(this);

        requestsFailed.incrementAndGet();
        requestsCompleted.incrementAndGet();

        if (t instanceof PrestoException) {
            clientCallback.clientFailed(PageBufferClient.this, t);
        }

        synchronized (PageBufferClient.this) {
            if (future == expectedFuture) {
                future = null;
            }
            lastUpdate = DateTime.now();
        }
        clientCallback.requestComplete(PageBufferClient.this);
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

        PageBufferClient that = (PageBufferClient) o;

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
