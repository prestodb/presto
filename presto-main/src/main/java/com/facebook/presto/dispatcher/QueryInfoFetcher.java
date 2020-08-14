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
package com.facebook.presto.dispatcher;

import com.facebook.airlift.concurrent.SetThreadName;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.RequestErrorTracker;
import com.facebook.presto.server.SimpleHttpResponseCallback;
import com.facebook.presto.server.SimpleHttpResponseHandler;
import com.facebook.presto.server.smile.BaseResponse;
import com.facebook.presto.server.smile.Codec;
import com.facebook.presto.server.smile.SmileCodec;
import com.facebook.presto.spi.QueryId;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.ws.rs.core.UriBuilder;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CURRENT_STATE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_WAIT;
import static com.facebook.presto.server.RequestErrorTracker.queryRequestErrorTracker;
import static com.facebook.presto.server.RequestHelpers.setContentTypeHeaders;
import static com.facebook.presto.server.smile.AdaptingJsonResponseHandler.createAdaptingJsonResponseHandler;
import static com.facebook.presto.server.smile.FullSmileResponseHandler.createFullSmileResponseHandler;
import static com.facebook.presto.server.smile.JsonCodecWrapper.unwrapJsonCodec;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_QUERY_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class QueryInfoFetcher
        implements SimpleHttpResponseCallback<BasicQueryInfo>
{
    private final QueryId queryId;
    private final Consumer<Throwable> onFail;
    private final StateMachine<BasicQueryInfo> queryInfo;
    private final StateMachine<Optional<BasicQueryInfo>> finalQueryInfo;
    private final Codec<BasicQueryInfo> queryInfoCodec;

    private final long updateIntervalMillis;
    private final Duration queryInfoRefreshMaxWait;
    private final AtomicLong lastUpdateNanos = new AtomicLong();
    private final Duration maxErrorDuration;

    private final HttpClient httpClient;
    private final Executor executor;
    private final ScheduledExecutorService updateScheduledExecutor;
    private final ScheduledExecutorService errorScheduledExecutor;
    private final RemoteQueryStats remoteQueryStats;
    private final LocationFactory httpLocationFactory;
    private final boolean isBinaryTransportEnabled;

    @GuardedBy("this")
    private ScheduledFuture<?> scheduledFuture;

    @GuardedBy("this")
    private final AtomicLong currentRequestStartNanos = new AtomicLong();

    @GuardedBy("this")
    private boolean running;

    @GuardedBy("this")
    private boolean stopped;

    @GuardedBy("this")
    private ListenableFuture<BaseResponse<BasicQueryInfo>> future;

    @GuardedBy("this")
    private RequestErrorTracker errorTracker;

    public QueryInfoFetcher(
            Consumer<Throwable> onFail,
            QueryId queryId,
            HttpClient httpClient,
            BasicQueryInfo initialQueryInfo,
            Duration queryInfoRefreshMaxWait,
            Duration updateInterval,
            Codec<BasicQueryInfo> queryInfoCodec,
            Duration maxErrorDuration,
            Executor executor,
            ScheduledExecutorService updateScheduledExecutor,
            ScheduledExecutorService errorScheduledExecutor,
            RemoteQueryStats remoteQueryStats,
            LocationFactory httpLocationFactory,
            boolean isBinaryTransportEnabled)
    {
        this.onFail = requireNonNull(onFail, "onFail is null");
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.queryInfo = new StateMachine<>("task " + queryId, executor, initialQueryInfo);
        this.finalQueryInfo = new StateMachine<>("query-" + queryId, executor, Optional.empty());
        this.queryInfoCodec = requireNonNull(queryInfoCodec, "queryInfoCodec is null");

        this.queryInfoRefreshMaxWait = requireNonNull(queryInfoRefreshMaxWait, "queryInfoRefreshMaxWait is null");
        this.updateIntervalMillis = requireNonNull(updateInterval, "updateInterval is null").toMillis();
        this.maxErrorDuration = requireNonNull(maxErrorDuration, "maxErrorDuration is null");

        this.executor = requireNonNull(executor, "executor is null");
        this.updateScheduledExecutor = requireNonNull(updateScheduledExecutor, "updateScheduledExecutor is null");
        this.errorScheduledExecutor = requireNonNull(errorScheduledExecutor, "errorScheduledExecutor is null");
        this.remoteQueryStats = requireNonNull(remoteQueryStats, "remoteQueryStats is null");
        this.httpLocationFactory = requireNonNull(httpLocationFactory, "httpLocationFactory is null");
        this.isBinaryTransportEnabled = isBinaryTransportEnabled;
    }

    public BasicQueryInfo getQueryInfo()
    {
        return queryInfo.get();
    }

    public synchronized void start(InternalNode coordinator)
    {
        checkState(!running, "Already running");
        checkState(!stopped, "Already stopped");
        requireNonNull(coordinator, "coordinator is null");
        checkArgument(coordinator.isCoordinator(), "node %s is not a coordinator", coordinator.getInternalUri());
        URI location = httpLocationFactory.createQueryLocation(coordinator, queryId);
        location = UriBuilder.fromUri(location).queryParam("basic", true).build();
        this.errorTracker = queryRequestErrorTracker(queryId, location, maxErrorDuration, errorScheduledExecutor);
        scheduleUpdate(location);
        running = true;
    }

    synchronized void stop()
    {
        running = false;
        stopped = true;
        if (future != null) {
            // do not terminate if the request is already running to avoid closing pooled connections
            future.cancel(false);
            future = null;
        }
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
            scheduledFuture = null;
        }
    }

    /**
     * Add a listener for the final task info.  This notification is guaranteed to be fired only once.
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    public void addFinalQueryInfoListener(StateMachine.StateChangeListener<BasicQueryInfo> stateChangeListener)
    {
        AtomicBoolean done = new AtomicBoolean();
        StateMachine.StateChangeListener<Optional<BasicQueryInfo>> fireOnceStateChangeListener = finalQueryInfo -> {
            if (finalQueryInfo.isPresent() && done.compareAndSet(false, true)) {
                stateChangeListener.stateChanged(finalQueryInfo.get());
            }
        };
        finalQueryInfo.addStateChangeListener(fireOnceStateChangeListener);
        fireOnceStateChangeListener.stateChanged(finalQueryInfo.get());
    }

    private synchronized void scheduleUpdate(URI location)
    {
        checkState(!running, "Already scheduled");
        scheduledFuture = updateScheduledExecutor.scheduleWithFixedDelay(() -> {
            synchronized (this) {
                // if the previous request still running, don't schedule a new request
                if (future != null && !future.isDone()) {
                    return;
                }
            }
            if (nanosSince(lastUpdateNanos.get()).toMillis() >= updateIntervalMillis) {
                sendNextRequest(location);
            }
        }, 0, 100, MILLISECONDS);
    }

    private synchronized void sendNextRequest(URI location)
    {
        QueryState queryState = getQueryInfo().getState();

        if (!running) {
            return;
        }

        // we already have the final query info
        if (queryState.isDone()) {
            stop();
            return;
        }

        // if we have an outstanding request
        if (future != null && !future.isDone()) {
            return;
        }

        // if throttled due to error, asynchronously wait for timeout and try again
        ListenableFuture<?> errorRateLimit = errorTracker.acquireRequestPermit();
        if (!errorRateLimit.isDone()) {
            errorRateLimit.addListener(() -> sendNextRequest(location), executor);
            return;
        }

        Request.Builder requestBuilder = setContentTypeHeaders(isBinaryTransportEnabled, prepareGet());

        if (queryInfoRefreshMaxWait.toMillis() != 0L) {
            requestBuilder.setHeader(PRESTO_CURRENT_STATE, queryState.toString())
                    .setHeader(PRESTO_MAX_WAIT, queryInfoRefreshMaxWait.toString());
        }

        Request request = requestBuilder.setUri(location).build();

        ResponseHandler responseHandler;
        if (isBinaryTransportEnabled) {
            responseHandler = createFullSmileResponseHandler((SmileCodec<BasicQueryInfo>) queryInfoCodec);
        }
        else {
            responseHandler = createAdaptingJsonResponseHandler(unwrapJsonCodec(queryInfoCodec));
        }

        errorTracker.startRequest();
        future = httpClient.executeAsync(request, responseHandler);
        currentRequestStartNanos.set(System.nanoTime());

        Futures.addCallback(
                future,
                new SimpleHttpResponseHandler<>(this, request.getUri(), remoteQueryStats.getHttpResponseStats(), REMOTE_QUERY_ERROR),
                executor);
    }

    private void updateQueryInfo(BasicQueryInfo newValue)
    {
        boolean updated = queryInfo.setIf(newValue, oldValue -> {
            QueryState newState = newValue.getState();
            QueryState oldState = oldValue.getState();
            return !newState.equals(oldState);
        });

        if (updated && newValue.getState().isDone()) {
            finalQueryInfo.compareAndSet(Optional.empty(), Optional.of(newValue));
            stop();
        }
    }

    @Override
    public void success(BasicQueryInfo newValue)
    {
        try (SetThreadName ignored = new SetThreadName("QueryInfoFetcher-%s", queryId)) {
            lastUpdateNanos.set(System.nanoTime());

            long startNanos;
            synchronized (this) {
                startNanos = this.currentRequestStartNanos.get();
            }

            updateStats(startNanos);
            errorTracker.requestSucceeded();
            updateQueryInfo(newValue);
        }
    }

    @Override
    public void failed(Throwable cause)
    {
        try (SetThreadName ignored = new SetThreadName("QueryInfoFetcher-%s", queryId)) {
            lastUpdateNanos.set(System.nanoTime());

            try {
                // if task not already done, record error
                if (!getQueryInfo().getState().isDone()) {
                    errorTracker.requestFailed(cause);
                }
            }
            catch (Error e) {
                onFail.accept(e);
                stop();
                throw e;
            }
            catch (RuntimeException e) {
                onFail.accept(e);
                stop();
            }
            catch (Throwable t) {
                stop();
                throw t;
            }
        }
    }

    @Override
    public void fatal(Throwable t)
    {
        try (SetThreadName ignored = new SetThreadName("QueryInfoFetcher-%s", queryId)) {
            onFail.accept(t);
        }
    }

    private void updateStats(long currentRequestStartNanos)
    {
        remoteQueryStats.infoRoundTripMillis(nanosSince(currentRequestStartNanos).toMillis());
    }
}
