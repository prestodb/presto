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
package com.facebook.presto.server.remotetask;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.scheduler.DomainRuntimeFilter;
import com.facebook.presto.execution.scheduler.DynamicFilterService;
import com.facebook.presto.execution.scheduler.DynamicFilterServiceStats;
import com.facebook.presto.execution.scheduler.JoinDynamicFilter;
import com.facebook.presto.execution.scheduler.RuntimeFilter;
import com.facebook.presto.server.RequestErrorTracker;
import com.facebook.presto.server.SimpleHttpResponseCallback;
import com.facebook.presto.server.SimpleHttpResponseHandler;
import com.facebook.presto.server.smile.BaseResponse;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.channel.EventLoop;

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.Request.Builder.prepareDelete;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_WAIT;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_COMPLETED_ID_DELIVERED;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_FETCHERS_STARTED;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_FETCHER_FINAL_FETCH_COMPLETED;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_FETCHER_POLLS;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_FETCHER_STOPPED_BY_CLEANUP;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PARTITIONS_RECEIVED_FROM_TASK;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static com.facebook.presto.server.RequestErrorTracker.taskRequestErrorTracker;
import static com.facebook.presto.server.smile.AdaptingJsonResponseHandler.createAdaptingJsonResponseHandler;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Long-polls {@code GET /v1/task/{taskId}/dynamicFilters?since=N} to collect
 * dynamic filters from build-side workers. Sends {@code DELETE ?through=N}
 * after processing to free worker memory.
 */
public class DynamicFilterFetcher
        implements SimpleHttpResponseCallback<DynamicFilterResponse>
{
    private static final Logger log = Logger.get(DynamicFilterFetcher.class);

    private final TaskId taskId;
    private final QueryId queryId;
    private final URI taskLocation;
    private final HttpClient httpClient;
    private final EventLoop taskEventLoop;
    private final JsonCodec<DynamicFilterResponse> filterCodec;
    private final RemoteTaskStats stats;
    private final DynamicFilterServiceStats dynamicFilterStats;
    private final RequestErrorTracker errorTracker;
    private final DynamicFilterService dynamicFilterService;
    private final boolean extendedMetrics;
    private final String taskSuffix;

    private final Consumer<Throwable> onFatal;

    private final AtomicLong lastFetchedVersion = new AtomicLong(0);
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(true);
    // Tracks filter IDs already delivered to JoinDynamicFilter.addPartitionByFilterId().
    // Entries are never removed: a filter ID is delivered at most once per fetcher instance
    // (one fetcher per build-side task), enforcing the single-contribution-per-task contract
    // of JoinDynamicFilter's partition counting.
    private final Set<String> deliveredFilterIds = new HashSet<>();
    // Tracks filter IDs that this task has reported in at least one response (via the filters
    // map or completedFilterIds). Used to scope the final-fetch failure fallback: we must only
    // deliver all() for filters this task actually owns, not for every query filter that was
    // pre-loaded into filterCache from getAllFiltersForQuery().
    private final Set<String> ownedFilterIds = new HashSet<>();
    private final Map<String, JoinDynamicFilter> filterCache = new HashMap<>();
    private final Duration maxWait;

    private volatile boolean isFinalFetch;
    private volatile ListenableFuture<BaseResponse<DynamicFilterResponse>> future;
    private long currentRequestStartNanos;

    public DynamicFilterFetcher(
            TaskId taskId,
            URI taskLocation,
            HttpClient httpClient,
            EventLoop taskEventLoop,
            Duration maxErrorDuration,
            Duration maxWait,
            RemoteTaskStats stats,
            JsonCodec<DynamicFilterResponse> filterCodec,
            DynamicFilterService dynamicFilterService,
            QueryId queryId,
            DynamicFilterServiceStats dynamicFilterStats,
            boolean extendedMetrics,
            Consumer<Throwable> onFatal)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.taskLocation = requireNonNull(taskLocation, "taskLocation is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.taskEventLoop = requireNonNull(taskEventLoop, "taskEventLoop is null");
        this.maxWait = requireNonNull(maxWait, "maxWait is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.dynamicFilterStats = requireNonNull(dynamicFilterStats, "dynamicFilterStats is null");
        this.filterCodec = requireNonNull(filterCodec, "filterCodec is null");
        this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
        this.extendedMetrics = extendedMetrics;
        this.onFatal = requireNonNull(onFatal, "onFatal is null");
        this.taskSuffix = taskId.getStageExecutionId().getStageId().getId() + "." + taskId.getId();
        this.errorTracker = taskRequestErrorTracker(
                taskId,
                taskLocation,
                maxErrorDuration,
                taskEventLoop,
                "fetching dynamic filters for task");
    }

    public void start()
    {
        verify(started.compareAndSet(false, true), "start() already called");
        dynamicFilterStats.getFetchersStarted().update(1);
        dynamicFilterService.getAllFiltersForQuery(queryId).forEach(filterCache::putIfAbsent);
        filterCache.values().stream()
                .findFirst()
                .map(JoinDynamicFilter::getRuntimeStats)
                .ifPresent(rs -> rs.addMetricValue(DYNAMIC_FILTER_FETCHERS_STARTED, NONE, 1));
        taskEventLoop.execute(this::sendFetchRequest);
    }

    private void sendFetchRequest()
    {
        verify(taskEventLoop.inEventLoop());

        if (!running.get()) {
            return;
        }

        if (future != null && !future.isDone()) {
            return;
        }

        ListenableFuture<?> errorRateLimit = errorTracker.acquireRequestPermit();
        if (!errorRateLimit.isDone()) {
            errorRateLimit.addListener(this::sendFetchRequest, taskEventLoop);
            return;
        }

        long currentVersion = lastFetchedVersion.get();
        URI uri = uriBuilderFrom(taskLocation)
                .appendPath("dynamicFilters")
                .addParameter("since", String.valueOf(currentVersion))
                .build();

        Request request = prepareGet()
                .setUri(uri)
                .setHeader(PRESTO_MAX_WAIT, maxWait.toString())
                .build();

        // Note: errorTracker.startRequest() is paired with errorTracker.requestSucceeded() in
        // success() or errorTracker.requestFailed() in failed(). The final fetch path
        // (sendFinalFetchRequest) intentionally skips startRequest() because the final fetch is
        // a one-shot best-effort collection and should not count against the error budget.
        errorTracker.startRequest();
        future = httpClient.executeAsync(request, createAdaptingJsonResponseHandler(filterCodec));
        currentRequestStartNanos = System.nanoTime();

        SimpleHttpResponseHandler<DynamicFilterResponse> callback = new SimpleHttpResponseHandler<>(
                this,
                request.getUri(),
                stats.getHttpResponseStats(),
                REMOTE_TASK_ERROR);

        Futures.addCallback(future, callback, taskEventLoop);
    }

    @Override
    public void success(DynamicFilterResponse response)
    {
        verify(taskEventLoop.inEventLoop());

        long requestDurationNanos = System.nanoTime() - currentRequestStartNanos;
        long roundTripMs = requestDurationNanos / 1_000_000;
        stats.infoRoundTripMillis(roundTripMs);
        dynamicFilterStats.recordFilterFetchRoundTripMillis(roundTripMs);
        errorTracker.requestSucceeded();

        long responseVersion = response.getVersion();
        lastFetchedVersion.updateAndGet(current -> Math.max(current, responseVersion));

        Map<String, RuntimeFilter> filters = response.getFilters();
        dynamicFilterStats.getFilterFetchSuccess().update(1);

        if (extendedMetrics) {
            emitExtendedMetric(format("%s[%s]", DYNAMIC_FILTER_FETCHER_POLLS, taskSuffix), 1);
            for (String filterId : filters.keySet()) {
                emitExtendedMetric(format("%s[%s][%s]", DYNAMIC_FILTER_PARTITIONS_RECEIVED_FROM_TASK, filterId, taskSuffix), 1);
            }
            // operatorCompleted is set by the native Presto worker when the HashBuild operator
            // finishes. Java workers never set this field (dynamicFilters is always empty on the
            // Java side), so this metric will only fire in Prestissimo deployments.
            if (isFinalFetch && response.isOperatorCompleted()) {
                emitExtendedMetric(format("%s[%s]", DYNAMIC_FILTER_FETCHER_FINAL_FETCH_COMPLETED, taskSuffix), 1);
            }
        }

        if (!filters.isEmpty()) {
            dynamicFilterStats.getFiltersCollected().update(filters.size());

            for (Map.Entry<String, RuntimeFilter> entry : filters.entrySet()) {
                String filterId = entry.getKey();
                RuntimeFilter filterDomain = entry.getValue();
                ownedFilterIds.add(filterId);
                if (deliveredFilterIds.add(filterId)) {
                    resolveFilter(filterId)
                            .ifPresent(f -> f.addPartitionByFilterId(filterDomain));
                }
                else {
                    // The native worker must follow a publish-once-when-complete contract:
                    // each filter ID is emitted only after the HashBuild operator has fully
                    // resolved it. Receiving the same ID a second time indicates a protocol
                    // violation — log a warning so it is visible in operator diagnostics.
                    log.warn("DynamicFilterFetcher: filter ID '%s' received more than once from task %s — ignoring duplicate", filterId, taskId);
                }
            }

            sendDeleteRequest(responseVersion);
        }

        // Empty build: deliver none() so this task's partition counts toward quorum.
        for (String filterId : response.getCompletedFilterIds()) {
            ownedFilterIds.add(filterId);
            if (deliveredFilterIds.add(filterId)) {
                if (extendedMetrics) {
                    emitExtendedMetric(format("%s[%s][%s]", DYNAMIC_FILTER_COMPLETED_ID_DELIVERED, filterId, taskSuffix), 1);
                }
                resolveFilter(filterId)
                        .ifPresent(f -> f.addPartitionByFilterId(new DomainRuntimeFilter(TupleDomain.none())));
            }
        }

        // operatorCompleted is computed by the native (Prestissimo/Velox) worker in
        // PrestoTask::snapshotDynamicFilters: it is true when all registered build-side filter IDs
        // have been flushed to the coordinator, or the Velox task has terminated.
        // Java workers never set this field — TaskResource on the Java side always returns false;
        // the write path exists only in the native worker implementation.
        // isFinalFetch is the Java-side equivalent: set by stopAfterFinalFetch() when the
        // coordinator knows the build stage is done and one last poll is sufficient.
        if (response.isOperatorCompleted() || isFinalFetch) {
            dynamicFilterStats.getFilterFlushes().update(1);
            stop();
            return;
        }

        scheduleNextPoll();
    }

    private void sendDeleteRequest(long throughVersion)
    {
        URI uri = uriBuilderFrom(taskLocation)
                .appendPath("dynamicFilters")
                .addParameter("through", String.valueOf(throughVersion))
                .build();
        Request request = prepareDelete()
                .setUri(uri)
                .build();
        httpClient.executeAsync(request, createStatusResponseHandler());
    }

    private void scheduleNextPoll()
    {
        verify(taskEventLoop.inEventLoop());
        if (!running.get() || isFinalFetch) {
            return;
        }
        // Already on the event loop — call directly rather than re-enqueuing, so
        // stopAfterFinalFetch() (enqueued externally) cannot slip in between the
        // scheduleNextPoll call and the actual sendFetchRequest execution.
        sendFetchRequest();
    }

    @Override
    public void failed(Throwable cause)
    {
        verify(taskEventLoop.inEventLoop());

        // The final fetch future is cancelled by abort() if the query fails while the final
        // fetch is in-flight. Treat CancellationException on a final fetch as a clean stop
        // rather than a retriable error — there is nothing to retry at this point.
        if (isFinalFetch && cause instanceof CancellationException) {
            stop();
            return;
        }

        dynamicFilterStats.getFilterFetchFailure().update(1);
        if (extendedMetrics) {
            emitExtendedMetric(format("dynamicFilterFetcherFailed[%s]", taskSuffix), 1);
        }

        if (isFinalFetch) {
            // The final fetch failed for a non-cancellation reason. Log a warning and deliver
            // TupleDomain.all() for any undelivered filters so their JoinDynamicFilter reaches
            // quorum rather than waiting indefinitely.
            //
            // Scope the fallback to ownedFilterIds — filter IDs that this task actually reported
            // in at least one response. filterCache is pre-populated with ALL query filters from
            // getAllFiltersForQuery(), so iterating it would deliver a spurious all() partition to
            // filters owned by other build stages/tasks, potentially resolving them to all() early
            // and disabling pruning even though their own build tasks succeeded.
            log.warn(cause, "Final dynamic filter fetch failed for task %s; undelivered filters will not be pruned", taskId);
            for (String filterId : ownedFilterIds) {
                if (deliveredFilterIds.add(filterId)) {
                    JoinDynamicFilter filter = filterCache.get(filterId);
                    if (filter != null) {
                        filter.addPartitionByFilterId(new DomainRuntimeFilter(TupleDomain.all()));
                    }
                }
            }
            stop();
            return;
        }

        try {
            errorTracker.requestFailed(cause);
        }
        catch (PrestoException e) {
            stop();
            onFatal.accept(e);
            return;
        }

        scheduleNextPoll();
    }

    @Override
    public void fatal(Throwable cause)
    {
        verify(taskEventLoop.inEventLoop());
        // Propagate rather than swallowing — a fatal parse error should fail the query, not silently time out the filter.
        dynamicFilterStats.getFilterFetchFailure().update(1);
        if (extendedMetrics) {
            emitExtendedMetric(format("dynamicFilterFetcherFatal[%s]", taskSuffix), 1);
        }
        stop();
        onFatal.accept(cause);
    }

    /**
     * Stop polling. Pending requests are allowed to complete so late-arriving filters
     * are still processed.
     */
    public void stop()
    {
        running.set(false);
    }

    /**
     * Stop polling after one final immediate fetch to collect filter data from fast-completing
     * tasks. Must be called from the event loop.
     */
    public void stopAfterFinalFetch()
    {
        verify(taskEventLoop.inEventLoop());
        // Set isFinalFetch before running=false so that any scheduleNextPoll task
        // already enqueued on the event loop sees isFinalFetch=true and bails out.
        isFinalFetch = true;
        if (running.compareAndSet(true, false)) {
            if (extendedMetrics) {
                emitExtendedMetric(format("%s[%s]", DYNAMIC_FILTER_FETCHER_STOPPED_BY_CLEANUP, taskSuffix), 1);
            }
            // Cancel any in-flight regular poll so its success() callback does not
            // call scheduleNextPoll and fire another request before the final fetch lands.
            ListenableFuture<BaseResponse<DynamicFilterResponse>> inflight = future;
            if (inflight != null && !inflight.isDone()) {
                inflight.cancel(false);
            }
            sendFinalFetchRequest();
        }
    }

    private void sendFinalFetchRequest()
    {
        verify(taskEventLoop.inEventLoop());

        long currentVersion = lastFetchedVersion.get();
        URI uri = uriBuilderFrom(taskLocation)
                .appendPath("dynamicFilters")
                .addParameter("since", String.valueOf(currentVersion))
                .build();

        // No PRESTO_MAX_WAIT header — returns immediately with whatever data is available
        Request request = prepareGet()
                .setUri(uri)
                .build();

        future = httpClient.executeAsync(request, createAdaptingJsonResponseHandler(filterCodec));
        currentRequestStartNanos = System.nanoTime();

        Futures.addCallback(
                future,
                new SimpleHttpResponseHandler<>(this, request.getUri(), stats.getHttpResponseStats(), REMOTE_TASK_ERROR),
                taskEventLoop);
    }

    /** Cancels pending HTTP requests and stops polling. */
    public void abort()
    {
        running.set(false);
        // Do not cancel the final fetch — it must complete so the last partition is delivered.
        if (isFinalFetch) {
            return;
        }
        ListenableFuture<BaseResponse<DynamicFilterResponse>> pendingFuture = future;
        if (pendingFuture != null && !pendingFuture.isDone()) {
            pendingFuture.cancel(false);
        }
    }

    private Optional<JoinDynamicFilter> resolveFilter(String filterId)
    {
        JoinDynamicFilter existing = filterCache.get(filterId);
        if (existing != null) {
            return Optional.of(existing);
        }
        Optional<JoinDynamicFilter> filter = dynamicFilterService.getFilter(queryId, filterId);
        filter.ifPresent(f -> filterCache.put(filterId, f));
        return filter;
    }

    private void emitExtendedMetric(String metricName, long value)
    {
        filterCache.values().stream()
                .findFirst()
                .map(JoinDynamicFilter::getRuntimeStats)
                .ifPresent(runtimeStats -> runtimeStats.addMetricValue(metricName, NONE, value));
    }

    public TaskId getTaskId()
    {
        return taskId;
    }
}
