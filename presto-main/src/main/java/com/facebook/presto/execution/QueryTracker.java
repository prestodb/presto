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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryTracker.TrackedQuery;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.SystemSessionProperties.getQueryMaxExecutionTime;
import static com.facebook.presto.SystemSessionProperties.getQueryMaxRunTime;
import static com.facebook.presto.spi.StandardErrorCode.ABANDONED_QUERY;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_TIME_LIMIT;
import static com.facebook.presto.spi.StandardErrorCode.SERVER_SHUTTING_DOWN;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class QueryTracker<T extends TrackedQuery>
{
    private static final Logger log = Logger.get(QueryTracker.class);

    private final int maxQueryHistory;
    private final Duration minQueryExpireAge;

    private final ConcurrentMap<QueryId, T> queries = new ConcurrentHashMap<>();
    private final Queue<T> expirationQueue = new LinkedBlockingQueue<>();

    private final Duration clientTimeout;

    private final ScheduledExecutorService queryManagementExecutor;

    @GuardedBy("this")
    private ScheduledFuture<?> backgroundTask;

    public QueryTracker(QueryManagerConfig queryManagerConfig, ScheduledExecutorService queryManagementExecutor)
    {
        requireNonNull(queryManagerConfig, "queryManagerConfig is null");
        this.minQueryExpireAge = queryManagerConfig.getMinQueryExpireAge();
        this.maxQueryHistory = queryManagerConfig.getMaxQueryHistory();
        this.clientTimeout = queryManagerConfig.getClientTimeout();

        this.queryManagementExecutor = requireNonNull(queryManagementExecutor, "queryManagementExecutor is null");
    }

    public synchronized void start()
    {
        checkState(backgroundTask == null, "QueryTracker already started");
        backgroundTask = queryManagementExecutor.scheduleWithFixedDelay(() -> {
            try {
                failAbandonedQueries();
            }
            catch (Throwable e) {
                log.error(e, "Error cancelling abandoned queries");
            }

            try {
                enforceTimeLimits();
            }
            catch (Throwable e) {
                log.error(e, "Error enforcing query timeout limits");
            }

            try {
                removeExpiredQueries();
            }
            catch (Throwable e) {
                log.error(e, "Error removing expired queries");
            }

            try {
                pruneExpiredQueries();
            }
            catch (Throwable e) {
                log.error(e, "Error pruning expired queries");
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    public void stop()
    {
        synchronized (this) {
            if (backgroundTask != null) {
                backgroundTask.cancel(true);
            }
        }

        boolean queryCancelled = false;
        for (T trackedQuery : queries.values()) {
            if (trackedQuery.isDone()) {
                continue;
            }

            log.info("Server shutting down. Query %s has been cancelled", trackedQuery.getQueryId());
            trackedQuery.fail(new PrestoException(SERVER_SHUTTING_DOWN, "Server is shutting down. Query " + trackedQuery.getQueryId() + " has been cancelled"));
            queryCancelled = true;
        }
        if (queryCancelled) {
            try {
                TimeUnit.SECONDS.sleep(5);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public Collection<T> getAllQueries()
    {
        return ImmutableList.copyOf(queries.values());
    }

    public T getQuery(QueryId queryId)
            throws NoSuchElementException
    {
        return tryGetQuery(queryId)
                .orElseThrow(NoSuchElementException::new);
    }

    public Optional<T> tryGetQuery(QueryId queryId)
    {
        requireNonNull(queryId, "queryId is null");
        return Optional.ofNullable(queries.get(queryId));
    }

    public boolean addQuery(T execution)
    {
        return queries.putIfAbsent(execution.getQueryId(), execution) == null;
    }

    /**
     * Query is finished and expiration should begin.
     */
    public void expireQuery(QueryId queryId)
    {
        tryGetQuery(queryId)
                .ifPresent(expirationQueue::add);
    }

    /**
     * Enforce query max runtime/execution time limits
     */
    private void enforceTimeLimits()
    {
        for (T query : queries.values()) {
            if (query.isDone()) {
                continue;
            }
            Duration queryMaxRunTime = getQueryMaxRunTime(query.getSession());
            Duration queryMaxExecutionTime = getQueryMaxExecutionTime(query.getSession());
            Optional<DateTime> executionStartTime = query.getExecutionStartTime();
            DateTime createTime = query.getCreateTime();
            if (executionStartTime.isPresent() && executionStartTime.get().plus(queryMaxExecutionTime.toMillis()).isBeforeNow()) {
                query.fail(new PrestoException(EXCEEDED_TIME_LIMIT, "Query exceeded the maximum execution time limit of " + queryMaxExecutionTime));
            }
            if (createTime.plus(queryMaxRunTime.toMillis()).isBeforeNow()) {
                query.fail(new PrestoException(EXCEEDED_TIME_LIMIT, "Query exceeded maximum time limit of " + queryMaxRunTime));
            }
        }
    }

    /**
     * Prune extraneous info from old queries
     */
    private void pruneExpiredQueries()
    {
        if (expirationQueue.size() <= maxQueryHistory) {
            return;
        }

        int count = 0;
        // we're willing to keep full info for up to maxQueryHistory queries
        for (T query : expirationQueue) {
            if (expirationQueue.size() - count <= maxQueryHistory) {
                break;
            }
            query.pruneInfo();
            count++;
        }
    }

    /**
     * Remove completed queries after a waiting period
     */
    private void removeExpiredQueries()
    {
        DateTime timeHorizon = DateTime.now().minus(minQueryExpireAge.toMillis());

        // we're willing to keep queries beyond timeHorizon as long as we have fewer than maxQueryHistory
        while (expirationQueue.size() > maxQueryHistory) {
            T query = expirationQueue.peek();
            if (query == null) {
                return;
            }

            // expirationQueue is FIFO based on query end time. Stop when we see the
            // first query that's too young to expire
            Optional<DateTime> endTime = query.getEndTime();
            if (!endTime.isPresent()) {
                // this shouldn't happen but it is better to be safe here
                continue;
            }
            if (endTime.get().isAfter(timeHorizon)) {
                return;
            }

            // only expire them if they are older than minQueryExpireAge. We need to keep them
            // around for a while in case clients come back asking for status
            QueryId queryId = query.getQueryId();

            log.debug("Remove query %s", queryId);
            queries.remove(queryId);
            expirationQueue.remove(query);
        }
    }

    private void failAbandonedQueries()
    {
        for (T query : queries.values()) {
            try {
                // TODO: This fix is a workaround, and must be removed once the better fix is provided.
                // Issue is tracked at https://github.com/prestodb/presto/issues/11844
                //
                // getQueryInfo has a side effect. It sets the final query info if all the stageInfo's are final.
                //
                // Setting final query info is required to have the finalQueryInfoListener triggered.
                // The code that places the query into the eviction queue is the finalQueryInfoListener (see SqlQueryManager#createQueryInternal).
                //
                // If query is failed with SqlQueryExecution#fail or with SqlQueryExecution#cancelQuery the final query info must be
                // set when all the query stages respond with the final stage info.
                //
                // Currently the callback registered on all the stages in the SqlQueryScheduler's constructor doesn't set the final query info,
                // as due to the race, the stageInfo's in the moment of processing that callback are not final.
                if (query instanceof QueryExecution) {
                    QueryExecution execution = (QueryExecution) query;
                    execution.getQueryInfo();
                }

                if (query.isDone()) {
                    continue;
                }

                if (isAbandoned(query)) {
                    log.info("Failing abandoned query %s", query.getQueryId());
                    query.fail(new PrestoException(
                            ABANDONED_QUERY,
                            format("Query %s has not been accessed since %s: currentTime %s",
                                    query.getQueryId(),
                                    query.getLastHeartbeat(),
                                    DateTime.now())));
                }
            }
            catch (RuntimeException e) {
                log.error(e, "Exception failing abandoned query %s", query.getQueryId());
            }
        }
    }

    private boolean isAbandoned(T query)
    {
        DateTime oldestAllowedHeartbeat = DateTime.now().minus(clientTimeout.toMillis());
        DateTime lastHeartbeat = query.getLastHeartbeat();

        return lastHeartbeat != null && lastHeartbeat.isBefore(oldestAllowedHeartbeat);
    }

    public interface TrackedQuery
    {
        QueryId getQueryId();

        boolean isDone();

        Session getSession();

        DateTime getCreateTime();

        Optional<DateTime> getExecutionStartTime();

        DateTime getLastHeartbeat();

        Optional<DateTime> getEndTime();

        void fail(Throwable cause);

        // XXX: This should be removed when the client protocol is improved, so that we don't need to hold onto so much query history
        void pruneInfo();
    }
}
