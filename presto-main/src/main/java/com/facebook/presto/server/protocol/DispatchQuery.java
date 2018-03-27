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
package com.facebook.presto.server.protocol;

import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.server.ForStatementResource;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.QueryId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.ws.rs.core.UriInfo;

import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.util.Failures.toFailure;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.addTimeout;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class DispatchQuery
        extends AbstractQuery
{
    private static final Logger log = Logger.get(DispatchQuery.class);

    @Nullable
    @GuardedBy("this")
    private QueryResults redirectResults;

    @GuardedBy("this")
    private long updateCount;

    static DispatchQuery create(
            QueryId queryId,
            SessionContext sessionContext,
            String query,
            QueryManager queryManager,
            SessionPropertyManager sessionPropertyManager,
            Executor dataProcessorExecutor,
            ScheduledExecutorService timeoutExecutor)
    {
        DispatchQuery result = new DispatchQuery(queryId, sessionContext, query, queryManager, sessionPropertyManager, dataProcessorExecutor, timeoutExecutor);

        // set redirect results obtained from the coordinator
        result.queryManager.addRedirectResultsListener(result.getQueryId(), queryResults -> result.redirectResults = requireNonNull(queryResults, "queryResults is null"));

        return result;
    }

    private DispatchQuery(
            QueryId queryId,
            SessionContext sessionContext,
            String query,
            QueryManager queryManager,
            SessionPropertyManager sessionPropertyManager,
            Executor resultsProcessorExecutor,
            ScheduledExecutorService timeoutExecutor)
    {
        super(queryId, sessionContext, query, queryManager, sessionPropertyManager, resultsProcessorExecutor, timeoutExecutor);
    }

    @Override
    public synchronized void dispose()
    {
        // no-op
    }

    @Override
    public synchronized ListenableFuture<QueryResults> waitForResults(OptionalLong token, UriInfo uriInfo, Duration wait)
    {
        // before waiting, check if this request has already been processed and cached
        if (redirectResults != null) {
            return immediateFuture(redirectResults);
        }

        // wait for a results data or query to finish, up to the wait timeout
        ListenableFuture<?> futureStateChange = addTimeout(
                getFutureStateChange(),
                () -> null,
                wait,
                timeoutExecutor);

        // when state changes, fetch the next result
        return Futures.transform(futureStateChange, ignored -> getRedirectResult(uriInfo), resultsProcessorExecutor);
    }

    private synchronized ListenableFuture<?> getFutureStateChange()
    {
        queryManager.recordHeartbeat(queryId);
        return queryManager.getQueryState(queryId).map(this::queryDoneFuture)
                .orElse(immediateFuture(null));
    }

    private synchronized QueryResults getRedirectResult(UriInfo uriInfo)
    {
        // before waiting, check if this request has already been processed and cached
        if (redirectResults != null) {
            return redirectResults;
        }

        // get the query info before returning
        // force update if query manager is closed
        QueryInfo queryInfo = queryManager.getQueryInfo(queryId);
        queryManager.recordHeartbeat(queryId);

        updateInfo(queryInfo);
        updateCount++;

        // build the next uri to still connecting to the dispatcher
        return new QueryResults(
                queryId.toString(),
                uriInfo.getRequestUriBuilder().replaceQuery(queryId.toString()).replacePath("query.html").build(),
                null,
                uriInfo.getBaseUriBuilder().replacePath("/v1/statement").path(queryId.toString()).path("0").build(),
                ImmutableList.of(),
                null,
                toStatementStats(queryInfo),
                toQueryError(queryInfo),
                queryInfo.getUpdateType(),
                updateCount);
    }

    private ListenableFuture<?> queryDoneFuture(QueryState currentState)
    {
        if (currentState.isDone()) {
            return immediateFuture(null);
        }
        return Futures.transformAsync(queryManager.getStateChange(queryId, currentState), this::queryDoneFuture);
    }

    private static StatementStats toStatementStats(QueryInfo queryInfo)
    {
        QueryStats queryStats = queryInfo.getQueryStats();

        return StatementStats.builder()
                .setState(queryInfo.getState().toString())
                .setQueued(queryInfo.getState() == QueryState.QUEUED)
                .setQueuedTimeMillis(queryStats.getQueuedTime().toMillis())
                .setElapsedTimeMillis(queryStats.getElapsedTime().toMillis())
                .build();
    }

    private static QueryError toQueryError(QueryInfo queryInfo)
    {
        FailureInfo failure = queryInfo.getFailureInfo();
        if (failure == null) {
            QueryState state = queryInfo.getState();
            if ((!state.isDone()) || (state == QueryState.ACKNOWLEDGED)) {
                return null;
            }
            log.warn("Query %s in state %s has no failure info", queryInfo.getQueryId(), state);
            failure = toFailure(new RuntimeException(format("Query is %s (reason unknown)", state))).toFailureInfo();
        }

        ErrorCode errorCode;
        if (queryInfo.getErrorCode() != null) {
            errorCode = queryInfo.getErrorCode();
        }
        else {
            errorCode = GENERIC_INTERNAL_ERROR.toErrorCode();
            log.warn("Failed query %s has no error code", queryInfo.getQueryId());
        }
        return new QueryError(
                failure.getMessage(),
                null,
                errorCode.getCode(),
                errorCode.getName(),
                errorCode.getType().toString(),
                failure.getErrorLocation(),
                failure);
    }

    public static class DispatchQueryFactory
            implements QueryFactory<DispatchQuery>
    {
        private final QueryManager queryManager;
        private final SessionPropertyManager sessionPropertyManager;
        private final Executor dataProcessorExecutor;
        private final ScheduledExecutorService timeoutExecutor;

        @Inject
        DispatchQueryFactory(
                QueryManager queryManager,
                SessionPropertyManager sessionPropertyManager,
                @ForStatementResource BoundedExecutor dataProcessorExecutor,
                @ForStatementResource ScheduledExecutorService timeoutExecutor)
        {
            this.queryManager = requireNonNull(queryManager, "queryManager is null");
            this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
            this.dataProcessorExecutor = requireNonNull(dataProcessorExecutor, "responseExecutor is null");
            this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
        }

        @Override
        public DispatchQuery create(QueryId queryId, String query, SessionContext sessionContext)
        {
            return DispatchQuery.create(
                    queryId,
                    sessionContext,
                    query,
                    queryManager,
                    sessionPropertyManager,
                    dataProcessorExecutor,
                    timeoutExecutor);
        }
    }
}
