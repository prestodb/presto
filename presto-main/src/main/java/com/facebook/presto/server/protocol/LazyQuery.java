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

import com.facebook.presto.Session;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.execution.LazyOutput;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.server.ForStatementResource;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.server.protocol.DispatchQuery.DispatchQueryFactory;
import com.facebook.presto.server.protocol.SqlQuery.SqlQueryFactory;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.transaction.TransactionId;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.ws.rs.core.UriInfo;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.addTimeout;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class LazyQuery
        extends AbstractQuery<LazyOutput>
{
    private final Executor lazyExecutor;
    private final ScheduledExecutorService timeoutExecutor;
    private final SqlQueryFactory sqlQueryFactory;
    private final DispatchQueryFactory dispatchQueryFactory;

    private final SessionContext sessionContext;
    private final String query;

    @GuardedBy("this")
    private Query delegate;

    @GuardedBy("this")
    private SqlQuery sqlDelegate;

    @GuardedBy("this")
    private DispatchQuery dispatchDelegate;

    @GuardedBy("this")
    private boolean initialResultsFetched;

    @GuardedBy("this")
    private long updateCount;

    static LazyQuery create(
            QueryId queryId,
            SessionContext sessionContext,
            String query,
            QueryManager<LazyOutput> queryManager,
            SessionPropertyManager sessionPropertyManager,
            Executor lazyExecutor,
            ScheduledExecutorService timeoutExecutor,
            SqlQueryFactory sqlQueryFactory,
            DispatchQueryFactory dispatchQueryFactory)
    {
        requireNonNull(queryManager, "queryManager is null");
        requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");

        QueryInfo info = queryManager.createQuery(queryId, sessionContext, query);

        return new LazyQuery(
                queryId,
                sessionContext,
                info.getSession().toSession(sessionPropertyManager),
                query,
                queryManager,
                lazyExecutor,
                timeoutExecutor,
                sqlQueryFactory,
                dispatchQueryFactory);
    }

    private LazyQuery(
            QueryId queryId,
            SessionContext sessionContext,
            Session session,
            String query,
            QueryManager<LazyOutput> queryManager,
            Executor lazyExecutor,
            ScheduledExecutorService timeoutExecutor,
            SqlQueryFactory sqlQueryFactory,
            DispatchQueryFactory dispatchQueryFactory)
    {
        super(queryManager, queryId, session);
        this.sessionContext = requireNonNull(sessionContext, "sessionContext is null");
        this.query = requireNonNull(query, "query is null");
        this.lazyExecutor = requireNonNull(lazyExecutor, "redirectProcessorExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
        this.sqlQueryFactory = requireNonNull(sqlQueryFactory, "sqlQueryFactory is null");
        this.dispatchQueryFactory = requireNonNull(dispatchQueryFactory, "dispatchQueryFactory is null");

        queryManager.addOutputListener(getQueryId(), this::setOutput);
    }

    @Override
    public synchronized void dispose()
    {
        if (delegate != null) {
            delegate.dispose();
        }
    }

    @Override
    public synchronized ListenableFuture<QueryResults> waitForResults(OptionalLong token, UriInfo uriInfo, Duration wait)
    {
        if (delegate != null) {
            if (initialResultsFetched) {
                return delegate.waitForResults(token, uriInfo, wait);
            }
            else {
                initialResultsFetched = true;
                return delegate.waitForResults(OptionalLong.empty(), uriInfo, wait);
            }
        }

        // wait for a results data or query to finish, up to the wait timeout
        ListenableFuture<?> futureStateChange = addTimeout(
                getFutureStateChange(),
                () -> null,
                wait,
                timeoutExecutor);

        // the call is only to record heartbeat or throw an error given the query is still queued
        return Futures.transform(futureStateChange, ignored -> getEmptyResults(uriInfo), lazyExecutor);
    }

    @Override
    synchronized void setOutput(LazyOutput lazyOutput)
    {
        if (lazyOutput.isFailed()) {
            return;
        }
        if (!lazyOutput.isRemote()) {
            if (delegate == null) {
                checkState(sqlDelegate == null && dispatchDelegate == null);
                sqlDelegate = sqlQueryFactory.create(queryId, sessionContext, query);
                delegate = sqlDelegate;
            }
            if (lazyOutput.getSqlQueryOutput().isPresent()) {
                sqlDelegate.setOutput(lazyOutput.getSqlQueryOutput().get());
            }
        }
        else {
            if (delegate == null) {
                checkState(sqlDelegate == null && dispatchDelegate == null);
                dispatchDelegate = dispatchQueryFactory.create(queryId, sessionContext, query);
                delegate = dispatchDelegate;
            }
            if (lazyOutput.getDispatchQueryOutput().isPresent()) {
                dispatchDelegate.setOutput(lazyOutput.getDispatchQueryOutput().get());
            }
        }
    }

    @Override
    public synchronized Optional<String> getSetCatalog()
    {
        // getter could be null given updateInfo may not be called for the delegate yet
        if (delegate != null && delegate.getSetCatalog() != null) {
            return delegate.getSetCatalog();
        }
        return super.getSetCatalog();
    }

    @Override
    public synchronized Optional<String> getSetSchema()
    {
        if (delegate != null && delegate.getSetSchema() != null) {
            return delegate.getSetSchema();
        }
        return super.getSetSchema();
    }

    @Override
    public synchronized Map<String, String> getSetSessionProperties()
    {
        if (delegate != null && delegate.getSetSessionProperties() != null) {
            return delegate.getSetSessionProperties();
        }
        return super.getSetSessionProperties();
    }

    @Override
    public synchronized Set<String> getResetSessionProperties()
    {
        if (delegate != null && delegate.getResetSessionProperties() != null) {
            return delegate.getResetSessionProperties();
        }
        return super.getResetSessionProperties();
    }

    @Override
    public synchronized Map<String, String> getAddedPreparedStatements()
    {
        if (delegate != null && delegate.getAddedPreparedStatements() != null) {
            return delegate.getAddedPreparedStatements();
        }
        return super.getAddedPreparedStatements();
    }

    @Override
    public synchronized Set<String> getDeallocatedPreparedStatements()
    {
        if (delegate != null && delegate.getDeallocatedPreparedStatements() != null) {
            return delegate.getDeallocatedPreparedStatements();
        }
        return super.getDeallocatedPreparedStatements();
    }

    @Override
    public synchronized Optional<TransactionId> getStartedTransactionId()
    {
        if (delegate != null && delegate.getStartedTransactionId() != null) {
            return delegate.getStartedTransactionId();
        }
        return super.getStartedTransactionId();
    }

    @Override
    public synchronized boolean isClearTransactionId()
    {
        return delegate != null && delegate.isClearTransactionId();
    }

    private synchronized QueryResults getEmptyResults(UriInfo uriInfo)
    {
        // get the query info before returning
        // force update if query manager is closed
        QueryInfo queryInfo = queryManager.getQueryInfo(queryId);
        queryManager.recordHeartbeat(queryId);

        updateInfo(queryInfo);
        updateCount++;

        // build the next uri to still connecting to the dispatcher
        QueryError queryError = toQueryError(queryInfo, FINISHED);
        return new QueryResults(
                queryId.toString(),
                uriInfo.getRequestUriBuilder().replaceQuery(queryId.toString()).replacePath("query.html").build(),
                null,
                queryError == null ? uriInfo.getBaseUriBuilder().replacePath("/v1/statement").path(queryId.toString()).path("0").build() : null,
                null,
                null,
                toStatementStats(queryInfo),
                queryError,
                queryInfo.getUpdateType(),
                updateCount);
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

    public static class LazyQueryFactory
            implements QueryFactory<LazyQuery>
    {
        private final QueryManager<LazyOutput> queryManager;
        private final SessionPropertyManager sessionPropertyManager;
        private final Executor lazyExecutor;
        private final ScheduledExecutorService timeoutExecutor;
        private final SqlQueryFactory sqlQueryFactory;
        private final DispatchQueryFactory dispatchQueryFactory;

        @Inject
        LazyQueryFactory(
                QueryManager<LazyOutput> queryManager,
                SessionPropertyManager sessionPropertyManager,
                @ForStatementResource BoundedExecutor lazyExecutor,
                @ForStatementResource ScheduledExecutorService timeoutExecutor,
                SqlQueryFactory sqlQueryFactory,
                DispatchQueryFactory dispatchQueryFactory)
        {
            this.queryManager = requireNonNull(queryManager, "queryManager is null");
            this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
            this.lazyExecutor = requireNonNull(lazyExecutor, "lazyExecutor is null");
            this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
            this.sqlQueryFactory = requireNonNull(sqlQueryFactory, "sqlQueryFactory is null");
            this.dispatchQueryFactory = requireNonNull(dispatchQueryFactory, "dispatchQueryFactory is null");
        }

        @Override
        public LazyQuery create(QueryId queryId, SessionContext sessionContext, String query)
        {
            return LazyQuery.create(
                    queryId,
                    sessionContext,
                    query,
                    queryManager,
                    sessionPropertyManager,
                    lazyExecutor,
                    timeoutExecutor,
                    sqlQueryFactory,
                    dispatchQueryFactory);
        }
    }
}
