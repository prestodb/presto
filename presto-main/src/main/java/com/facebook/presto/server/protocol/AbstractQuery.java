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
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.transaction.TransactionId;

import javax.annotation.concurrent.GuardedBy;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

abstract class AbstractQuery
        implements Query
{
    final QueryManager queryManager;
    final Executor resultsProcessorExecutor;
    final ScheduledExecutorService timeoutExecutor;

    final QueryId queryId;
    final Session session;

    @GuardedBy("this")
    private Optional<String> setCatalog;

    @GuardedBy("this")
    private Optional<String> setSchema;

    @GuardedBy("this")
    private Map<String, String> setSessionProperties;

    @GuardedBy("this")
    private Set<String> resetSessionProperties;

    @GuardedBy("this")
    private Map<String, String> addedPreparedStatements;

    @GuardedBy("this")
    private Set<String> deallocatedPreparedStatements;

    @GuardedBy("this")
    private Optional<TransactionId> startedTransactionId;

    @GuardedBy("this")
    private boolean clearTransactionId;

    AbstractQuery(
            QueryId queryId,
            SessionContext sessionContext,
            String query,
            QueryManager queryManager,
            SessionPropertyManager sessionPropertyManager,
            Executor resultsProcessorExecutor,
            ScheduledExecutorService timeoutExecutor)
    {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(sessionContext, "sessionContext is null");
        requireNonNull(query, "query is null");
        requireNonNull(queryManager, "queryManager is null");
        requireNonNull(resultsProcessorExecutor, "resultsProcessorExecutor is null");
        requireNonNull(timeoutExecutor, "timeoutExecutor is null");

        this.queryManager = queryManager;
        QueryInfo queryInfo = queryManager.createQuery(queryId, sessionContext, query);
        checkState(queryInfo.getQueryId().equals(queryId));
        this.queryId = queryInfo.getQueryId();
        this.session = queryInfo.getSession().toSession(sessionPropertyManager);
        this.resultsProcessorExecutor = resultsProcessorExecutor;
        this.timeoutExecutor = timeoutExecutor;
    }

    @Override
    public void cancel()
    {
        queryManager.cancelQuery(queryId);
        dispose();
    }

    @Override
    public QueryId getQueryId()
    {
        return queryId;
    }

    @Override
    public synchronized Optional<String> getSetCatalog()
    {
        return setCatalog;
    }

    @Override
    public synchronized Optional<String> getSetSchema()
    {
        return setSchema;
    }

    @Override
    public synchronized Map<String, String> getSetSessionProperties()
    {
        return setSessionProperties;
    }

    @Override
    public synchronized Set<String> getResetSessionProperties()
    {
        return resetSessionProperties;
    }

    @Override
    public synchronized Map<String, String> getAddedPreparedStatements()
    {
        return addedPreparedStatements;
    }

    @Override
    public synchronized Set<String> getDeallocatedPreparedStatements()
    {
        return deallocatedPreparedStatements;
    }

    @Override
    public synchronized Optional<TransactionId> getStartedTransactionId()
    {
        return startedTransactionId;
    }

    @Override
    public synchronized boolean isClearTransactionId()
    {
        return clearTransactionId;
    }

    synchronized void updateInfo(QueryInfo queryInfo)
    {
        // update catalog and schema
        setCatalog = queryInfo.getSetCatalog();
        setSchema = queryInfo.getSetSchema();

        // update setSessionProperties
        setSessionProperties = queryInfo.getSetSessionProperties();
        resetSessionProperties = queryInfo.getResetSessionProperties();

        // update preparedStatements
        addedPreparedStatements = queryInfo.getAddedPreparedStatements();
        deallocatedPreparedStatements = queryInfo.getDeallocatedPreparedStatements();

        // update startedTransactionId
        startedTransactionId = queryInfo.getStartedTransactionId();
        clearTransactionId = queryInfo.isClearTransactionId();
    }
}
