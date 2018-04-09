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

import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.sql.planner.Plan;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public class LazyProxyQueryManager<T>
        implements QueryManager<T>
{
    private final QueryManager<LazyOutput> queryManager;

    @Inject
    public LazyProxyQueryManager(QueryManager<LazyOutput> queryManager)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
    }

    @Override
    public List<QueryInfo> getAllQueryInfo()
    {
        return queryManager.getAllQueryInfo();
    }

    @Override
    public void addOutputListener(QueryId queryId, Consumer<T> listener)
    {
        // no-op
        // the logic should be explicitly handled by LazyQuery::setOutput
    }

    @Override
    public void addStateChangeListener(QueryId queryId, StateChangeListener<QueryState> listener)
    {
        queryManager.addStateChangeListener(queryId, listener);
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryId queryId, QueryState currentState)
    {
        return queryManager.getStateChange(queryId, currentState);
    }

    @Override
    public QueryInfo getQueryInfo(QueryId queryId)
    {
        return queryManager.getQueryInfo(queryId);
    }

    @Override
    public Optional<ResourceGroupId> getQueryResourceGroup(QueryId queryId)
    {
        return queryManager.getQueryResourceGroup(queryId);
    }

    @Override
    public Plan getQueryPlan(QueryId queryId)
    {
        return queryManager.getQueryPlan(queryId);
    }

    @Override
    public Optional<QueryState> getQueryState(QueryId queryId)
    {
        return queryManager.getQueryState(queryId);
    }

    @Override
    public void recordHeartbeat(QueryId queryId)
    {
        queryManager.recordHeartbeat(queryId);
    }

    @Override
    public QueryInfo createQuery(QueryId queryId, SessionContext sessionContext, String query)
    {
        return queryManager.getQueryInfo(queryId);
    }

    @Override
    public void failQuery(QueryId queryId, Throwable cause)
    {
        queryManager.failQuery(queryId, cause);
    }

    @Override
    public void cancelQuery(QueryId queryId)
    {
        queryManager.cancelQuery(queryId);
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        queryManager.cancelStage(stageId);
    }

    @Override
    public SqlQueryManagerStats getStats()
    {
        return queryManager.getStats();
    }
}
