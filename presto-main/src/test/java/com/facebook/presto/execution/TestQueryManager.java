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
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.QueryId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.Futures.immediateFuture;

public class TestQueryManager
        implements QueryManager
{
    public List<BasicQueryInfo> getQueries()
    {
        return ImmutableList.of();
    }

    public void addOutputInfoListener(QueryId queryId, Consumer<QueryExecution.QueryOutputInfo> listener)
    {}

    public void addStateChangeListener(QueryId queryId, StateMachine.StateChangeListener<QueryState> listener)
    {}

    public ListenableFuture<QueryState> getStateChange(QueryId queryId, QueryState currentState)
    {
        return immediateFuture(null);
    }

    public BasicQueryInfo getQueryInfo(QueryId queryId)
    {
        return null;
    }

    public QueryInfo getFullQueryInfo(QueryId queryId)
    {
        return null;
    }

    public Session getQuerySession(QueryId queryId)
    {
        return null;
    }

    public boolean isQuerySlugValid(QueryId queryId, String slug)
    {
        return true;
    }

    @Override
    public int getQueryRetryCount(QueryId queryId)
    {
        return 0;
    }

    public QueryState getQueryState(QueryId queryId)
    {
        return null;
    }

    public void recordHeartbeat(QueryId queryId)
    {}

    public void createQuery(QueryExecution execution)
    {}

    public void failQuery(QueryId queryId, Throwable cause)
    {}

    public void cancelQuery(QueryId queryId)
    {}

    public void cancelStage(StageId stageId)
    {}

    public QueryManagerStats getStats()
    {
        return null;
    }
}
