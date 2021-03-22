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
package com.facebook.presto.spark.node;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryManagerStats;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.QueryId;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

public class PrestoSparkQueryManager
        implements QueryManager
{
    @Override
    public List<BasicQueryInfo> getQueries()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addOutputInfoListener(QueryId queryId, Consumer<QueryExecution.QueryOutputInfo> listener)
            throws NoSuchElementException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addStateChangeListener(QueryId queryId, StateMachine.StateChangeListener<QueryState> listener)
            throws NoSuchElementException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryId queryId, QueryState currentState)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BasicQueryInfo getQueryInfo(QueryId queryId)
            throws NoSuchElementException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryInfo getFullQueryInfo(QueryId queryId)
            throws NoSuchElementException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Session getQuerySession(QueryId queryId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isQuerySlugValid(QueryId queryId, String slug)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getQueryRetryCount(QueryId queryId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryState getQueryState(QueryId queryId)
            throws NoSuchElementException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void recordHeartbeat(QueryId queryId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createQuery(QueryExecution execution)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void failQuery(QueryId queryId, Throwable cause)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cancelQuery(QueryId queryId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryManagerStats getStats()
    {
        throw new UnsupportedOperationException();
    }
}
