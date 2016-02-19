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
package com.facebook.presto.testing;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryId;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.StageId;
import io.airlift.units.Duration;

import java.util.List;
import java.util.Optional;

public class MockQueryManager
        implements QueryManager
{
    @Override
    public List<QueryInfo> getAllQueryInfo()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Duration waitForStateChange(QueryId queryId, QueryState currentState, Duration maxWait)
            throws InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryInfo getQueryInfo(QueryId queryId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<QueryState> getQueryState(QueryId queryId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void recordHeartbeat(QueryId queryId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryInfo createQuery(Session session, String query)
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
}
