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
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.memory.VersionedMemoryPoolId;
import com.facebook.presto.transaction.TransactionManager;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static java.util.Objects.requireNonNull;

public class FailedQueryExecution
        implements QueryExecution
{
    private final QueryInfo queryInfo;
    private final Session session;

    public FailedQueryExecution(QueryId queryId, String query, Session session, URI self, TransactionManager transactionManager, Executor executor, Throwable cause)
    {
        requireNonNull(cause, "cause is null");
        this.session = requireNonNull(session, "session is null");
        QueryStateMachine queryStateMachine = QueryStateMachine.failed(queryId, query, session, self, transactionManager, executor, cause);

        queryInfo = queryStateMachine.getQueryInfo(null);
    }

    @Override
    public QueryId getQueryId()
    {
        return queryInfo.getQueryId();
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        return queryInfo;
    }

    @Override
    public QueryState getState()
    {
        return queryInfo.getState();
    }

    @Override
    public VersionedMemoryPoolId getMemoryPool()
    {
        return new VersionedMemoryPoolId(GENERAL_POOL, 0);
    }

    @Override
    public void setMemoryPool(VersionedMemoryPoolId poolId)
    {
        // no-op
    }

    @Override
    public long getTotalMemoryReservation()
    {
        return 0;
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return new Duration(0, TimeUnit.SECONDS);
    }

    @Override
    public Session getSession()
    {
        return session;
    }

    @Override
    public void start()
    {
        // no-op
    }

    @Override
    public Duration waitForStateChange(QueryState currentState, Duration maxWait)
            throws InterruptedException
    {
        return maxWait;
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        stateChangeListener.stateChanged(QueryState.FAILED);
    }

    @Override
    public void fail(Throwable cause)
    {
        // no-op
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        // no-op
    }

    @Override
    public void recordHeartbeat()
    {
        // no-op
    }

    @Override
    public void pruneInfo()
    {
        // no-op
    }
}
