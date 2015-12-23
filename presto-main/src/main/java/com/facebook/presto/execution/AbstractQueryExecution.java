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
import com.google.common.base.Throwables;
import io.airlift.units.Duration;

import static java.util.Objects.requireNonNull;

public abstract class AbstractQueryExecution
        implements QueryExecution
{
    protected final Session session;
    protected final QueryStateMachine stateMachine;

    protected AbstractQueryExecution(Session session, QueryStateMachine stateMachine)
    {
        this.session = requireNonNull(session, "session is null");
        this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
    }

    protected abstract void execute();

    @Override
    public QueryId getQueryId()
    {
        return stateMachine.getQueryId();
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        return stateMachine.getQueryInfoWithoutDetails();
    }

    @Override
    public QueryState getState()
    {
        return stateMachine.getQueryState();
    }

    @Override
    public Duration waitForStateChange(QueryState currentState, Duration maxWait)
            throws InterruptedException
    {
        return stateMachine.waitForStateChange(currentState, maxWait);
    }

    @Override
    public VersionedMemoryPoolId getMemoryPool()
    {
        return stateMachine.getMemoryPool();
    }

    @Override
    public void setMemoryPool(VersionedMemoryPoolId poolId)
    {
        stateMachine.setMemoryPool(poolId);
    }

    @Override
    public long getTotalMemoryReservation()
    {
        return 0;
    }

    @Override
    public Session getSession()
    {
        return session;
    }

    @Override
    public void start()
    {
        try {
            // transition to running
            if (!stateMachine.transitionToRunning()) {
                // query already running or finished
                return;
            }

            execute();

            stateMachine.transitionToFinished();
        }
        catch (Throwable e) {
            fail(e);
            if (!(e instanceof RuntimeException)) {
                throw Throwables.propagate(e);
            }
        }
    }

    @Override
    public void fail(Throwable cause)
    {
        stateMachine.transitionToFailed(cause);
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        // no-op
    }

    @Override
    public void recordHeartbeat()
    {
        stateMachine.recordHeartbeat();
    }

    @Override
    public void pruneInfo()
    {
        // no-op
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener);
    }
}
