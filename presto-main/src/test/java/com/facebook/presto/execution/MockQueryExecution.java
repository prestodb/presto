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
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.QUERY_PRIORITY;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class MockQueryExecution
        implements QueryExecution
{
    private final List<StateChangeListener<QueryState>> listeners = new ArrayList<>();
    private final long memoryUsage;
    private final Duration cpuUsage;
    private final Session session;
    private QueryState state = QUEUED;
    private Throwable failureCause;

    public MockQueryExecution(long memoryUsage)
    {
        this(memoryUsage, 1);
    }

    public MockQueryExecution(long memoryUsage, int priority)
    {
        this(memoryUsage, new Duration(0, MILLISECONDS), priority);
    }

    public MockQueryExecution(long memoryUsage, Duration cpuUsage, int priority)
    {
        this.memoryUsage = memoryUsage;
        this.cpuUsage = cpuUsage;
        this.session = testSessionBuilder()
                .setSystemProperty(QUERY_PRIORITY, String.valueOf(priority))
                .build();
    }

    public void complete()
    {
        state = FINISHED;
        fireStateChange();
    }

    @Override
    public QueryId getQueryId()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        return new QueryInfo(
                new QueryId("test"),
                session.toSessionRepresentation(),
                state,
                new MemoryPoolId("test"),
                !state.isDone(),
                URI.create("http://test"),
                ImmutableList.of(),
                "SELECT 1",
                new QueryStats(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                "",
                Optional.empty(),
                null,
                null,
                ImmutableSet.of(),
                Optional.empty(),
                state.isDone(),
                Optional.empty());
    }

    @Override
    public QueryState getState()
    {
        return state;
    }

    public Throwable getFailureCause()
    {
        return failureCause;
    }

    @Override
    public Duration waitForStateChange(QueryState currentState, Duration maxWait)
            throws InterruptedException
    {
        return null;
    }

    @Override
    public VersionedMemoryPoolId getMemoryPool()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setMemoryPool(VersionedMemoryPoolId poolId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getTotalMemoryReservation()
    {
        return memoryUsage;
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return cpuUsage;
    }

    @Override
    public Session getSession()
    {
        return session;
    }

    @Override
    public void start(Optional<String> resourceGroupName)
    {
        state = RUNNING;
        fireStateChange();
    }

    @Override
    public void fail(Throwable cause)
    {
        state = FAILED;
        failureCause = cause;
        fireStateChange();
    }

    @Override
    public void cancelQuery()
    {
        state = FAILED;
        fireStateChange();
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void recordHeartbeat()
    {
    }

    @Override
    public void pruneInfo()
    {
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        listeners.add(stateChangeListener);
    }

    @Override
    public void addFinalQueryInfoListener(StateChangeListener<QueryInfo> stateChangeListener)
    {
        throw new UnsupportedOperationException();
    }

    private void fireStateChange()
    {
        for (StateChangeListener<QueryState> listener : listeners) {
            listener.stateChanged(state);
        }
    }
}
