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
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.BasicQueryStats;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupQueryLimits;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;

import static com.facebook.presto.SystemSessionProperties.QUERY_PRIORITY;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.QueryState.WAITING_FOR_PREREQUISITES;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class MockManagedQueryExecution
        implements ManagedQueryExecution
{
    private final List<StateChangeListener<QueryState>> listeners = new ArrayList<>();
    private final DataSize memoryUsage;
    private final Duration cpuUsage;
    private final Session session;
    private QueryState state = WAITING_FOR_PREREQUISITES;
    private Throwable failureCause;
    private Optional<ResourceGroupQueryLimits> resourceGroupQueryLimits = Optional.empty();

    public MockManagedQueryExecution(long memoryUsage)
    {
        this(memoryUsage, "query_id", 1);
    }

    public MockManagedQueryExecution(long memoryUsage, String queryId, int priority)
    {
        this(memoryUsage, queryId, priority, new Duration(0, MILLISECONDS));
    }

    public MockManagedQueryExecution(long memoryUsage, String queryId, int priority, Duration cpuUsage)
    {
        this.memoryUsage = succinctBytes(memoryUsage);
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

    public Throwable getThrowable()
    {
        return failureCause;
    }

    @Override
    public Session getSession()
    {
        return session;
    }

    @Override
    public Optional<ErrorCode> getErrorCode()
    {
        return Optional.empty();
    }

    @Override
    public boolean isRetry()
    {
        return false;
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        return new BasicQueryInfo(
                new QueryId("test"),
                session.toSessionRepresentation(),
                Optional.empty(),
                state,
                new MemoryPoolId("test"),
                !state.isDone(),
                URI.create("http://test"),
                "SELECT 1",
                new BasicQueryStats(
                        new DateTime(1),
                        new DateTime(2),
                        new Duration(2, NANOSECONDS),
                        new Duration(3, NANOSECONDS),
                        new Duration(4, NANOSECONDS),
                        new Duration(5, NANOSECONDS),
                        5,
                        5,
                        6,
                        7,
                        8,
                        9,
                        new DataSize(14, BYTE),
                        15,
                        16.0,
                        25.0,
                        new DataSize(17, BYTE),
                        new DataSize(18, BYTE),
                        new DataSize(19, BYTE),
                        new DataSize(20, BYTE),
                        new DataSize(21, BYTE),
                        new DataSize(42, BYTE),
                        new Duration(22, NANOSECONDS),
                        new Duration(23, NANOSECONDS),
                        false,
                        ImmutableSet.of(),
                        new DataSize(24, BYTE),
                        OptionalDouble.empty()),
                null,
                Optional.empty(),
                ImmutableList.of());
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        return memoryUsage;
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        return memoryUsage;
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return cpuUsage;
    }

    public QueryState getState()
    {
        return state;
    }

    @Override
    public void startWaitingForPrerequisites()
    {
        state = QUEUED;
        fireStateChange();
    }

    @Override
    public void startWaitingForResources()
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
    public boolean isDone()
    {
        return getState().isDone();
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        listeners.add(stateChangeListener);
    }

    @Override
    public void setResourceGroupQueryLimits(ResourceGroupQueryLimits resourceGroupQueryLimits)
    {
        this.resourceGroupQueryLimits = Optional.of(resourceGroupQueryLimits);
    }

    private void fireStateChange()
    {
        for (StateChangeListener<QueryState> listener : listeners) {
            listener.stateChanged(state);
        }
    }
}
