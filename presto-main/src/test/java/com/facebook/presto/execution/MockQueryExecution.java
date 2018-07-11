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
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.sql.planner.Plan;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static com.facebook.presto.SystemSessionProperties.QUERY_PRIORITY;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class MockQueryExecution
        implements QueryExecution
{
    private final List<StateChangeListener<QueryState>> listeners = new ArrayList<>();
    private final long memoryUsage;
    private final Duration cpuUsage;
    private final Session session;
    private final QueryId queryId;
    private QueryState state = QUEUED;
    private Throwable failureCause;
    private Optional<ResourceGroupId> resourceGroupId;

    public MockQueryExecution(long memoryUsage)
    {
        this(memoryUsage, "query_id", 1);
    }

    public MockQueryExecution(long memoryUsage, String queryId, int priority)
    {
        this(memoryUsage, queryId, priority, new Duration(0, MILLISECONDS));
    }

    public MockQueryExecution(long memoryUsage, String queryId, int priority, Duration cpuUsage)
    {
        this.memoryUsage = memoryUsage;
        this.cpuUsage = cpuUsage;
        this.session = testSessionBuilder()
                .setSystemProperty(QUERY_PRIORITY, String.valueOf(priority))
                .build();
        this.resourceGroupId = Optional.empty();
        this.queryId = new QueryId(queryId);
    }

    public void complete()
    {
        state = FINISHED;
        fireStateChange();
    }

    @Override
    public QueryId getQueryId()
    {
        return queryId;
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
                new QueryStats(
                        new DateTime(1),
                        new DateTime(2),
                        new DateTime(3),
                        new DateTime(4),
                        new Duration(6, NANOSECONDS),
                        new Duration(5, NANOSECONDS),
                        new Duration(31, NANOSECONDS),
                        new Duration(7, NANOSECONDS),
                        new Duration(8, NANOSECONDS),

                        new Duration(100, NANOSECONDS),
                        new Duration(200, NANOSECONDS),

                        9,
                        10,
                        11,

                        12,
                        13,
                        15,
                        30,
                        16,

                        17.0,
                        new DataSize(18, BYTE),
                        new DataSize(19, BYTE),
                        new DataSize(20, BYTE),
                        new DataSize(21, BYTE),
                        new DataSize(22, BYTE),

                        true,
                        new Duration(20, NANOSECONDS),
                        new Duration(21, NANOSECONDS),
                        new Duration(22, NANOSECONDS),
                        new Duration(23, NANOSECONDS),
                        false,
                        ImmutableSet.of(),

                        new DataSize(24, BYTE),
                        25,

                        new DataSize(26, BYTE),
                        27,

                        new DataSize(28, BYTE),
                        29,

                        new DataSize(30, BYTE),

                        ImmutableList.of(),
                        ImmutableList.of()),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
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

    @Override
    public Plan getQueryPlan()
    {
        throw new UnsupportedOperationException();
    }

    public Throwable getFailureCause()
    {
        return failureCause;
    }

    @Override
    public void addOutputInfoListener(Consumer<QueryOutputInfo> listener)
    {
        // no-op
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        return immediateFuture(state);
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
    public long getUserMemoryReservation()
    {
        return memoryUsage;
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
    public Optional<ResourceGroupId> getResourceGroup()
    {
        return this.resourceGroupId;
    }

    @Override
    public void setResourceGroup(ResourceGroupId resourceGroupId)
    {
        this.resourceGroupId = Optional.of(requireNonNull(resourceGroupId, "resourceGroupId is null"));
    }

    @Override
    public void start()
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
