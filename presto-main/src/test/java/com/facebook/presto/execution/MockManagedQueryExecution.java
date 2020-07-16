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
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.SystemSessionProperties.QUERY_PRIORITY;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.operator.BlockedReason.WAITING_FOR_MEMORY;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static io.airlift.units.DataSize.succinctBytes;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class MockManagedQueryExecution
        implements ManagedQueryExecution
{
    private final List<StateChangeListener<QueryState>> listeners = new ArrayList<>();
    private final DataSize memoryUsage;
    private final Duration cpuUsage;
    private final Session session;
    private QueryState state = QUEUED;
    private Throwable failureCause;

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
    public BasicQueryInfo getBasicQueryInfo()
    {
        return new BasicQueryInfo(getQueryInfo());
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        return new QueryInfo(
                new QueryId("test"),
                TEST_SESSION.toSessionRepresentation(),
                state,
                new MemoryPoolId("reserved"),
                true,
                URI.create("1"),
                ImmutableList.of("2", "3"),
                "SELECT 1",
                new QueryStats(
                        DateTime.parse("1991-09-06T05:00-05:30"),
                        DateTime.parse("1991-09-06T05:01-05:30"),
                        DateTime.parse("1991-09-06T05:02-05:30"),
                        DateTime.parse("1991-09-06T06:00-05:30"),
                        Duration.valueOf("8m"),
                        Duration.valueOf("7m"),
                        Duration.valueOf("34m"),
                        Duration.valueOf("35m"),
                        Duration.valueOf("44m"),
                        Duration.valueOf("9m"),
                        Duration.valueOf("10m"),
                        Duration.valueOf("11m"),
                        13,
                        14,
                        15,
                        16,
                        100,
                        17,
                        18,
                        34,
                        19,
                        20.0,
                        DataSize.valueOf("21GB"),
                        DataSize.valueOf("22GB"),
                        DataSize.valueOf("23GB"),
                        DataSize.valueOf("24GB"),
                        DataSize.valueOf("25GB"),
                        DataSize.valueOf("26GB"),
                        true,
                        Duration.valueOf("23m"),
                        Duration.valueOf("24m"),
                        Duration.valueOf("0m"),
                        Duration.valueOf("26m"),
                        true,
                        ImmutableSet.of(WAITING_FOR_MEMORY),
                        DataSize.valueOf("123MB"),
                        DataSize.valueOf("27GB"),
                        28,
                        DataSize.valueOf("29GB"),
                        30,
                        DataSize.valueOf("31GB"),
                        32,
                        33,
                        DataSize.valueOf("34GB"),
                        DataSize.valueOf("35GB"),
                        DataSize.valueOf("36GB"),
                        ImmutableList.of(),
                        ImmutableList.of()),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                "33",
                Optional.empty(),
                null,
                null,
                ImmutableList.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
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

    private void fireStateChange()
    {
        for (StateChangeListener<QueryState> listener : listeners) {
            listener.stateChanged(state);
        }
    }
}
