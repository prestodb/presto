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
package com.facebook.presto.dispatcher;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStateMachine;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.memory.VersionedMemoryPoolId;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.sql.planner.Plan;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TestingQueryExecution
        implements QueryExecution
{
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
    private final List<StateMachine.StateChangeListener<QueryState>> listeners = new ArrayList<>();

    private final long runtimeMillis;
    private final long totalMemoryBytes;
    private final QueryStateMachine stateMachine;
    private final TestingClusterMemoryPoolManager memoryManager;

    public TestingQueryExecution(Duration runtime, DataSize totalMemory, QueryStateMachine stateMachine, TestingClusterMemoryPoolManager memoryManager)
    {
        this.runtimeMillis = requireNonNull(runtime, "runtime is null").toMillis();
        this.totalMemoryBytes = requireNonNull(totalMemory, "totalMemoryBytes is null").toBytes();
        this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
        this.memoryManager = requireNonNull(memoryManager, "memoryManager");
    }

    @Override
    public QueryState getState()
    {
        return stateMachine.getQueryState();
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        return stateMachine.getStateChange(currentState);
    }

    @Override
    public void addStateChangeListener(StateMachine.StateChangeListener<QueryState> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener);
    }

    @Override
    public synchronized void start()
    {
        memoryManager.reserve(totalMemoryBytes);
        executor.schedule(this::endQuery, runtimeMillis, MILLISECONDS);
    }

    private synchronized void endQuery()
    {
        memoryManager.free(totalMemoryBytes);
        stateMachine.transitionToFinishing();
    }

    @Override
    public void addOutputInfoListener(Consumer<QueryOutputInfo> listener)
    {
    }

    @Override
    public Plan getQueryPlan()
    {
        return null;
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        return null;
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        return null;
    }

    @Override
    public String getSlug()
    {
        return null;
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return null;
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        return null;
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        return null;
    }

    @Override
    public VersionedMemoryPoolId getMemoryPool()
    {
        return null;
    }

    @Override
    public void setMemoryPool(VersionedMemoryPoolId poolId)
    {
    }

    @Override
    public void cancelQuery()
    {
    }

    @Override
    public void cancelStage(StageId stageId)
    {
    }

    @Override
    public void recordHeartbeat()
    {
    }

    @Override
    public void addFinalQueryInfoListener(StateMachine.StateChangeListener<QueryInfo> stateChangeListener)
    {
    }

    @Override
    public QueryId getQueryId()
    {
        return null;
    }

    @Override
    public boolean isDone()
    {
        return false;
    }

    @Override
    public Session getSession()
    {
        return null;
    }

    @Override
    public DateTime getCreateTime()
    {
        return null;
    }

    @Override
    public Optional<DateTime> getExecutionStartTime()
    {
        return Optional.empty();
    }

    @Override
    public DateTime getLastHeartbeat()
    {
        return null;
    }

    @Override
    public Optional<DateTime> getEndTime()
    {
        return Optional.empty();
    }

    @Override
    public void fail(Throwable cause)
    {
    }

    @Override
    public void pruneInfo()
    {
    }
}
