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

import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.memory.VersionedMemoryPoolId;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupQueryLimits;
import com.facebook.presto.sql.planner.Plan;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.facebook.airlift.units.Duration.succinctDuration;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class MockQueryExecution
        implements QueryExecution
{
    private static final AtomicLong ID_COUNTER = new AtomicLong(0);

    private final QueryId queryId;
    private final int runningTaskCount;
    private Optional<Throwable> failureReason = Optional.empty();

    public MockQueryExecution()
    {
        this(0);
    }

    private MockQueryExecution(int runningTaskCount)
    {
        this.queryId = QueryId.valueOf(String.valueOf(ID_COUNTER.getAndIncrement()));
        this.runningTaskCount = runningTaskCount;
    }

    public static MockQueryExecution withRunningTaskCount(int runningTaskCount)
    {
        return new MockQueryExecution(runningTaskCount);
    }

    @Override
    public QueryState getState()
    {
        return null;
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        return null;
    }

    @Override
    public void addStateChangeListener(StateMachine.StateChangeListener<QueryState> stateChangeListener)
    {}

    @Override
    public void addOutputInfoListener(Consumer<QueryOutputInfo> listener)
    {}

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
    public int getRetryCount()
    {
        return 0;
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return null;
    }

    @Override
    public long getRawInputDataSizeInBytes()
    {
        return 0;
    }

    @Override
    public long getWrittenIntermediateDataSizeInBytes()
    {
        return 0;
    }

    @Override
    public long getOutputPositions()
    {
        return 0;
    }

    @Override
    public long getOutputDataSizeInBytes()
    {
        return 0;
    }

    @Override
    public int getRunningTaskCount()
    {
        return runningTaskCount;
    }

    @Override
    public long getUserMemoryReservationInBytes()
    {
        return 0;
    }

    @Override
    public long getTotalMemoryReservationInBytes()
    {
        return 0;
    }

    @Override
    public VersionedMemoryPoolId getMemoryPool()
    {
        return null;
    }

    @Override
    public QueryId getQueryId()
    {
        return queryId;
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
    public long getCreateTimeInMillis()
    {
        return 0L;
    }

    @Override
    public Duration getQueuedTime()
    {
        return succinctDuration(0, MILLISECONDS);
    }

    @Override
    public long getExecutionStartTimeInMillis()
    {
        return 0L;
    }

    @Override
    public long getLastHeartbeatInMillis()
    {
        return 0L;
    }

    @Override
    public long getEndTimeInMillis()
    {
        return 0L;
    }

    @Override
    public Optional<ResourceGroupQueryLimits> getResourceGroupQueryLimits()
    {
        return Optional.empty();
    }

    @Override
    public void fail(Throwable cause)
    {
        this.failureReason = Optional.ofNullable(cause);
    }

    public Optional<Throwable> getFailureReason()
    {
        return failureReason;
    }

    @Override
    public void pruneExpiredQueryInfo()
    {}

    @Override
    public void pruneFinishedQueryInfo()
    {}

    @Override
    public void setResourceGroupQueryLimits(ResourceGroupQueryLimits resourceGroupQueryLimits)
    {}

    @Override
    public void setMemoryPool(VersionedMemoryPoolId poolId)
    {}

    @Override
    public void start()
    {}

    @Override
    public void cancelQuery()
    {}

    @Override
    public void cancelStage(StageId stageId)
    {}

    @Override
    public void recordHeartbeat()
    {}

    @Override
    public void addFinalQueryInfoListener(StateMachine.StateChangeListener<QueryInfo> stateChangeListener)
    {}
}
