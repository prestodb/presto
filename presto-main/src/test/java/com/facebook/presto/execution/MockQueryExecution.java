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
import com.facebook.presto.memory.VersionedMemoryPoolId;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupQueryLimits;
import com.facebook.presto.sql.planner.Plan;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import java.util.Optional;
import java.util.function.Consumer;

public class MockQueryExecution
        implements QueryExecution
{
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
    { }

    @Override
    public void addOutputInfoListener(Consumer<QueryOutputInfo> listener)
    { }

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
    public DataSize getRawInputDataSize()
    {
        return null;
    }

    @Override
    public DataSize getOutputDataSize()
    {
        return null;
    }

    @Override
    public int getRunningTaskCount()
    {
        return 0;
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
    public Optional<ResourceGroupQueryLimits> getResourceGroupQueryLimits()
    {
        return Optional.empty();
    }

    @Override
    public void fail(Throwable cause)
    { }

    @Override
    public void pruneInfo()
    { }

    @Override
    public void setResourceGroupQueryLimits(ResourceGroupQueryLimits resourceGroupQueryLimits)
    { }

    @Override
    public void setMemoryPool(VersionedMemoryPoolId poolId)
    { }

    @Override
    public void start()
    { }

    @Override
    public void cancelQuery()
    { }

    @Override
    public void cancelStage(StageId stageId)
    { }

    @Override
    public void recordHeartbeat()
    { }

    @Override
    public void addFinalQueryInfoListener(StateMachine.StateChangeListener<QueryInfo> stateChangeListener)
    { }
}
