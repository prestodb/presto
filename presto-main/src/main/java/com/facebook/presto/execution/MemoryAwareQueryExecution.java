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
import com.facebook.presto.memory.ClusterMemoryManager;
import com.facebook.presto.memory.VersionedMemoryPoolId;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.sql.planner.Plan;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;

import java.util.Optional;
import java.util.function.Consumer;

import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.memory.LocalMemoryManager.RESERVED_POOL;
import static com.google.common.base.Throwables.throwIfInstanceOf;

public class MemoryAwareQueryExecution
        implements QueryExecution
{
    private final ClusterMemoryManager memoryManager;
    private final SqlQueryExecution delegate;
    private final long peakMemoryEstimate;

    @GuardedBy("this")
    private boolean startedWaiting;

    public MemoryAwareQueryExecution(ClusterMemoryManager memoryManager, SqlQueryExecution delegate)
    {
        this.memoryManager = memoryManager;
        this.delegate = delegate;
        this.peakMemoryEstimate = delegate.getSession().getResourceEstimates().getPeakMemory().map(DataSize::toBytes).orElse(0L);
    }

    @Override
    public QueryId getQueryId()
    {
        return delegate.getQueryId();
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        return delegate.getQueryInfo();
    }

    @Override
    public QueryState getState()
    {
        return delegate.getState();
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        return delegate.getStateChange(currentState);
    }

    @Override
    public void addOutputInfoListener(Consumer<QueryOutputInfo> listener)
    {
        delegate.addOutputInfoListener(listener);
    }

    @Override
    public Optional<ResourceGroupId> getResourceGroup()
    {
        return delegate.getResourceGroup();
    }

    @Override
    public void setResourceGroup(ResourceGroupId resourceGroupId)
    {
        delegate.setResourceGroup(resourceGroupId);
    }

    @Override
    public Plan getQueryPlan()
    {
        return delegate.getQueryPlan();
    }

    @Override
    public VersionedMemoryPoolId getMemoryPool()
    {
        return delegate.getMemoryPool();
    }

    @Override
    public void setMemoryPool(VersionedMemoryPoolId poolId)
    {
        delegate.setMemoryPool(poolId);
    }

    @Override
    public Optional<ErrorCode> getErrorCode()
    {
        return delegate.getErrorCode();
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        return delegate.getBasicQueryInfo();
    }

    @Override
    public Session getSession()
    {
        return delegate.getSession();
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        return delegate.getUserMemoryReservation();
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        return delegate.getTotalMemoryReservation();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return delegate.getTotalCpuTime();
    }

    @Override
    public synchronized void start()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", delegate.getQueryId())) {
            try {
                if (memoryManager.preAllocateQueryMemory(delegate.getQueryId(), peakMemoryEstimate)) {
                    delegate.addStateChangeListener(state -> {
                        if (state.isDone()) {
                            memoryManager.removePreAllocation(delegate.getQueryId());
                        }
                    });
                    delegate.start();
                    return;
                }

                if (!startedWaiting) {
                    // This may cause starvation, since requests may not be granted in the order they arrive
                    startedWaiting = true;
                    delegate.startWaitingForResources();
                    memoryManager.addChangeListener(GENERAL_POOL, none -> start());
                    memoryManager.addChangeListener(RESERVED_POOL, none -> start());
                }
            }
            catch (Throwable e) {
                fail(e);
                throwIfInstanceOf(e, Error.class);
            }
        }
    }

    @Override
    public void fail(Throwable cause)
    {
        delegate.fail(cause);
    }

    @Override
    public boolean isDone()
    {
        return getState().isDone();
    }

    @Override
    public void cancelQuery()
    {
        delegate.cancelQuery();
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        delegate.cancelStage(stageId);
    }

    @Override
    public void recordHeartbeat()
    {
        delegate.recordHeartbeat();
    }

    @Override
    public void pruneInfo()
    {
        delegate.pruneInfo();
    }

    @Override
    public void addStateChangeListener(StateMachine.StateChangeListener<QueryState> stateChangeListener)
    {
        delegate.addStateChangeListener(stateChangeListener);
    }

    @Override
    public void addFinalQueryInfoListener(StateMachine.StateChangeListener<QueryInfo> stateChangeListener)
    {
        delegate.addFinalQueryInfoListener(stateChangeListener);
    }
}
