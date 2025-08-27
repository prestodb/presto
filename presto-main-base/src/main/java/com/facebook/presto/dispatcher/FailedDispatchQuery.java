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

import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupQueryLimits;
import com.google.common.util.concurrent.ListenableFuture;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.Executor;

import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.server.BasicQueryInfo.immediateFailureQueryInfo;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FailedDispatchQuery
        implements DispatchQuery
{
    private final BasicQueryInfo basicQueryInfo;
    private final Session session;
    private final Executor executor;
    private final DispatchInfo dispatchInfo;

    public FailedDispatchQuery(
            Session session,
            String query,
            URI self,
            Optional<ResourceGroupId> resourceGroup,
            ExecutionFailureInfo failure,
            Executor executor)
    {
        requireNonNull(session, "session is null");
        requireNonNull(query, "query is null");
        requireNonNull(self, "self is null");
        requireNonNull(resourceGroup, "resourceGroup is null");
        requireNonNull(failure, "failure is null");
        requireNonNull(executor, "executor is null");

        this.basicQueryInfo = immediateFailureQueryInfo(session, query, self, resourceGroup, failure);
        this.session = requireNonNull(session, "session is null");
        this.executor = requireNonNull(executor, "executor is null");

        this.dispatchInfo = DispatchInfo.failed(
                failure,
                basicQueryInfo.getQueryStats().getElapsedTime(),
                basicQueryInfo.getQueryStats().getWaitingForPrerequisitesTime(),
                basicQueryInfo.getQueryStats().getQueuedTime());
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        return basicQueryInfo;
    }

    @Override
    public Session getSession()
    {
        return session;
    }

    @Override
    public ListenableFuture<?> getDispatchedFuture()
    {
        return immediateFuture(null);
    }

    @Override
    public DispatchInfo getDispatchInfo()
    {
        return dispatchInfo;
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        executor.execute(() -> stateChangeListener.stateChanged(FAILED));
    }

    @Override
    public void startWaitingForPrerequisites() {}

    @Override
    public void startWaitingForResources() {}

    @Override
    public void fail(Throwable throwable) {}

    @Override
    public void cancel() {}

    @Override
    public void pruneExpiredQueryInfo() {}

    @Override
    public void pruneFinishedQueryInfo() {}

    @Override
    public QueryId getQueryId()
    {
        return basicQueryInfo.getQueryId();
    }

    @Override
    public boolean isDone()
    {
        return true;
    }

    @Override
    public Optional<ErrorCode> getErrorCode()
    {
        return Optional.ofNullable(basicQueryInfo.getErrorCode());
    }

    @Override
    public boolean isRetry()
    {
        return false;
    }

    @Override
    public void recordHeartbeat() {}

    @Override
    public long getLastHeartbeatInMillis()
    {
        return basicQueryInfo.getQueryStats().getEndTimeInMillis();
    }

    @Override
    public long getCreateTimeInMillis()
    {
        return basicQueryInfo.getQueryStats().getCreateTimeInMillis();
    }

    @Override
    public Duration getQueuedTime()
    {
        return basicQueryInfo.getQueryStats().getQueuedTime();
    }

    @Override
    public long getExecutionStartTimeInMillis()
    {
        return getEndTimeInMillis();
    }

    @Override
    public long getEndTimeInMillis()
    {
        return basicQueryInfo.getQueryStats().getEndTimeInMillis();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return new Duration(0, MILLISECONDS);
    }

    @Override
    public long getTotalMemoryReservationInBytes()
    {
        return 0L;
    }

    @Override
    public long getUserMemoryReservationInBytes()
    {
        return 0L;
    }

    @Override
    public Optional<ResourceGroupQueryLimits> getResourceGroupQueryLimits()
    {
        return Optional.empty();
    }

    @Override
    public void setResourceGroupQueryLimits(ResourceGroupQueryLimits resourceGroupQueryLimits)
    {}
}
