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
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.Executor;

import static com.facebook.presto.execution.QueryInfo.immediateFailureQueryInfo;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FailedDispatchQuery
        implements DispatchQuery
{
    private final QueryInfo queryInfo;
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

        this.queryInfo = immediateFailureQueryInfo(session, query, self, resourceGroup, failure);
        this.basicQueryInfo = new BasicQueryInfo(queryInfo);
        this.session = requireNonNull(session, "session is null");
        this.executor = requireNonNull(executor, "executor is null");

        this.dispatchInfo = DispatchInfo.failed(
                failure,
                queryInfo.getQueryStats().getElapsedTime(),
                queryInfo.getQueryStats().getQueuedTime());
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        return basicQueryInfo;
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        return queryInfo;
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
    public void startWaitingForResources() {}

    @Override
    public void fail(Throwable throwable) {}

    @Override
    public void cancel() {}

    @Override
    public void pruneInfo() {}

    @Override
    public QueryId getQueryId()
    {
        return queryInfo.getQueryId();
    }

    @Override
    public boolean isDone()
    {
        return true;
    }

    @Override
    public Optional<ErrorCode> getErrorCode()
    {
        return Optional.ofNullable(queryInfo.getErrorCode());
    }

    @Override
    public void recordHeartbeat() {}

    @Override
    public int getRunningTaskCount()
    {
        return 0;
    }

    @Override
    public DateTime getLastHeartbeat()
    {
        return queryInfo.getQueryStats().getEndTime();
    }

    @Override
    public DateTime getCreateTime()
    {
        return queryInfo.getQueryStats().getCreateTime();
    }

    @Override
    public Optional<DateTime> getExecutionStartTime()
    {
        return getEndTime();
    }

    @Override
    public Optional<DateTime> getEndTime()
    {
        return Optional.ofNullable(queryInfo.getQueryStats().getEndTime());
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return new Duration(0, MILLISECONDS);
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        return new DataSize(0, BYTE);
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        return new DataSize(0, BYTE);
    }
}
