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
import com.facebook.presto.execution.ClusterSizeMonitor;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStateMachine;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.QueryId;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.PLANNING;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class LocalDispatchQuery
        implements DispatchQuery
{
    private final QueryStateMachine stateMachine;
    private final ListenableFuture<QueryExecution> queryExecutionFuture;

    private final CoordinatorLocation coordinatorLocation;

    private final ClusterSizeMonitor clusterSizeMonitor;

    private final ExecutorService queryExecutor;

    private final Function<QueryExecution, ListenableFuture<?>> querySubmitter;

    public LocalDispatchQuery(
            QueryStateMachine stateMachine,
            ListenableFuture<QueryExecution> queryExecutionFuture,
            CoordinatorLocation coordinatorLocation,
            ClusterSizeMonitor clusterSizeMonitor,
            ExecutorService queryExecutor,
            Function<QueryExecution, ListenableFuture<?>> querySubmitter)
    {
        this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
        this.queryExecutionFuture = requireNonNull(queryExecutionFuture, "queryExecutionFuture is null");
        this.coordinatorLocation = requireNonNull(coordinatorLocation, "coordinatorLocation is null");
        this.clusterSizeMonitor = requireNonNull(clusterSizeMonitor, "clusterSizeMonitor is null");
        this.queryExecutor = requireNonNull(queryExecutor, "queryExecutor is null");
        this.querySubmitter = requireNonNull(querySubmitter, "querySubmitter is null");

        addExceptionCallback(queryExecutionFuture, stateMachine::transitionToFailed);
    }

    @Override
    public void startWaitingForResources()
    {
        if (stateMachine.transitionToWaitingForResources()) {
            waitForMinimumWorkers();
        }
    }

    private void waitForMinimumWorkers()
    {
        ListenableFuture<?> minimumWorkerFuture = clusterSizeMonitor.waitForMinimumWorkers();
        // when worker requirement is met, wait for query execution to finish construction and then start the execution
        addSuccessCallback(minimumWorkerFuture, () -> addSuccessCallback(queryExecutionFuture, this::startExecution));
        addExceptionCallback(minimumWorkerFuture, throwable -> queryExecutor.submit(() -> stateMachine.transitionToFailed(throwable)));
    }

    private void startExecution(QueryExecution queryExecution)
    {
        queryExecutor.submit(() -> {
            if (stateMachine.transitionToDispatching()) {
                querySubmitter.apply(queryExecution);
            }
        });
    }

    @Override
    public void recordHeartbeat()
    {
        stateMachine.recordHeartbeat();
    }

    @Override
    public DateTime getLastHeartbeat()
    {
        return stateMachine.getLastHeartbeat();
    }

    @Override
    public ListenableFuture<?> getDispatchedFuture()
    {
        return queryDispatchFuture(stateMachine.getQueryState());
    }

    private ListenableFuture<?> queryDispatchFuture(QueryState currentState)
    {
        if (currentState.ordinal() >= PLANNING.ordinal()) {
            return immediateFuture(null);
        }
        return Futures.transformAsync(stateMachine.getStateChange(currentState), this::queryDispatchFuture, directExecutor());
    }

    @Override
    public DispatchInfo getDispatchInfo()
    {
        BasicQueryInfo queryInfo = stateMachine.getBasicQueryInfo(Optional.empty());
        Optional<CoordinatorLocation> coordinator = Optional.empty();
        if (queryInfo.getState().ordinal() >= PLANNING.ordinal()) {
            coordinator = Optional.of(coordinatorLocation);
        }
        Optional<ExecutionFailureInfo> failureInfo = Optional.empty();
        if (queryInfo.getState() == FAILED) {
            failureInfo = stateMachine.getFailureInfo();
        }
        return new DispatchInfo(coordinator, failureInfo, queryInfo.getQueryStats().getElapsedTime(), queryInfo.getQueryStats().getQueuedTime());
    }

    @Override
    public QueryId getQueryId()
    {
        return stateMachine.getQueryId();
    }

    @Override
    public boolean isDone()
    {
        return stateMachine.getQueryState().isDone();
    }

    @Override
    public DateTime getCreateTime()
    {
        return stateMachine.getCreateTime();
    }

    @Override
    public Optional<DateTime> getExecutionStartTime()
    {
        return stateMachine.getExecutionStartTime();
    }

    @Override
    public Optional<DateTime> getEndTime()
    {
        return stateMachine.getEndTime();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return tryGetQueryExecution()
                .map(QueryExecution::getTotalCpuTime)
                .orElse(new Duration(0, MILLISECONDS));
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        return tryGetQueryExecution()
                .map(QueryExecution::getTotalMemoryReservation)
                .orElse(new DataSize(0, BYTE));
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        return tryGetQueryExecution()
                .map(QueryExecution::getUserMemoryReservation)
                .orElse(new DataSize(0, BYTE));
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        return tryGetQueryExecution()
                .map(QueryExecution::getBasicQueryInfo)
                .orElse(stateMachine.getBasicQueryInfo(Optional.empty()));
    }

    @Override
    public Session getSession()
    {
        return stateMachine.getSession();
    }

    @Override
    public void fail(Throwable throwable)
    {
        stateMachine.transitionToFailed(throwable);
    }

    @Override
    public void cancel()
    {
        stateMachine.transitionToCanceled();
    }

    @Override
    public void pruneInfo()
    {
        stateMachine.pruneQueryInfo();
    }

    @Override
    public Optional<ErrorCode> getErrorCode()
    {
        return stateMachine.getFailureInfo().map(ExecutionFailureInfo::getErrorCode);
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener);
    }

    private Optional<QueryExecution> tryGetQueryExecution()
    {
        try {
            return tryGetFutureValue(queryExecutionFuture);
        }
        catch (Exception ignored) {
            return Optional.empty();
        }
    }
}
