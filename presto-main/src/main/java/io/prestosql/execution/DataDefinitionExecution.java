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
package io.prestosql.execution;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.execution.QueryPreparer.PreparedQuery;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.memory.VersionedMemoryPoolId;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.security.AccessControl;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.sql.planner.Plan;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Statement;
import io.prestosql.transaction.TransactionManager;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class DataDefinitionExecution<T extends Statement>
        implements QueryExecution
{
    private final DataDefinitionTask<T> task;
    private final T statement;
    private final TransactionManager transactionManager;
    private final Metadata metadata;
    private final AccessControl accessControl;
    private final QueryStateMachine stateMachine;
    private final List<Expression> parameters;

    private DataDefinitionExecution(
            DataDefinitionTask<T> task,
            T statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters)
    {
        this.task = requireNonNull(task, "task is null");
        this.statement = requireNonNull(statement, "statement is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
        this.parameters = parameters;
    }

    @Override
    public VersionedMemoryPoolId getMemoryPool()
    {
        return stateMachine.getMemoryPool();
    }

    @Override
    public void setMemoryPool(VersionedMemoryPoolId poolId)
    {
        stateMachine.setMemoryPool(poolId);
    }

    @Override
    public Session getSession()
    {
        return stateMachine.getSession();
    }

    @Override
    public Optional<ErrorCode> getErrorCode()
    {
        return stateMachine.getFailureInfo().map(ExecutionFailureInfo::getErrorCode);
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        return new DataSize(0, BYTE);
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        return new DataSize(0, BYTE);
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
    public DateTime getLastHeartbeat()
    {
        return stateMachine.getLastHeartbeat();
    }

    @Override
    public Optional<DateTime> getEndTime()
    {
        return stateMachine.getEndTime();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return new Duration(0, NANOSECONDS);
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        return stateMachine.getFinalQueryInfo()
                .map(BasicQueryInfo::new)
                .orElseGet(() -> stateMachine.getBasicQueryInfo(Optional.empty()));
    }

    @Override
    public void start()
    {
        try {
            // transition to running
            if (!stateMachine.transitionToRunning()) {
                // query already running or finished
                return;
            }

            ListenableFuture<?> future = task.execute(statement, transactionManager, metadata, accessControl, stateMachine, parameters);
            Futures.addCallback(future, new FutureCallback<Object>()
            {
                @Override
                public void onSuccess(@Nullable Object result)
                {
                    stateMachine.transitionToFinishing();
                }

                @Override
                public void onFailure(Throwable throwable)
                {
                    fail(throwable);
                }
            }, directExecutor());
        }
        catch (Throwable e) {
            fail(e);
            throwIfInstanceOf(e, Error.class);
        }
    }

    @Override
    public void addOutputInfoListener(Consumer<QueryOutputInfo> listener)
    {
        // DDL does not have an output
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        return stateMachine.getStateChange(currentState);
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener);
    }

    @Override
    public void addFinalQueryInfoListener(StateChangeListener<QueryInfo> stateChangeListener)
    {
        stateMachine.addQueryInfoStateChangeListener(stateChangeListener);
    }

    @Override
    public void fail(Throwable cause)
    {
        stateMachine.transitionToFailed(cause);
    }

    @Override
    public boolean isDone()
    {
        return getState().isDone();
    }

    @Override
    public void cancelQuery()
    {
        stateMachine.transitionToCanceled();
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        // no-op
    }

    @Override
    public void recordHeartbeat()
    {
        stateMachine.recordHeartbeat();
    }

    @Override
    public void pruneInfo()
    {
        // no-op
    }

    @Override
    public QueryId getQueryId()
    {
        return stateMachine.getQueryId();
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        return stateMachine.getFinalQueryInfo().orElseGet(() -> stateMachine.updateQueryInfo(Optional.empty()));
    }

    @Override
    public Plan getQueryPlan()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryState getState()
    {
        return stateMachine.getQueryState();
    }

    public List<Expression> getParameters()
    {
        return parameters;
    }

    public static class DataDefinitionExecutionFactory
            implements QueryExecutionFactory<DataDefinitionExecution<?>>
    {
        private final LocationFactory locationFactory;
        private final TransactionManager transactionManager;
        private final Metadata metadata;
        private final AccessControl accessControl;
        private final ExecutorService executor;
        private final Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks;

        @Inject
        public DataDefinitionExecutionFactory(
                LocationFactory locationFactory,
                TransactionManager transactionManager,
                MetadataManager metadata,
                AccessControl accessControl,
                @ForQueryExecution ExecutorService executor,
                Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks)
        {
            this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
            this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
            this.executor = requireNonNull(executor, "executor is null");
            this.tasks = requireNonNull(tasks, "tasks is null");
        }

        @Override
        public DataDefinitionExecution<?> createQueryExecution(
                String query,
                Session session,
                PreparedQuery preparedQuery,
                ResourceGroupId resourceGroup,
                WarningCollector warningCollector)
        {
            return createDataDefinitionExecution(query, session, resourceGroup, preparedQuery.getStatement(), preparedQuery.getParameters(), warningCollector);
        }

        private <T extends Statement> DataDefinitionExecution<T> createDataDefinitionExecution(
                String query,
                Session session,
                ResourceGroupId resourceGroup,
                T statement,
                List<Expression> parameters,
                WarningCollector warningCollector)
        {
            @SuppressWarnings("unchecked")
            DataDefinitionTask<T> task = (DataDefinitionTask<T>) tasks.get(statement.getClass());
            checkArgument(task != null, "no task for statement: %s", statement.getClass().getSimpleName());

            QueryStateMachine stateMachine = QueryStateMachine.begin(
                    query,
                    session,
                    locationFactory.createQueryLocation(session.getQueryId()),
                    resourceGroup,
                    task.isTransactionControl(),
                    transactionManager,
                    accessControl,
                    executor,
                    metadata,
                    warningCollector);
            stateMachine.setUpdateType(task.getName());
            return new DataDefinitionExecution<>(task, statement, transactionManager, metadata, accessControl, stateMachine, parameters);
        }
    }
}
