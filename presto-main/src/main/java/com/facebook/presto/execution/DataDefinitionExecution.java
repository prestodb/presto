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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.base.Throwables;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class DataDefinitionExecution<T extends Statement>
        implements QueryExecution
{
    private final DataDefinitionTask<T> task;
    private final T statement;
    private final TransactionManager transactionManager;
    private final Metadata metadata;
    private final AccessControl accessControl;
    private final QueryStateMachine stateMachine;

    private DataDefinitionExecution(
            DataDefinitionTask<T> task,
            T statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine)
    {
        this.task = requireNonNull(task, "task is null");
        this.statement = requireNonNull(statement, "statement is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
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
    public long getTotalMemoryReservation()
    {
        return 0;
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return new Duration(0, TimeUnit.SECONDS);
    }

    @Override
    public Session getSession()
    {
        return stateMachine.getSession();
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

            CompletableFuture<?> future = task.execute(statement, transactionManager, metadata, accessControl, stateMachine);
            future.whenComplete((o, throwable) -> {
                if (throwable == null) {
                    stateMachine.transitionToFinishing();
                }
                else {
                    fail(throwable);
                }
            });
        }
        catch (Throwable e) {
            fail(e);
            if (!(e instanceof RuntimeException)) {
                throw Throwables.propagate(e);
            }
        }
    }

    @Override
    public Duration waitForStateChange(QueryState currentState, Duration maxWait)
            throws InterruptedException
    {
        return stateMachine.waitForStateChange(currentState, maxWait);
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener);
    }

    @Override
    public void fail(Throwable cause)
    {
        stateMachine.transitionToFailed(cause);
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
        return stateMachine.getQueryInfoWithoutDetails();
    }

    @Override
    public QueryState getState()
    {
        return stateMachine.getQueryState();
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

        public String explain(Statement statement)
        {
            DataDefinitionTask<Statement> task = getTask(statement);
            checkArgument(task != null, "no task for statement: %s", statement.getClass().getSimpleName());
            return task.explain(statement);
        }

        @Override
        public DataDefinitionExecution<?> createQueryExecution(
                QueryId queryId,
                String query,
                Session session,
                Statement statement)
        {
            URI self = locationFactory.createQueryLocation(queryId);

            DataDefinitionTask<Statement> task = getTask(statement);
            checkArgument(task != null, "no task for statement: %s", statement.getClass().getSimpleName());

            QueryStateMachine stateMachine = QueryStateMachine.begin(queryId, query, session, self, task.isTransactionControl(), transactionManager, executor);
            stateMachine.setUpdateType(task.getName());
            return new DataDefinitionExecution<>(task, statement, transactionManager, metadata, accessControl, stateMachine);
        }

        @SuppressWarnings("unchecked")
        private <T extends Statement> DataDefinitionTask<T> getTask(T statement)
        {
            return (DataDefinitionTask<T>) tasks.get(statement.getClass());
        }
    }
}
