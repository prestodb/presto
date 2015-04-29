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
import com.facebook.presto.sql.tree.Statement;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DataDefinitionExecution<T extends Statement>
        implements QueryExecution
{
    private final DataDefinitionTask<T> task;
    private final T statement;
    private final Session session;
    private final Metadata metadata;
    private final QueryStateMachine stateMachine;

    private DataDefinitionExecution(
            DataDefinitionTask<T> task,
            T statement,
            Session session,
            Metadata metadata,
            QueryStateMachine stateMachine)
    {
        this.task = checkNotNull(task, "task is null");
        this.statement = checkNotNull(statement, "statement is null");
        this.session = checkNotNull(session, "session is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.stateMachine = checkNotNull(stateMachine, "stateMachine is null");
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
    public void start()
    {
        try {
            // transition to starting
            if (!stateMachine.starting()) {
                // query already started or finished
                return;
            }

            stateMachine.recordExecutionStart();

            task.execute(statement, session, metadata, stateMachine);

            stateMachine.finished();
        }
        catch (RuntimeException e) {
            fail(e);
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
        stateMachine.fail(cause);
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

    public static class DataDefinitionExecutionFactory
            implements QueryExecutionFactory<DataDefinitionExecution<?>>
    {
        private final LocationFactory locationFactory;
        private final Metadata metadata;
        private final ExecutorService executor;
        private final Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks;

        @Inject
        public DataDefinitionExecutionFactory(
                LocationFactory locationFactory,
                MetadataManager metadata,
                @ForQueryExecution ExecutorService executor,
                Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks)
        {
            this.locationFactory = checkNotNull(locationFactory, "locationFactory is null");
            this.metadata = checkNotNull(metadata, "metadata is null");
            this.executor = checkNotNull(executor, "executor is null");
            this.tasks = checkNotNull(tasks, "tasks is null");
        }

        @Override
        public DataDefinitionExecution<?> createQueryExecution(
                QueryId queryId,
                String query,
                Session session,
                Statement statement)
        {
            URI self = locationFactory.createQueryLocation(queryId);
            QueryStateMachine stateMachine = new QueryStateMachine(queryId, query, session, self, executor);
            return createExecution(statement, session, stateMachine);
        }

        private <T extends Statement> DataDefinitionExecution<?> createExecution(
                T statement,
                Session session,
                QueryStateMachine stateMachine)
        {
            DataDefinitionTask<T> task = getTask(statement);
            checkArgument(task != null, "no task for statement: %s", statement.getClass().getSimpleName());
            stateMachine.setUpdateType(task.getName());
            return new DataDefinitionExecution<>(task, statement, session, metadata, stateMachine);
        }

        @SuppressWarnings("unchecked")
        private <T extends Statement> DataDefinitionTask<T> getTask(T statement)
        {
            return (DataDefinitionTask<T>) tasks.get(statement.getClass());
        }
    }
}
