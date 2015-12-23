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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.sql.tree.Statement;

import javax.inject.Inject;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class DataDefinitionExecution<T extends Statement>
        extends AbstractQueryExecution
{
    private final DataDefinitionTask<T> task;
    private final T statement;
    private final Metadata metadata;
    private final AccessControl accessControl;

    private DataDefinitionExecution(
            DataDefinitionTask<T> task,
            T statement,
            Session session,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine)
    {
        super(session, stateMachine);
        this.task = requireNonNull(task, "task is null");
        this.statement = requireNonNull(statement, "statement is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    protected void execute()
    {
        task.execute(statement, session, metadata, accessControl, stateMachine);
    }

    public static class DataDefinitionExecutionFactory
            implements QueryExecutionFactory<DataDefinitionExecution<?>>
    {
        private final LocationFactory locationFactory;
        private final Metadata metadata;
        private final AccessControl accessControl;
        private final ExecutorService executor;
        private final Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks;

        @Inject
        public DataDefinitionExecutionFactory(
                LocationFactory locationFactory,
                MetadataManager metadata,
                AccessControl accessControl,
                @ForQueryExecution ExecutorService executor,
                Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks)
        {
            this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
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
            QueryStateMachine stateMachine = new QueryStateMachine(queryId, query, session, self, executor);
            return createExecution(statement, session, stateMachine);
        }

        private DataDefinitionExecution<?> createExecution(Statement statement, Session session, QueryStateMachine stateMachine)
        {
            DataDefinitionTask<Statement> task = getTask(statement);
            checkArgument(task != null, "no task for statement: %s", statement.getClass().getSimpleName());
            stateMachine.setUpdateType(task.getName());
            return new DataDefinitionExecution<>(task, statement, session, metadata, accessControl, stateMachine);
        }

        @SuppressWarnings("unchecked")
        private <T extends Statement> DataDefinitionTask<T> getTask(T statement)
        {
            return (DataDefinitionTask<T>) tasks.get(statement.getClass());
        }
    }
}
