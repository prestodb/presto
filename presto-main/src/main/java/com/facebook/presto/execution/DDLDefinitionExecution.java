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

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.resourceGroups.QueryType;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class DDLDefinitionExecution<T extends Statement>
        extends DataDefinitionExecution<T>
{
    private final DDLDefinitionTask<T> task;

    private DDLDefinitionExecution(
            DDLDefinitionTask<T> task,
            T statement,
            String slug,
            int retryCount,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters)
    {
        super(statement, slug, retryCount, transactionManager, metadata, accessControl, stateMachine, parameters);
        this.task = requireNonNull(task, "task is null");
    }

    @Override
    protected ListenableFuture<?> executeTask()
    {
        return task.execute(statement, transactionManager, metadata, accessControl, stateMachine.getSession(), parameters, stateMachine.getWarningCollector());
    }

    public static class DDLDefinitionExecutionFactory
            implements QueryExecution.QueryExecutionFactory<DDLDefinitionExecution<?>>
    {
        private final TransactionManager transactionManager;
        private final Metadata metadata;
        private final AccessControl accessControl;
        private final Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks;

        @Inject
        public DDLDefinitionExecutionFactory(
                TransactionManager transactionManager,
                MetadataManager metadata,
                AccessControl accessControl,
                Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks)
        {
            this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
            this.tasks = requireNonNull(tasks, "tasks is null");
        }

        @Override
        public DDLDefinitionExecution<?> createQueryExecution(
                QueryPreparer.PreparedQuery preparedQuery,
                QueryStateMachine stateMachine,
                String slug,
                int retryCount,
                WarningCollector warningCollector,
                Optional<QueryType> queryType)
        {
            return createDDLDefinitionExecution(preparedQuery.getStatement(), preparedQuery.getParameters(), stateMachine, slug, retryCount);
        }

        private <T extends Statement> DDLDefinitionExecution<T> createDDLDefinitionExecution(
                T statement,
                List<Expression> parameters,
                QueryStateMachine stateMachine,
                String slug,
                int retryCount)
        {
            @SuppressWarnings("unchecked")
            DDLDefinitionTask<T> task = (DDLDefinitionTask<T>) tasks.get(statement.getClass());
            checkArgument(task != null, "no task for statement: %s", statement.getClass().getSimpleName());

            stateMachine.setUpdateType(task.getName());
            return new DDLDefinitionExecution<>(task, statement, slug, retryCount, transactionManager, metadata, accessControl, stateMachine, parameters);
        }
    }
}
