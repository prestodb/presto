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
package com.facebook.presto.spark.execution;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.execution.DDLDefinitionTask;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.QueryStateTimer;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkQueryExecution;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.transaction.TransactionInfo;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.spark.util.PrestoSparkFailureUtils.toPrestoSparkFailure;
import static com.facebook.presto.util.Failures.toFailure;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class PrestoSparkDataDefinitionExecution<T extends Statement>
        implements IPrestoSparkQueryExecution
{
    private static final Logger log = Logger.get(PrestoSparkDataDefinitionExecution.class);

    private final DDLDefinitionTask<T> task;
    private final T statement;
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final Metadata metadata;
    private final Session session;
    private final QueryStateTimer queryStateTimer;
    private final WarningCollector warningCollector;

    public PrestoSparkDataDefinitionExecution(
            DDLDefinitionTask<T> task,
            T statement,
            TransactionManager transactionManager,
            AccessControl accessControl,
            Metadata metadata,
            Session session,
            QueryStateTimer queryStateTimer,
            WarningCollector warningCollector)
    {
        this.task = requireNonNull(task, "task is null");
        this.statement = requireNonNull(statement, "statement is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.session = requireNonNull(session, "session is null");
        this.queryStateTimer = requireNonNull(queryStateTimer, "queryStateTimer is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    }

    @Override
    public List<List<Object>> execute()
    {
        queryStateTimer.beginRunning();
        try {
            ListenableFuture<?> future = task.execute(statement, transactionManager, metadata, accessControl, session, Collections.emptyList(), warningCollector);
            getFutureValue(future);
            queryStateTimer.beginFinishing();
            commit(session, transactionManager);
            queryStateTimer.endQuery();
        }
        catch (Throwable executionException) {
            queryStateTimer.beginFinishing();
            try {
                rollback(session, transactionManager);
            }
            catch (RuntimeException rollbackFailure) {
                log.error(rollbackFailure, "Encountered error when performing rollback");
            }

            Optional<ExecutionFailureInfo> failureInfo = Optional.of(toFailure(executionException));
            queryStateTimer.endQuery();

            throw toPrestoSparkFailure(session, failureInfo.get());
        }
        return Collections.emptyList();
    }

    private static void commit(Session session, TransactionManager transactionManager)
    {
        getFutureValue(transactionManager.asyncCommit(getTransactionInfo(session, transactionManager).getTransactionId()));
    }

    private static void rollback(Session session, TransactionManager transactionManager)
    {
        getFutureValue(transactionManager.asyncAbort(getTransactionInfo(session, transactionManager).getTransactionId()));
    }

    private static TransactionInfo getTransactionInfo(Session session, TransactionManager transactionManager)
    {
        Optional<TransactionInfo> transaction = session.getTransactionId()
                .flatMap(transactionManager::getOptionalTransactionInfo);
        checkState(transaction.isPresent(), "transaction is not present");
        checkState(transaction.get().isAutoCommitContext(), "transaction doesn't have auto commit context enabled");
        return transaction.get();
    }
}
