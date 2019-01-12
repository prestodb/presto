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

import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.transaction.IsolationLevel;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Isolation;
import io.prestosql.sql.tree.StartTransaction;
import io.prestosql.sql.tree.TransactionAccessMode;
import io.prestosql.transaction.TransactionId;
import io.prestosql.transaction.TransactionManager;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.sql.analyzer.SemanticErrorCode.INVALID_TRANSACTION_MODE;

public class StartTransactionTask
        implements DataDefinitionTask<StartTransaction>
{
    @Override
    public String getName()
    {
        return "START TRANSACTION";
    }

    @Override
    public ListenableFuture<?> execute(StartTransaction statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();
        if (!session.isClientTransactionSupport()) {
            throw new PrestoException(StandardErrorCode.INCOMPATIBLE_CLIENT, "Client does not support transactions");
        }
        if (session.getTransactionId().isPresent()) {
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Nested transactions not supported");
        }

        Optional<IsolationLevel> isolationLevel = extractIsolationLevel(statement);
        Optional<Boolean> readOnly = extractReadOnly(statement);

        TransactionId transactionId = transactionManager.beginTransaction(
                isolationLevel.orElse(TransactionManager.DEFAULT_ISOLATION),
                readOnly.orElse(TransactionManager.DEFAULT_READ_ONLY),
                false);

        stateMachine.setStartedTransactionId(transactionId);

        // Since the current session does not contain this new transaction ID, we need to manually mark it as inactive
        // when this statement completes.
        transactionManager.trySetInactive(transactionId);

        return immediateFuture(null);
    }

    @Override
    public boolean isTransactionControl()
    {
        return true;
    }

    private static Optional<IsolationLevel> extractIsolationLevel(StartTransaction startTransaction)
    {
        if (startTransaction.getTransactionModes().stream()
                .filter(Isolation.class::isInstance)
                .count() > 1) {
            throw new SemanticException(INVALID_TRANSACTION_MODE, startTransaction, "Multiple transaction isolation levels specified");
        }

        return startTransaction.getTransactionModes().stream()
                .filter(Isolation.class::isInstance)
                .map(Isolation.class::cast)
                .map(Isolation::getLevel)
                .map(StartTransactionTask::convertLevel)
                .findFirst();
    }

    private static Optional<Boolean> extractReadOnly(StartTransaction startTransaction)
    {
        if (startTransaction.getTransactionModes().stream()
                .filter(TransactionAccessMode.class::isInstance)
                .count() > 1) {
            throw new SemanticException(INVALID_TRANSACTION_MODE, startTransaction, "Multiple transaction read modes specified");
        }

        return startTransaction.getTransactionModes().stream()
                .filter(TransactionAccessMode.class::isInstance)
                .map(TransactionAccessMode.class::cast)
                .map(TransactionAccessMode::isReadOnly)
                .findFirst();
    }

    private static IsolationLevel convertLevel(Isolation.Level level)
    {
        switch (level) {
            case SERIALIZABLE:
                return IsolationLevel.SERIALIZABLE;
            case REPEATABLE_READ:
                return IsolationLevel.REPEATABLE_READ;
            case READ_COMMITTED:
                return IsolationLevel.READ_COMMITTED;
            case READ_UNCOMMITTED:
                return IsolationLevel.READ_UNCOMMITTED;
            default:
                throw new AssertionError("Unhandled isolation level: " + level);
        }
    }
}
