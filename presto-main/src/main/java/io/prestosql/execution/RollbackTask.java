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
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Rollback;
import io.prestosql.transaction.TransactionId;
import io.prestosql.transaction.TransactionManager;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.spi.StandardErrorCode.NOT_IN_TRANSACTION;

public class RollbackTask
        implements DataDefinitionTask<Rollback>
{
    @Override
    public String getName()
    {
        return "ROLLBACK";
    }

    @Override
    public ListenableFuture<?> execute(Rollback statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();
        if (!session.getTransactionId().isPresent()) {
            throw new PrestoException(NOT_IN_TRANSACTION, "No transaction in progress");
        }
        TransactionId transactionId = session.getTransactionId().get();

        stateMachine.clearTransactionId();
        transactionManager.asyncAbort(transactionId);
        return immediateFuture(null);
    }

    @Override
    public boolean isTransactionControl()
    {
        return true;
    }
}
