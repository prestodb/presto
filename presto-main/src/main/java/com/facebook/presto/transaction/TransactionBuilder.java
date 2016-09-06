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
package com.facebook.presto.transaction;

import com.facebook.presto.Session;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.transaction.IsolationLevel;

import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TransactionBuilder
{
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private IsolationLevel isolationLevel = TransactionManager.DEFAULT_ISOLATION;
    private boolean readOnly = TransactionManager.DEFAULT_READ_ONLY;
    private boolean singleStatement;

    private TransactionBuilder(TransactionManager transactionManager, AccessControl accessControl)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    public static TransactionBuilder transaction(TransactionManager transactionManager, AccessControl accessControl)
    {
        return new TransactionBuilder(transactionManager, accessControl);
    }

    public TransactionBuilder withIsolationLevel(IsolationLevel isolationLevel)
    {
        this.isolationLevel = requireNonNull(isolationLevel, "isolationLevel is null");
        return this;
    }

    public TransactionBuilder readUncommitted()
    {
        return withIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
    }

    public TransactionBuilder readCommitted()
    {
        return withIsolationLevel(IsolationLevel.READ_COMMITTED);
    }

    public TransactionBuilder repeatableRead()
    {
        return withIsolationLevel(IsolationLevel.REPEATABLE_READ);
    }

    public TransactionBuilder serializable()
    {
        return withIsolationLevel(IsolationLevel.SERIALIZABLE);
    }

    public TransactionBuilder readOnly()
    {
        readOnly = true;
        return this;
    }

    public TransactionBuilder singleStatement()
    {
        singleStatement = true;
        return this;
    }

    public void execute(Consumer<TransactionId> callback)
    {
        requireNonNull(callback, "callback is null");

        execute(transactionId -> {
            callback.accept(transactionId);
            return null;
        });
    }

    public <T> T execute(Function<TransactionId, T> callback)
    {
        requireNonNull(callback, "callback is null");

        TransactionId transactionId = transactionManager.beginTransaction(isolationLevel, readOnly, singleStatement);

        boolean success = false;
        try {
            T result = callback.apply(transactionId);
            success = true;
            return result;
        }
        finally {
            if (success) {
                transactionManager.asyncCommit(transactionId).join();
            }
            else {
                transactionManager.asyncAbort(transactionId);
            }
        }
    }

    public void execute(Session session, Consumer<Session> callback)
    {
        requireNonNull(session, "session is null");
        requireNonNull(callback, "callback is null");

        execute(session, transactionSession -> {
            callback.accept(transactionSession);
            return null;
        });
    }

    public <T> T execute(Session session, Function<Session, T> callback)
    {
        requireNonNull(session, "session is null");
        requireNonNull(callback, "callback is null");

        boolean managedTransaction = !session.getTransactionId().isPresent();

        Session transactionSession;
        if (managedTransaction) {
            TransactionId transactionId = transactionManager.beginTransaction(isolationLevel, readOnly, singleStatement);
            transactionSession = session.beginTransactionId(transactionId, transactionManager, accessControl);
        }
        else {
            // Check if we can merge with the existing transaction
            TransactionInfo transactionInfo = transactionManager.getTransactionInfo(session.getTransactionId().get());
            checkState(transactionInfo.getIsolationLevel().meetsRequirementOf(isolationLevel), "Cannot provide %s isolation with existing transaction isolation: %s", isolationLevel, transactionInfo.getIsolationLevel());
            checkState(!transactionInfo.isReadOnly() || readOnly, "Cannot provide read-write semantics with existing read-only transaction");
            checkState(!transactionInfo.isAutoCommitContext() && !singleStatement, "Cannot combine auto commit transactions");
            transactionSession = session;
        }

        boolean success = false;
        try {
            T result = callback.apply(transactionSession);
            success = true;
            return result;
        }
        finally {
            if (managedTransaction) {
                if (success) {
                    transactionManager.asyncCommit(transactionSession.getTransactionId().get()).join();
                }
                else {
                    transactionManager.asyncAbort(transactionSession.getTransactionId().get());
                }
            }
        }
    }
}
