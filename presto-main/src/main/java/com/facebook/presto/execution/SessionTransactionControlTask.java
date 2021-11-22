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
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Optional;

/**
 * Interface specifically for tasks needing QueryStateMachine for transaction and session management.
 * @param <T>
 */
public interface SessionTransactionControlTask<T extends Statement>
        extends DataDefinitionTask<T>
{
    String getName();

    ListenableFuture<?> execute(T statement, TransactionManager transactionManager, Metadata metadata,
                                AccessControl accessControl, List<Expression> parameters,
                                 QueryStateMachine stateMachine);

    default String explain(T statement, List<Expression> parameters)
    {
        if (statement instanceof Prepare) {
            return SqlFormatter.formatSql(statement, Optional.empty());
        }

        return SqlFormatter.formatSql(statement, Optional.of(parameters));
    }

    default boolean isTransactionControl()
    {
        return false;
    }

    default boolean isSessionControl()
    {
        return false;
    }
}
