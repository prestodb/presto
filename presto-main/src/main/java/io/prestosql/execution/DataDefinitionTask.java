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
import io.prestosql.metadata.Metadata;
import io.prestosql.security.AccessControl;
import io.prestosql.sql.SqlFormatter;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Prepare;
import io.prestosql.sql.tree.Statement;
import io.prestosql.transaction.TransactionManager;

import java.util.List;
import java.util.Optional;

public interface DataDefinitionTask<T extends Statement>
{
    String getName();

    ListenableFuture<?> execute(T statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters);

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
}
