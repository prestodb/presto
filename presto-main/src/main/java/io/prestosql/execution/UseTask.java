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
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Use;
import io.prestosql.transaction.TransactionManager;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.sql.analyzer.SemanticErrorCode.CATALOG_NOT_SPECIFIED;
import static java.util.Locale.ENGLISH;

public class UseTask
        implements DataDefinitionTask<Use>
{
    @Override
    public String getName()
    {
        return "USE";
    }

    @Override
    public ListenableFuture<?> execute(Use statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();

        if (!statement.getCatalog().isPresent() && !session.getCatalog().isPresent()) {
            throw new SemanticException(CATALOG_NOT_SPECIFIED, statement, "Catalog must be specified when session catalog is not set");
        }

        if (statement.getCatalog().isPresent()) {
            String catalog = statement.getCatalog().get().getValue().toLowerCase(ENGLISH);
            if (!metadata.getCatalogHandle(session, catalog).isPresent()) {
                throw new PrestoException(NOT_FOUND, "Catalog does not exist: " + catalog);
            }
            stateMachine.setSetCatalog(catalog);
        }

        stateMachine.setSetSchema(statement.getSchema().getValue().toLowerCase(ENGLISH));

        return immediateFuture(null);
    }
}
