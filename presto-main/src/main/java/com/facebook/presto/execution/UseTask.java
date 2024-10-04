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
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Use;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.facebook.presto.metadata.MetadataUtil.getConnectorIdOrThrow;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.CATALOG_NOT_SPECIFIED;
import static com.google.common.util.concurrent.Futures.immediateFuture;

public class UseTask
        implements SessionTransactionControlTask<Use>
{
    @Override
    public String getName()
    {
        return "USE";
    }

    @Override
    public ListenableFuture<?> execute(
            Use statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters)
    {
        Session session = stateMachine.getSession();

        checkCatalogAndSessionPresent(statement, session);

        checkAndSetCatalog(statement, metadata, stateMachine, session);

        stateMachine.setSetSchema(statement.getSchema().getValueLowerCase());

        return immediateFuture(null);
    }

    private void checkCatalogAndSessionPresent(Use statement, Session session)
    {
        if (!statement.getCatalog().isPresent() && !session.getCatalog().isPresent()) {
            throw new SemanticException(CATALOG_NOT_SPECIFIED, statement, "Catalog must be specified when session catalog is not set");
        }
    }

    private void checkAndSetCatalog(Use statement, Metadata metadata, QueryStateMachine stateMachine, Session session)
    {
        if (statement.getCatalog().isPresent()) {
            String catalog = statement.getCatalog().get().getValueLowerCase();
            getConnectorIdOrThrow(session, metadata, catalog);
            stateMachine.setSetCatalog(catalog);
        }
    }
}
