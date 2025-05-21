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
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Use;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.facebook.presto.metadata.MetadataUtil.getConnectorIdOrThrow;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCatalogAccess;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.CATALOG_NOT_SPECIFIED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_SCHEMA;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.String.format;

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
            List<Expression> parameters,
            String query)
    {
        Session session = stateMachine.getSession();

        checkCatalogAndSessionPresent(statement, session);

        checkAndSetCatalog(statement, metadata, stateMachine, session, accessControl);

        checkAndSetSchema(statement, metadata, stateMachine, session, accessControl);

        return immediateFuture(null);
    }

    private void checkCatalogAndSessionPresent(Use statement, Session session)
    {
        if (!statement.getCatalog().isPresent() && !session.getCatalog().isPresent()) {
            throw new SemanticException(CATALOG_NOT_SPECIFIED, statement, "Catalog must be specified when session catalog is not set");
        }
    }

    private void checkAndSetCatalog(Use statement, Metadata metadata, QueryStateMachine stateMachine, Session session, AccessControl accessControl)
    {
        String catalog = statement.getCatalog()
                .map(Identifier::getValueLowerCase)
                .orElseGet(() -> session.getCatalog().map(String::toLowerCase).get());
        getConnectorIdOrThrow(session, metadata, catalog);
        if (!hasCatalogAccess(session.getIdentity(), session.getAccessControlContext(), catalog, accessControl)) {
            denyCatalogAccess(catalog);
        }
        stateMachine.setSetCatalog(catalog);
    }

    private void checkAndSetSchema(Use statement, Metadata metadata, QueryStateMachine stateMachine, Session session, AccessControl accessControl)
    {
        String catalog = statement.getCatalog()
                .map(Identifier::getValueLowerCase)
                .orElseGet(() -> session.getCatalog().map(String::toLowerCase).get());

        Identifier schemaIdentifier = statement.getSchema();
        String schema = metadata.normalizeIdentifier(session, catalog, schemaIdentifier.getValue());
        if (!metadata.getMetadataResolver(session).schemaExists(new CatalogSchemaName(catalog, schema))) {
            throw new SemanticException(MISSING_SCHEMA, format("Schema does not exist: %s.%s", catalog, schema));
        }
        if (!hasSchemaAccess(session.getTransactionId().get(), session.getIdentity(), session.getAccessControlContext(), catalog, schema, accessControl)) {
            throw new AccessDeniedException("Cannot access schema: " + new CatalogSchemaName(catalog, schema));
        }
        stateMachine.setSetSchema(schema);
    }

    private boolean hasCatalogAccess(Identity identity, AccessControlContext context, String catalog, AccessControl accessControl)
    {
        return !accessControl.filterCatalogs(identity, context, ImmutableSet.of(catalog)).isEmpty();
    }

    private boolean hasSchemaAccess(TransactionId transactionId, Identity identity, AccessControlContext context, String catalog, String schema, AccessControl accessControl)
    {
        return !accessControl.filterSchemas(transactionId, identity, context, catalog, ImmutableSet.of(schema)).isEmpty();
    }
}
