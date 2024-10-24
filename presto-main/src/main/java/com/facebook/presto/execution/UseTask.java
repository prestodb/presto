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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Use;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCatalogAccess;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.CATALOG_NOT_SPECIFIED;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Locale.ENGLISH;

public class UseTask
        implements SessionTransactionControlTask<Use>
{
    String catalog;
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

        TransactionId transactionId = session.getTransactionId().get();

        Identity identity = session.getIdentity();

        AccessControlContext context = session.getAccessControlContext();

        checkCatalogAndSessionPresent(statement, session);

        checkAndSetCatalog(statement, metadata, stateMachine, session);

        String schema = statement.getSchema().getValue();

        stateMachine.setSetSchema(schema);

        if (!hasCatalogAccess(identity, context, catalog, accessControl)) {
            denyCatalogAccess(catalog);
        }

        CatalogSchemaName name = new CatalogSchemaName(catalog, schema);

        MetadataResolver metadataresolver = metadata.getMetadataResolver(session);
        if (!metadataresolver.schemaExists(name)) {
            throw new PrestoException(NOT_FOUND, "Schema does not exist: " + name);
        }

        if (!hasSchemaAccess(transactionId, identity, context, catalog, schema, accessControl)) {
            throw new AccessDeniedException("Cannot access schema: " + name);
        }

        if (statement.getCatalog().isPresent()) {
            stateMachine.setSetCatalog(catalog);
        }

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
        if (statement.getCatalog().isPresent() || session.getCatalog().isPresent()) {
            catalog = statement.getCatalog()
                    .map(identifier -> identifier.getValue().toLowerCase(ENGLISH))
                    .orElseGet(() -> session.getCatalog().get());
            if (!metadata.getCatalogHandle(session, catalog).isPresent()) {
                throw new PrestoException(NOT_FOUND, "Catalog does not exist: " + catalog);
            }
            stateMachine.setSetCatalog(catalog);
        }
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
