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
import com.facebook.presto.client.ClientCapabilities;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.SqlPath;
import com.facebook.presto.sql.SqlPathElement;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SetPath;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.CATALOG_NOT_SPECIFIED;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Locale.ENGLISH;

public class SetPathTask
        implements DataDefinitionTask<SetPath>
{
    @Override
    public String getName()
    {
        return "SET PATH";
    }

    @Override
    public ListenableFuture<?> execute(
            SetPath statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters)
    {
        Session session = stateMachine.getSession();

        if (!session.getClientCapabilities().contains(ClientCapabilities.PATH.toString())) {
            throw new PrestoException(NOT_SUPPORTED, "SET PATH not supported by client");
        }

        // convert to IR before setting HTTP headers - ensures that the representations of all path objects outside the parser remain consistent
        SqlPath sqlPath = new SqlPath(Optional.of(statement.getPathSpecification().toString()));

        for (SqlPathElement element : sqlPath.getParsedPath()) {
            if (!element.getCatalog().isPresent() && !session.getCatalog().isPresent()) {
                throw new SemanticException(CATALOG_NOT_SPECIFIED, statement, "Catalog must be specified for each path element when session catalog is not set");
            }

            element.getCatalog().ifPresent(catalog -> {
                String catalogName = catalog.getValue().toLowerCase(ENGLISH);
                if (!metadata.getCatalogHandle(session, catalogName).isPresent()) {
                    throw new PrestoException(NOT_FOUND, "Catalog does not exist: " + catalogName);
                }
            });
        }
        stateMachine.setSetPath(sqlPath.toString());
        return immediateFuture(null);
    }
}
