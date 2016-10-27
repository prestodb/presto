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

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ResetSession;
import com.facebook.presto.transaction.TransactionManager;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_CATALOG;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class ResetSessionTask
        implements DataDefinitionTask<ResetSession>
{
    @Override
    public String getName()
    {
        return "RESET SESSION";
    }

    @Override
    public CompletableFuture<?> execute(ResetSession statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        List<String> parts = statement.getName().getParts();
        if (parts.size() > 2) {
            throw new SemanticException(INVALID_SESSION_PROPERTY, statement, "Invalid session property '%s'", statement.getName());
        }

        // validate the property name
        if (parts.size() == 1) {
            metadata.getSessionPropertyManager().getSystemSessionPropertyMetadata(parts.get(0))
                    .orElseThrow(() -> new SemanticException(INVALID_SESSION_PROPERTY, statement, "Session property %s does not exist", statement.getName()));
        }
        else {
            ConnectorId connectorId = metadata.getCatalogHandle(stateMachine.getSession(), parts.get(0))
                    .orElseThrow(() -> new SemanticException(MISSING_CATALOG, statement, "Catalog %s does not exist", parts.get(0)));
            metadata.getSessionPropertyManager().getConnectorSessionPropertyMetadata(connectorId, parts.get(1))
                    .orElseThrow(() -> new SemanticException(INVALID_SESSION_PROPERTY, statement, "Session property %s does not exist", statement.getName()));
        }

        stateMachine.addResetSessionProperties(statement.getName().toString());

        return completedFuture(null);
    }
}
