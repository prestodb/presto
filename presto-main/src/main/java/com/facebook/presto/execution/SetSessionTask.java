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
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.facebook.presto.metadata.SessionPropertyManager.evaluatePropertyValue;
import static com.facebook.presto.metadata.SessionPropertyManager.serializeSessionProperty;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_CATALOG;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.String.format;

public class SetSessionTask
        implements DataDefinitionTask<SetSession>
{
    @Override
    public String getName()
    {
        return "SET SESSION";
    }

    @Override
    public ListenableFuture<?> execute(SetSession statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();
        QualifiedName propertyName = statement.getName();
        List<String> parts = propertyName.getParts();
        if (parts.size() > 2) {
            throw new SemanticException(INVALID_SESSION_PROPERTY, statement, "Invalid session property '%s'", propertyName);
        }

        // validate the property name
        PropertyMetadata<?> propertyMetadata;
        if (parts.size() == 1) {
            accessControl.checkCanSetSystemSessionProperty(session.getIdentity(), parts.get(0));
            propertyMetadata = metadata.getSessionPropertyManager().getSystemSessionPropertyMetadata(parts.get(0))
                    .orElseThrow(() -> new SemanticException(INVALID_SESSION_PROPERTY, statement, "Session property %s does not exist", statement.getName()));
        }
        else {
            ConnectorId connectorId = metadata.getCatalogHandle(stateMachine.getSession(), parts.get(0))
                    .orElseThrow(() -> new SemanticException(MISSING_CATALOG, statement, "Catalog %s does not exist", parts.get(0)));
            accessControl.checkCanSetCatalogSessionProperty(session.getRequiredTransactionId(), session.getIdentity(), parts.get(0), parts.get(1));
            propertyMetadata = metadata.getSessionPropertyManager().getConnectorSessionPropertyMetadata(connectorId, parts.get(1))
                    .orElseThrow(() -> new SemanticException(INVALID_SESSION_PROPERTY, statement, "Session property %s does not exist", statement.getName()));
        }

        Type type = propertyMetadata.getSqlType();
        Object objectValue;

        try {
            objectValue = evaluatePropertyValue(statement.getValue(), type, session, metadata, parameters);
        }
        catch (SemanticException e) {
            throw new PrestoException(StandardErrorCode.INVALID_SESSION_PROPERTY,
                    format("Unable to set session property '%s' to '%s': %s", propertyName, statement.getValue(), e.getMessage()));
        }

        String value = serializeSessionProperty(type, objectValue);

        // verify the SQL value can be decoded by the property
        stateMachine.addSetSessionProperties(propertyName.toString(), value);

        return immediateFuture(null);
    }
}
