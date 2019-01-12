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
import io.prestosql.connector.ConnectorId;
import io.prestosql.metadata.Metadata;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.tree.CreateSchema;
import io.prestosql.sql.tree.Expression;
import io.prestosql.transaction.TransactionManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.metadata.MetadataUtil.createCatalogSchemaName;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.sql.NodeUtils.mapFromProperties;
import static io.prestosql.sql.analyzer.SemanticErrorCode.SCHEMA_ALREADY_EXISTS;

public class CreateSchemaTask
        implements DataDefinitionTask<CreateSchema>
{
    @Override
    public String getName()
    {
        return "CREATE SCHEMA";
    }

    @Override
    public String explain(CreateSchema statement, List<Expression> parameters)
    {
        return "CREATE SCHEMA " + statement.getSchemaName();
    }

    @Override
    public ListenableFuture<?> execute(CreateSchema statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();
        CatalogSchemaName schema = createCatalogSchemaName(session, statement, Optional.of(statement.getSchemaName()));

        // TODO: validate that catalog exists

        accessControl.checkCanCreateSchema(session.getRequiredTransactionId(), session.getIdentity(), schema);

        if (metadata.schemaExists(session, schema)) {
            if (!statement.isNotExists()) {
                throw new SemanticException(SCHEMA_ALREADY_EXISTS, statement, "Schema '%s' already exists", schema);
            }
            return immediateFuture(null);
        }

        ConnectorId connectorId = metadata.getCatalogHandle(session, schema.getCatalogName())
                .orElseThrow(() -> new PrestoException(NOT_FOUND, "Catalog does not exist: " + schema.getCatalogName()));

        Map<String, Object> properties = metadata.getSchemaPropertyManager().getProperties(
                connectorId,
                schema.getCatalogName(),
                mapFromProperties(statement.getProperties()),
                session,
                metadata,
                parameters);

        metadata.createSchema(session, schema, properties);

        return immediateFuture(null);
    }
}
