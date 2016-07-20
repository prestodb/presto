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
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.CatalogSchemaName;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.CreateSchema;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.transaction.TransactionManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.metadata.MetadataUtil.createCatalogSchemaName;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.SCHEMA_ALREADY_EXISTS;
import static java.util.concurrent.CompletableFuture.completedFuture;

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
    public CompletableFuture<?> execute(CreateSchema statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();
        CatalogSchemaName schema = createCatalogSchemaName(session, statement, Optional.of(statement.getSchemaName()));

        // TODO: validate that catalog exists

        accessControl.checkCanCreateSchema(session.getRequiredTransactionId(), session.getIdentity(), schema);

        if (metadata.schemaExists(session, schema)) {
            if (!statement.isNotExists()) {
                throw new SemanticException(SCHEMA_ALREADY_EXISTS, statement, "Schema '%s' already exists", schema);
            }
            return completedFuture(null);
        }

        Map<String, Object> properties = metadata.getSchemaPropertyManager().getProperties(
                schema.getCatalogName(),
                statement.getProperties(),
                session,
                metadata,
                parameters);

        metadata.createSchema(session, schema, properties);

        return completedFuture(null);
    }
}
