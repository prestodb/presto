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
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.RenameSchema;
import io.prestosql.transaction.TransactionManager;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.metadata.MetadataUtil.createCatalogSchemaName;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_SCHEMA;
import static io.prestosql.sql.analyzer.SemanticErrorCode.SCHEMA_ALREADY_EXISTS;

public class RenameSchemaTask
        implements DataDefinitionTask<RenameSchema>
{
    @Override
    public String getName()
    {
        return "RENAME SCHEMA";
    }

    @Override
    public ListenableFuture<?> execute(RenameSchema statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();
        CatalogSchemaName source = createCatalogSchemaName(session, statement, Optional.of(statement.getSource()));
        CatalogSchemaName target = new CatalogSchemaName(source.getCatalogName(), statement.getTarget().getValue());

        if (!metadata.schemaExists(session, source)) {
            throw new SemanticException(MISSING_SCHEMA, statement, "Schema '%s' does not exist", source);
        }

        if (metadata.schemaExists(session, target)) {
            throw new SemanticException(SCHEMA_ALREADY_EXISTS, statement, "Target schema '%s' already exists", target);
        }

        accessControl.checkCanRenameSchema(session.getRequiredTransactionId(), session.getIdentity(), source, statement.getTarget().getValue());

        metadata.renameSchema(session, source, statement.getTarget().getValue());

        return immediateFuture(null);
    }
}
