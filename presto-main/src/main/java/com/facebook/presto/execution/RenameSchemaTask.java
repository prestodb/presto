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
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.RenameSchema;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.createCatalogSchemaName;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_SCHEMA;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.SCHEMA_ALREADY_EXISTS;
import static com.google.common.util.concurrent.Futures.immediateFuture;

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
        CatalogSchemaName target = new CatalogSchemaName(source.getCatalogName(), statement.getTarget());

        if (!metadata.schemaExists(session, source)) {
            throw new SemanticException(MISSING_SCHEMA, statement, "Schema '%s' does not exist", source);
        }

        if (metadata.schemaExists(session, target)) {
            throw new SemanticException(SCHEMA_ALREADY_EXISTS, statement, "Target schema '%s' already exists", target);
        }

        accessControl.checkCanRenameSchema(session.getRequiredTransactionId(), session.getIdentity(), source, statement.getTarget());

        metadata.renameSchema(session, source, statement.getTarget());

        return immediateFuture(null);
    }
}
