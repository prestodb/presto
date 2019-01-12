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
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.tree.DropSchema;
import io.prestosql.sql.tree.Expression;
import io.prestosql.transaction.TransactionManager;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.metadata.MetadataUtil.createCatalogSchemaName;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_SCHEMA;

public class DropSchemaTask
        implements DataDefinitionTask<DropSchema>
{
    @Override
    public String getName()
    {
        return "DROP SCHEMA";
    }

    @Override
    public String explain(DropSchema statement, List<Expression> parameters)
    {
        return "DROP SCHEMA " + statement.getSchemaName();
    }

    @Override
    public ListenableFuture<?> execute(DropSchema statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        if (statement.isCascade()) {
            throw new PrestoException(NOT_SUPPORTED, "CASCADE is not yet supported for DROP SCHEMA");
        }

        Session session = stateMachine.getSession();
        CatalogSchemaName schema = createCatalogSchemaName(session, statement, Optional.of(statement.getSchemaName()));

        if (!metadata.schemaExists(session, schema)) {
            if (!statement.isExists()) {
                throw new SemanticException(MISSING_SCHEMA, statement, "Schema '%s' does not exist", schema);
            }
            return immediateFuture(null);
        }

        accessControl.checkCanDropSchema(session.getRequiredTransactionId(), session.getIdentity(), schema);

        metadata.dropSchema(session, schema);

        return immediateFuture(null);
    }
}
