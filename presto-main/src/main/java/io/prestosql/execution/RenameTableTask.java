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
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.TableHandle;
import io.prestosql.security.AccessControl;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.RenameTable;
import io.prestosql.transaction.TransactionManager;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.metadata.MetadataUtil.createQualifiedObjectName;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_CATALOG;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static io.prestosql.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static io.prestosql.sql.analyzer.SemanticErrorCode.TABLE_ALREADY_EXISTS;

public class RenameTableTask
        implements DataDefinitionTask<RenameTable>
{
    @Override
    public String getName()
    {
        return "RENAME TABLE";
    }

    @Override
    public ListenableFuture<?> execute(RenameTable statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getSource());
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);
        if (!tableHandle.isPresent()) {
            throw new SemanticException(MISSING_TABLE, statement, "Table '%s' does not exist", tableName);
        }

        QualifiedObjectName target = createQualifiedObjectName(session, statement, statement.getTarget());
        if (!metadata.getCatalogHandle(session, target.getCatalogName()).isPresent()) {
            throw new SemanticException(MISSING_CATALOG, statement, "Target catalog '%s' does not exist", target.getCatalogName());
        }
        if (metadata.getTableHandle(session, target).isPresent()) {
            throw new SemanticException(TABLE_ALREADY_EXISTS, statement, "Target table '%s' already exists", target);
        }
        if (!tableName.getCatalogName().equals(target.getCatalogName())) {
            throw new SemanticException(NOT_SUPPORTED, statement, "Table rename across catalogs is not supported");
        }
        accessControl.checkCanRenameTable(session.getRequiredTransactionId(), session.getIdentity(), tableName, target);

        metadata.renameTable(session, tableHandle.get(), target);

        return immediateFuture(null);
    }
}
