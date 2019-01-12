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
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.RenameColumn;
import io.prestosql.transaction.TransactionManager;

import java.util.List;
import java.util.Map;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.metadata.MetadataUtil.createQualifiedObjectName;
import static io.prestosql.sql.analyzer.SemanticErrorCode.COLUMN_ALREADY_EXISTS;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_COLUMN;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static io.prestosql.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static java.util.Locale.ENGLISH;

public class RenameColumnTask
        implements DataDefinitionTask<RenameColumn>
{
    @Override
    public String getName()
    {
        return "RENAME COLUMN";
    }

    @Override
    public ListenableFuture<?> execute(RenameColumn statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getTable());
        TableHandle tableHandle = metadata.getTableHandle(session, tableName)
                .orElseThrow(() -> new SemanticException(MISSING_TABLE, statement, "Table '%s' does not exist", tableName));

        String source = statement.getSource().getValue().toLowerCase(ENGLISH);
        String target = statement.getTarget().getValue().toLowerCase(ENGLISH);

        accessControl.checkCanRenameColumn(session.getRequiredTransactionId(), session.getIdentity(), tableName);

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
        ColumnHandle columnHandle = columnHandles.get(source);
        if (columnHandle == null) {
            throw new SemanticException(MISSING_COLUMN, statement, "Column '%s' does not exist", source);
        }

        if (columnHandles.containsKey(target)) {
            throw new SemanticException(COLUMN_ALREADY_EXISTS, statement, "Column '%s' already exists", target);
        }

        if (metadata.getColumnMetadata(session, tableHandle, columnHandle).isHidden()) {
            throw new SemanticException(NOT_SUPPORTED, statement, "Cannot rename hidden column");
        }

        metadata.renameColumn(session, tableHandle, columnHandle, target);

        return immediateFuture(null);
    }
}
