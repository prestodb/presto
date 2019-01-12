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
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.DropColumn;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_COLUMN;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Locale.ENGLISH;

public class DropColumnTask
        implements DataDefinitionTask<DropColumn>
{
    @Override
    public String getName()
    {
        return "DROP COLUMN";
    }

    @Override
    public ListenableFuture<?> execute(DropColumn statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getTable());
        TableHandle tableHandle = metadata.getTableHandle(session, tableName)
                .orElseThrow(() -> new SemanticException(MISSING_TABLE, statement, "Table '%s' does not exist", tableName));

        String column = statement.getColumn().getValue().toLowerCase(ENGLISH);

        accessControl.checkCanDropColumn(session.getRequiredTransactionId(), session.getIdentity(), tableName);

        ColumnHandle columnHandle = metadata.getColumnHandles(session, tableHandle).get(column);
        if (columnHandle == null) {
            throw new SemanticException(MISSING_COLUMN, statement, "Column '%s' does not exist", column);
        }

        if (metadata.getColumnMetadata(session, tableHandle, columnHandle).isHidden()) {
            throw new SemanticException(NOT_SUPPORTED, statement, "Cannot drop hidden column");
        }

        if (metadata.getTableMetadata(session, tableHandle).getColumns().stream()
                .filter(info -> !info.isHidden()).count() <= 1) {
            throw new SemanticException(NOT_SUPPORTED, statement, "Cannot drop the only column in a table");
        }

        metadata.dropColumn(session, tableHandle, columnHandle);

        return immediateFuture(null);
    }
}
