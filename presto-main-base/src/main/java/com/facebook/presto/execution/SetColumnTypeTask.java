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
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SetColumnType;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_COLUMN;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

public class SetColumnTypeTask
        implements DDLDefinitionTask<SetColumnType>
{
    private final Metadata metadata;

    @Inject
    public SetColumnTypeTask(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public String getName()
    {
        return "SET COLUMN TYPE";
    }

    public ListenableFuture<Void> execute(SetColumnType statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, Session session, List<Expression> parameters, WarningCollector warningCollector, String query)
    {
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getTableName(), metadata);
        Optional<TableHandle> tableHandle = metadata.getMetadataResolver(session).getTableHandle(tableName);
        if (!tableHandle.isPresent()) {
            if (!statement.isTableExists()) {
                throw new SemanticException(MISSING_TABLE, statement, "Table '%s' does not exist", tableName);
            }
            return immediateFuture(null);
        }

        Optional<MaterializedViewDefinition> optionalMaterializedView = metadata.getMetadataResolver(session).getMaterializedView(tableName);
        if (optionalMaterializedView.isPresent()) {
            if (!statement.isTableExists()) {
                throw new SemanticException(SemanticErrorCode.NOT_SUPPORTED, statement, "'%s' is a materialized view, and alter column is not supported", tableName);
            }
            return immediateFuture(null);
        }

        accessControl.checkCanAlterColumn(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), tableName);

        TableHandle tableHandleOptional = tableHandle.get();
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandleOptional);
        ColumnHandle column = columnHandles.get(statement.getColumnName().getValue());
        if (column == null) {
            throw new SemanticException(MISSING_COLUMN, statement, "Column '%s' does not exist", statement.getColumnName());
        }
        metadata.setColumnType(session, tableHandleOptional, column, getColumnType(statement));

        return immediateFuture(null);
    }

    private Type getColumnType(SetColumnType statement)
    {
        Type type;
        try {
            type = metadata.getType(parseTypeSignature(statement.getType()));
        }
        catch (IllegalArgumentException e) {
            throw new SemanticException(TYPE_MISMATCH, statement, "Unknown type '%s' for column '%s'", statement.getType(), statement.getColumnName());
        }
        if (type.equals(UNKNOWN)) {
            throw new SemanticException(TYPE_MISMATCH, statement, "Unknown type '%s' for column '%s'", statement.getType(), statement.getColumnName());
        }
        return type;
    }
}
