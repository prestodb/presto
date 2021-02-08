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
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.AddColumn;
import com.facebook.presto.sql.tree.ColumnDefinition;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static com.facebook.presto.sql.NodeUtils.mapFromProperties;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.COLUMN_ALREADY_EXISTS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Locale.ENGLISH;

public class AddColumnTask
        implements DataDefinitionTask<AddColumn>
{
    @Override
    public String getName()
    {
        return "ADD COLUMN";
    }

    @Override
    public ListenableFuture<?> execute(AddColumn statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getName());
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);
        if (!tableHandle.isPresent()) {
            if (!statement.isTableExists()) {
                throw new SemanticException(MISSING_TABLE, statement, "Table '%s' does not exist", tableName);
            }
            return immediateFuture(null);
        }

        ConnectorId connectorId = metadata.getCatalogHandle(session, tableName.getCatalogName())
                .orElseThrow(() -> new PrestoException(NOT_FOUND, "Catalog does not exist: " + tableName.getCatalogName()));

        accessControl.checkCanAddColumns(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), tableName);

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle.get());

        ColumnDefinition element = statement.getColumn();
        Type type;
        try {
            type = metadata.getType(parseTypeSignature(element.getType()));
        }
        catch (IllegalArgumentException e) {
            throw new SemanticException(TYPE_MISMATCH, element, "Unknown type '%s' for column '%s'", element.getType(), element.getName());
        }
        if (type.equals(UNKNOWN)) {
            throw new SemanticException(TYPE_MISMATCH, element, "Unknown type '%s' for column '%s'", element.getType(), element.getName());
        }
        if (columnHandles.containsKey(element.getName().getValue().toLowerCase(ENGLISH))) {
            if (!statement.isColumnNotExists()) {
                throw new SemanticException(COLUMN_ALREADY_EXISTS, statement, "Column '%s' already exists", element.getName());
            }
            return immediateFuture(null);
        }
        if (!element.isNullable() && !metadata.getConnectorCapabilities(session, connectorId).contains(NOT_NULL_COLUMN_CONSTRAINT)) {
            throw new SemanticException(NOT_SUPPORTED, element, "Catalog '%s' does not support NOT NULL for column '%s'", connectorId.getCatalogName(), element.getName());
        }

        Map<String, Expression> sqlProperties = mapFromProperties(element.getProperties());
        Map<String, Object> columnProperties = metadata.getColumnPropertyManager().getProperties(
                connectorId,
                tableName.getCatalogName(),
                sqlProperties,
                session,
                metadata,
                parameters);

        ColumnMetadata column = new ColumnMetadata(
                element.getName().getValue(),
                type,
                element.isNullable(), element.getComment().orElse(null),
                null,
                false,
                columnProperties);

        metadata.addColumn(session, tableHandle.get(), column);

        return immediateFuture(null);
    }
}
