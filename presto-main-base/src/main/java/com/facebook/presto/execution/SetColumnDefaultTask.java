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
import com.facebook.presto.metadata.Catalog.CatalogContext;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SetColumnDefault;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_COLUMN;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.google.common.util.concurrent.Futures.immediateFuture;

public class SetColumnDefaultTask
        implements DDLDefinitionTask<SetColumnDefault>
{
    @Override
    public String getName()
    {
        return "SET COLUMN DEFAULT";
    }

    @Override
    public ListenableFuture<?> execute(SetColumnDefault statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, Session session, List<Expression> parameters, WarningCollector warningCollector, String query)
    {
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getTable(), metadata);
        Optional<TableHandle> tableHandleOptional = metadata.getMetadataResolver(session).getTableHandle(tableName);
        if (!tableHandleOptional.isPresent()) {
            if (!statement.isTableExists()) {
                throw new SemanticException(MISSING_TABLE, statement, "Table '%s' does not exist", tableName);
            }
            return immediateFuture(null);
        }

        Optional<MaterializedViewDefinition> optionalMaterializedView = metadata.getMetadataResolver(session).getMaterializedView(tableName);
        if (optionalMaterializedView.isPresent()) {
            if (!statement.isTableExists()) {
                throw new SemanticException(NOT_SUPPORTED, statement, "'%s' is a materialized view, and set column default is not supported", tableName);
            }
            return immediateFuture(null);
        }
        if (!isIcebergTable(metadata, session, tableName)) {
            String catalogName = tableName.getCatalogName();
            String connectorName = getConnectorName(metadata, session, tableName);
            throw new SemanticException(NOT_SUPPORTED, statement,
                    "Catalog '%s' (connector: %s) does not support SET COLUMN DEFAULT. This feature is currently only supported for Iceberg v3 tables.", catalogName, connectorName);
        }

        TableHandle tableHandle = tableHandleOptional.get();
        String columnName = metadata.normalizeIdentifier(session, tableName.getCatalogName(), statement.getColumn().getValue());

        accessControl.checkCanRenameColumn(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), tableName);

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
        ColumnHandle columnHandle = columnHandles.get(columnName);
        if (columnHandle == null) {
            throw new SemanticException(MISSING_COLUMN, statement, "Column '%s' does not exist", columnName);
        }

        ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, tableHandle, columnHandle);
        if (columnMetadata.isHidden()) {
            throw new SemanticException(NOT_SUPPORTED, statement, "Cannot set default on hidden column");
        }

        // Evaluate the default expression
        Type columnType = columnMetadata.getType();
        Expression defaultExpr = statement.getDefaultExpression();
        Object defaultValue = ExpressionInterpreter.evaluateConstantExpression(defaultExpr, columnType, metadata, session, ImmutableMap.of());

        // Call the metadata method to set the write-default
        metadata.setColumnDefault(session, tableHandle, columnName, defaultValue);

        return immediateFuture(null);
    }

    private static String getConnectorName(Metadata metadata, Session session, QualifiedObjectName tableName)
    {
        String catalogName = tableName.getCatalogName();
        CatalogContext catalogContext = metadata.getCatalogNamesWithConnectorContext(session).get(catalogName);
        return catalogContext != null ? catalogContext.getConnectorName() : "unknown";
    }

    private static boolean isIcebergTable(Metadata metadata, Session session, QualifiedObjectName tableName)
    {
        return "iceberg".equalsIgnoreCase(getConnectorName(metadata, session, tableName));
    }
}
