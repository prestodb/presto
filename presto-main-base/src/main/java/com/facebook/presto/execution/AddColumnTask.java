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
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.ColumnPosition;
import com.facebook.presto.spi.derivedcolumns.DerivedColumnSpec;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.spi.type.UnknownTypeException;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.tree.AddColumn;
import com.facebook.presto.sql.tree.ColumnDefinition;
import com.facebook.presto.sql.tree.ColumnPosition.After;
import com.facebook.presto.sql.tree.ColumnPosition.First;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.execution.CreateTableTask.normalizeDerivedColumnSpec;
import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.metadata.MetadataUtil.getConnectorIdOrThrow;
import static com.facebook.presto.spi.ColumnMetadata.DEFAULT_VALUE_PROPERTY;
import static com.facebook.presto.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static com.facebook.presto.sql.NodeUtils.mapFromProperties;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.COLUMN_ALREADY_EXISTS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_COLUMN;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.sql.analyzer.utils.ParameterUtils.parameterExtractor;
import static com.google.common.util.concurrent.Futures.immediateFuture;

public class AddColumnTask
        implements DDLDefinitionTask<AddColumn>
{
    @Override
    public String getName()
    {
        return "ADD COLUMN";
    }

    @Override
    public ListenableFuture<?> execute(AddColumn statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, Session session, List<Expression> parameters, WarningCollector warningCollector, String query)
    {
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getName(), metadata);
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
                throw new SemanticException(NOT_SUPPORTED, statement, "'%s' is a materialized view, and add column is not supported", tableName);
            }
            return immediateFuture(null);
        }

        ConnectorId connectorId = getConnectorIdOrThrow(session, metadata, tableName.getCatalogName());

        accessControl.checkCanAddColumns(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), tableName);

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle.get());

        ColumnDefinition element = statement.getColumn();
        Type type;
        try {
            type = metadata.getType(parseTypeSignature(element.getType()));
        }
        catch (IllegalArgumentException | UnknownTypeException e) {
            throw new SemanticException(TYPE_MISMATCH, element, "Unknown type '%s' for column '%s'", element.getType(), element.getName());
        }
        if (type.equals(UNKNOWN)) {
            throw new SemanticException(TYPE_MISMATCH, element, "Unknown type '%s' for column '%s'", element.getType(), element.getName());
        }
        if (columnHandles.containsKey(element.getName().getValueLowerCase())) {
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
                parameterExtractor(statement, parameters));

        Identifier columnIdentifier = element.getName();
        String name = metadata.normalizeIdentifier(session, tableName.getCatalogName(), columnIdentifier.getValue());
        if (element.getDefaultExpression().isPresent() && element.getDerivedColumnSpec().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, element, "Both default expression and derived column expression cannot be set on the same column %s.", element.getName());
        }
        // Handle default expression if present
        if (element.getDefaultExpression().isPresent()) {
            Map<String, Object> updatedProperties = new java.util.HashMap<>(columnProperties);
            Expression defaultExpr = element.getDefaultExpression().get();
            Object defaultValue = ExpressionInterpreter.evaluateConstantExpression(defaultExpr, type, metadata, session, ImmutableMap.of());
            updatedProperties.put(DEFAULT_VALUE_PROPERTY, defaultValue);
            columnProperties = updatedProperties;
        }
        Optional<DerivedColumnSpec> derivedColumnSpec = normalizeDerivedColumnSpec(element, name);
        ColumnMetadata column = ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setNullable(element.isNullable())
                .setComment(element.getComment().orElse(null))
                .setProperties(columnProperties)
                .setDerivedColumnSpec(derivedColumnSpec)
                .build();

        ColumnPosition position = toConnectorColumnPosition(statement, metadata, session, tableName.getCatalogName(), tableHandle.get(), columnHandles);

        metadata.addColumn(session, tableHandle.get(), column, position);

        return immediateFuture(null);
    }

    /**
     * Converts the optional {@code FIRST | AFTER <column>} clause of the statement into the connector
     * representation. An absent clause becomes {@link ColumnPosition.Last}, so the column is appended,
     * which is the pre-existing behavior.
     * <p>
     * The two {@code ColumnPosition} types in scope here are distinct: the unqualified {@code First} and
     * {@code After} are {@link com.facebook.presto.sql.tree.ColumnPosition} from the statement, while
     * {@code ColumnPosition.First} and the like are the {@link ColumnPosition} handed to the connector.
     */
    private static ColumnPosition toConnectorColumnPosition(
            AddColumn statement,
            Metadata metadata,
            Session session,
            String catalogName,
            TableHandle tableHandle,
            Map<String, ColumnHandle> columnHandles)
    {
        return statement.getPosition()
                .<ColumnPosition>map(position -> {
                    if (position instanceof First) {
                        return new ColumnPosition.First();
                    }
                    if (position instanceof After) {
                        Identifier afterIdentifier = ((After) position).getColumn();
                        String afterColumn = metadata.normalizeIdentifier(session, catalogName, afterIdentifier.getValue());
                        ColumnHandle afterColumnHandle = columnHandles.get(afterColumn);
                        if (afterColumnHandle == null) {
                            throw new SemanticException(MISSING_COLUMN, statement, "Column '%s' does not exist", afterIdentifier.getValue());
                        }
                        // A hidden column, such as a connector's synthesized "$path", has no place in the table's
                        // column order, so it cannot be positioned after even though getColumnHandles exposes it
                        if (metadata.getColumnMetadata(session, tableHandle, afterColumnHandle).isHidden()) {
                            throw new SemanticException(NOT_SUPPORTED, statement, "Cannot add a column after hidden column '%s'", afterIdentifier.getValue());
                        }
                        return new ColumnPosition.After(afterColumn);
                    }
                    throw new SemanticException(NOT_SUPPORTED, statement, "Unsupported column position: %s", position);
                })
                .orElseGet(ColumnPosition.Last::new);
    }
}
