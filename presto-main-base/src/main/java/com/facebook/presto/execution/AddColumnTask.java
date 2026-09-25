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
import static com.facebook.presto.execution.ColumnPositionUtil.toConnectorColumnPosition;
import static com.facebook.presto.execution.CreateTableTask.normalizeDerivedColumnSpec;
import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.metadata.MetadataUtil.getConnectorIdOrThrow;
import static com.facebook.presto.spi.ColumnMetadata.DEFAULT_VALUE_PROPERTY;
import static com.facebook.presto.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static com.facebook.presto.sql.NodeUtils.mapFromProperties;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.COLUMN_ALREADY_EXISTS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.sql.analyzer.utils.ParameterUtils.parameterExtractor;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.stream.Collectors.joining;

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

        ColumnDefinition element = statement.getColumn();
        List<String> nameParts = element.getName().getParts();

        if (nameParts.size() == 1) {
            // ---- top-level ADD COLUMN ----
            return executeAddColumn(statement, metadata, accessControl, session, parameters, tableName, tableHandle.get(), element);
        }
        else {
            // ---- nested ADD COLUMN (struct field) ----
            return executeAddField(statement, metadata, accessControl, session, tableName, tableHandle.get(), element);
        }
    }

    private ListenableFuture<?> executeAddColumn(
            AddColumn statement,
            Metadata metadata,
            AccessControl accessControl,
            Session session,
            List<Expression> parameters,
            QualifiedObjectName tableName,
            TableHandle tableHandle,
            ColumnDefinition element)
    {
        accessControl.checkCanAddColumns(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), tableName);

        ConnectorId connectorId = getConnectorIdOrThrow(session, metadata, tableName.getCatalogName());

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);

        String columnName = element.getName().getOriginalParts().get(0).getValue();
        String name = metadata.normalizeIdentifier(session, tableName.getCatalogName(), columnName);
        Type type;
        try {
            type = metadata.getType(parseTypeSignature(element.getType()));
        }
        catch (IllegalArgumentException | UnknownTypeException e) {
            throw new SemanticException(TYPE_MISMATCH, element, "Unknown type '%s' for column '%s'", element.getType(), columnName);
        }
        if (type.equals(UNKNOWN)) {
            throw new SemanticException(TYPE_MISMATCH, element, "Unknown type '%s' for column '%s'", element.getType(), columnName);
        }
        if (columnHandles.containsKey(name)) {
            if (!statement.isColumnNotExists()) {
                throw new SemanticException(COLUMN_ALREADY_EXISTS, statement, "Column '%s' already exists", columnName);
            }
            return immediateFuture(null);
        }
        if (!element.isNullable() && !metadata.getConnectorCapabilities(session, connectorId).contains(NOT_NULL_COLUMN_CONSTRAINT)) {
            throw new SemanticException(NOT_SUPPORTED, element, "Catalog '%s' does not support NOT NULL for column '%s'", connectorId.getCatalogName(), columnName);
        }

        Map<String, Expression> sqlProperties = mapFromProperties(element.getProperties());
        Map<String, Object> columnProperties = metadata.getColumnPropertyManager().getProperties(
                connectorId,
                tableName.getCatalogName(),
                sqlProperties,
                session,
                metadata,
                parameterExtractor(statement, parameters));

        if (element.getDefaultExpression().isPresent() && element.getDerivedColumnSpec().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, element, "Both default expression and derived column expression cannot be set on the same column %s.", columnName);
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

        ColumnPosition position = statement.getPosition()
                .map(p -> toConnectorColumnPosition(p, statement, metadata, session, tableName.getCatalogName(), tableHandle, columnHandles))
                .orElseGet(ColumnPosition.Last::new);

        metadata.addColumn(session, tableHandle, column, position);

        return immediateFuture(null);
    }

    private ListenableFuture<?> executeAddField(
            AddColumn statement,
            Metadata metadata,
            AccessControl accessControl,
            Session session,
            QualifiedObjectName tableName,
            TableHandle tableHandle,
            ColumnDefinition element)
    {
        accessControl.checkCanAlterColumn(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), tableName);

        // FIRST / AFTER positioning is not supported for nested fields
        if (statement.getPosition().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, statement,
                    "FIRST/AFTER is not supported for nested ADD COLUMN");
        }

        // NOT NULL is silently accepted by the grammar but has no SPI support for nested fields
        if (!element.isNullable()) {
            throw new SemanticException(NOT_SUPPORTED, element,
                    "NOT NULL constraint is not supported for nested ADD COLUMN");
        }

        // COMMENT is silently accepted by the grammar but has no SPI support for nested fields
        if (element.getComment().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, element,
                    "COMMENT is not supported for nested ADD COLUMN");
        }

        // DEFAULT is silently accepted by the grammar but has no SPI support for nested fields
        if (element.getDefaultExpression().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, element,
                    "DEFAULT is not supported for nested ADD COLUMN");
        }

        // GENERATED / AS derived-column expression is not supported for nested fields
        if (element.getDerivedColumnSpec().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, element,
                    "GENERATED/AS expression is not supported for nested ADD COLUMN");
        }

        // WITH column properties are not supported for nested fields
        if (!element.getProperties().isEmpty()) {
            throw new SemanticException(NOT_SUPPORTED, element,
                    "WITH properties are not supported for nested ADD COLUMN");
        }

        String displayName = element.getName().getOriginalParts().stream()
                .map(Identifier::getValue)
                .collect(joining("."));
        Type type;
        try {
            type = metadata.getType(parseTypeSignature(element.getType()));
        }
        catch (IllegalArgumentException | UnknownTypeException e) {
            throw new SemanticException(TYPE_MISMATCH, element, "Unknown type '%s' for column '%s'", element.getType(), displayName);
        }
        if (type.equals(UNKNOWN)) {
            throw new SemanticException(TYPE_MISMATCH, element, "Unknown type '%s' for column '%s'", element.getType(), displayName);
        }

        // columnPath is all parts except the last; fieldName is the last part.
        // Use getOriginalParts() + getValue() to preserve user-supplied casing before normalizeIdentifier.
        List<String> originalParts = element.getName().getOriginalParts().stream()
                .map(id -> id.getValue())
                .collect(toImmutableList());
        List<String> parentPath = originalParts.subList(0, originalParts.size() - 1).stream()
                .map(part -> metadata.normalizeIdentifier(session, tableName.getCatalogName(), part))
                .collect(toImmutableList());
        String fieldName = metadata.normalizeIdentifier(session, tableName.getCatalogName(),
                originalParts.get(originalParts.size() - 1));

        metadata.addField(session, tableHandle, parentPath, fieldName, type, statement.isColumnNotExists());

        return immediateFuture(null);
    }
}
