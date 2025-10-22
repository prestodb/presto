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
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableMetadata;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.constraints.PrimaryKeyConstraint;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.spi.type.UnknownTypeException;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.ColumnDefinition;
import com.facebook.presto.sql.tree.ConstraintSpecification;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LikeClause;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.TableElement;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.execution.AddConstraintTask.convertToTableConstraint;
import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.metadata.MetadataUtil.getConnectorIdOrThrow;
import static com.facebook.presto.metadata.MetadataUtil.likeTableCatalogError;
import static com.facebook.presto.metadata.MetadataUtil.toSchemaTableName;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static com.facebook.presto.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static com.facebook.presto.sql.NodeUtils.mapFromProperties;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_COLUMN_NAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_ALREADY_EXISTS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.sql.analyzer.utils.ParameterUtils.parameterExtractor;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.String.format;

public class CreateTableTask
        implements DDLDefinitionTask<CreateTable>
{
    @Override
    public String getName()
    {
        return "CREATE TABLE";
    }

    @Override
    public String explain(CreateTable statement, List<Expression> parameters)
    {
        return "CREATE TABLE " + statement.getName();
    }

    @Override
    public ListenableFuture<?> execute(CreateTable statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, Session session, List<Expression> parameters, WarningCollector warningCollector, String query)
    {
        return internalExecute(statement, metadata, accessControl, session, parameters, warningCollector, query);
    }

    @VisibleForTesting
    public ListenableFuture<?> internalExecute(CreateTable statement, Metadata metadata, AccessControl accessControl, Session session, List<Expression> parameters, WarningCollector warningCollector, String query)
    {
        checkArgument(!statement.getElements().isEmpty(), "no columns for table");

        Map<NodeRef<Parameter>, Expression> parameterLookup = parameterExtractor(statement, parameters);
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getName(), metadata);
        Optional<TableHandle> tableHandle = metadata.getMetadataResolver(session).getTableHandle(tableName);
        if (tableHandle.isPresent()) {
            if (!statement.isNotExists()) {
                throw new SemanticException(TABLE_ALREADY_EXISTS, statement, "Table '%s' already exists", tableName);
            }
            return immediateFuture(null);
        }

        ConnectorId connectorId = getConnectorIdOrThrow(session, metadata, tableName.getCatalogName());

        LinkedHashMap<String, ColumnMetadata> columns = new LinkedHashMap<>();
        Map<String, Object> inheritedProperties = ImmutableMap.of();
        boolean includingProperties = false;
        List<TableConstraint<String>> constraints = new ArrayList<>();
        for (TableElement element : statement.getElements()) {
            if (element instanceof ColumnDefinition) {
                ColumnDefinition column = (ColumnDefinition) element;
                String columnName = column.getName().getValue();
                String name = metadata.normalizeIdentifier(session, tableName.getCatalogName(), columnName);
                Type type;
                try {
                    type = metadata.getType(parseTypeSignature(column.getType()));
                }
                catch (IllegalArgumentException | UnknownTypeException e) {
                    throw new SemanticException(TYPE_MISMATCH, element, "Unknown type '%s' for column '%s'", column.getType(), column.getName());
                }
                if (type.equals(UNKNOWN)) {
                    throw new SemanticException(TYPE_MISMATCH, element, "Unknown type '%s' for column '%s'", column.getType(), column.getName());
                }
                if (columns.containsKey(name)) {
                    throw new SemanticException(DUPLICATE_COLUMN_NAME, column, "Column name '%s' specified more than once", column.getName());
                }
                if (!column.isNullable() && !metadata.getConnectorCapabilities(session, connectorId).contains(NOT_NULL_COLUMN_CONSTRAINT)) {
                    throw new SemanticException(NOT_SUPPORTED, column, "Catalog '%s' does not support non-null column for column name '%s'", connectorId.getCatalogName(), column.getName());
                }

                Map<String, Expression> sqlProperties = mapFromProperties(column.getProperties());
                Map<String, Object> columnProperties = metadata.getColumnPropertyManager().getProperties(
                        connectorId,
                        tableName.getCatalogName(),
                        sqlProperties,
                        session,
                        metadata,
                        parameterLookup);

                columns.put(name, ColumnMetadata.builder()
                        .setName(name)
                        .setType(type)
                        .setNullable(column.isNullable())
                        .setComment(column.getComment().orElse(null))
                        .setProperties(columnProperties)
                        .build());
            }
            else if (element instanceof LikeClause) {
                LikeClause likeClause = (LikeClause) element;
                QualifiedObjectName likeTableName = createQualifiedObjectName(session, statement, likeClause.getTableName(), metadata);
                getConnectorIdOrThrow(session, metadata, likeTableName.getCatalogName(), statement, likeTableCatalogError);
                if (!tableName.getCatalogName().equals(likeTableName.getCatalogName())) {
                    throw new SemanticException(NOT_SUPPORTED, statement, "LIKE table across catalogs is not supported");
                }
                TableHandle likeTable = metadata.getMetadataResolver(session).getTableHandle(likeTableName)
                        .orElseThrow(() -> new SemanticException(MISSING_TABLE, statement, "LIKE table '%s' does not exist", likeTableName));

                TableMetadata likeTableMetadata = metadata.getTableMetadata(session, likeTable);

                Optional<LikeClause.PropertiesOption> propertiesOption = likeClause.getPropertiesOption();
                if (propertiesOption.isPresent() && propertiesOption.get().equals(LikeClause.PropertiesOption.INCLUDING)) {
                    if (includingProperties) {
                        throw new SemanticException(NOT_SUPPORTED, statement, "Only one LIKE clause can specify INCLUDING PROPERTIES");
                    }
                    includingProperties = true;
                    inheritedProperties = likeTableMetadata.getMetadata().getProperties();
                }

                likeTableMetadata.getColumns().stream()
                        .filter(column -> !column.isHidden())
                        .forEach(column -> {
                            if (columns.containsKey(column.getName())) {
                                throw new SemanticException(DUPLICATE_COLUMN_NAME, element, "Column name '%s' specified more than once", column.getName());
                            }
                            columns.put(column.getName(), column);
                        });
            }
            else if (element instanceof ConstraintSpecification) {
                accessControl.checkCanAddConstraints(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), tableName);
                constraints.add(convertToTableConstraint(metadata, session, connectorId, (ConstraintSpecification) element, warningCollector, query));
            }
            else {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Invalid TableElement: " + element.getClass().getName());
            }
        }

        accessControl.checkCanCreateTable(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), tableName);

        constraints.stream()
                .filter(c -> c.getName().isPresent())
                .collect(Collectors.groupingBy(c -> c.getName().get(), Collectors.counting()))
                .forEach((constraintName, count) -> {
                    if (count > 1) {
                        throw new PrestoException(SYNTAX_ERROR, format("Constraint name '%s' specified more than once", constraintName));
                    }
                });

        if (constraints.stream()
                .filter(PrimaryKeyConstraint.class::isInstance)
                .collect(Collectors.groupingBy(c -> c.getName().orElse(""), Collectors.counting()))
                .size() > 1) {
            throw new PrestoException(SYNTAX_ERROR, "Multiple primary key constraints are not allowed");
        }

        Map<String, Expression> sqlProperties = mapFromProperties(statement.getProperties());
        Map<String, Object> properties = metadata.getTablePropertyManager().getProperties(
                connectorId,
                tableName.getCatalogName(),
                sqlProperties,
                session,
                metadata,
                parameterLookup);

        Map<String, Object> finalProperties = combineProperties(sqlProperties.keySet(), properties, inheritedProperties);

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(toSchemaTableName(tableName), ImmutableList.copyOf(columns.values()), finalProperties, statement.getComment(), constraints, Collections.emptyMap());
        try {
            metadata.createTable(session, tableName.getCatalogName(), tableMetadata, statement.isNotExists());
        }
        catch (PrestoException e) {
            // connectors are not required to handle the ignoreExisting flag
            if (!e.getErrorCode().equals(ALREADY_EXISTS.toErrorCode()) || !statement.isNotExists()) {
                throw e;
            }
        }
        return immediateFuture(null);
    }

    private static Map<String, Object> combineProperties(Set<String> specifiedPropertyKeys, Map<String, Object> defaultProperties, Map<String, Object> inheritedProperties)
    {
        Map<String, Object> finalProperties = new HashMap<>(inheritedProperties);
        for (Map.Entry<String, Object> entry : defaultProperties.entrySet()) {
            if (specifiedPropertyKeys.contains(entry.getKey()) || !finalProperties.containsKey(entry.getKey())) {
                finalProperties.put(entry.getKey(), entry.getValue());
            }
        }
        return finalProperties;
    }
}
