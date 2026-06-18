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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.constraints.PrimaryKeyConstraint;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.constraints.UniqueConstraint;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.AddConstraint;
import com.facebook.presto.sql.tree.ConstraintSpecification;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardWarningCode.SEMANTIC_WARNING;
import static com.facebook.presto.spi.connector.ConnectorCapabilities.ENFORCE_CONSTRAINTS;
import static com.facebook.presto.spi.connector.ConnectorCapabilities.PRIMARY_KEY_CONSTRAINT;
import static com.facebook.presto.spi.connector.ConnectorCapabilities.UNIQUE_CONSTRAINT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.String.format;
import static java.util.stream.Collectors.toCollection;

public class AddConstraintTask
        implements DDLDefinitionTask<AddConstraint>
{
    public static TableConstraint<String> convertToTableConstraint(Metadata metadata, Session session, ConnectorId connectorId, ConstraintSpecification node, WarningCollector warningCollector, String query)
    {
        TableConstraint<String> tableConstraint;
        LinkedHashSet<String> constraintColumns = node.getColumns().stream().collect(toCollection(LinkedHashSet::new));
        switch (node.getConstraintType()) {
            case UNIQUE:
                if (!metadata.getConnectorCapabilities(session, connectorId).contains(UNIQUE_CONSTRAINT)) {
                    throw new SemanticException(NOT_SUPPORTED, node, "Catalog %s does not support Unique constraints", connectorId.getCatalogName());
                }
                tableConstraint = new UniqueConstraint<>(node.getConstraintName(), constraintColumns, node.isEnabled(), node.isRely(), node.isEnforced());
                break;
            case PRIMARY_KEY:
                if (!metadata.getConnectorCapabilities(session, connectorId).contains(PRIMARY_KEY_CONSTRAINT)) {
                    throw new SemanticException(NOT_SUPPORTED, node, "Catalog %s does not support Primary Key constraints", connectorId.getCatalogName());
                }
                tableConstraint = new PrimaryKeyConstraint<>(node.getConstraintName(), constraintColumns, node.isEnabled(), node.isRely(), node.isEnforced());
                break;
            default:
                throw new SemanticException(NOT_SUPPORTED, node, "Given constraint type %s is not supported", node.getConstraintType().toString());
        }

        if (!metadata.getConnectorCapabilities(session, connectorId).contains(ENFORCE_CONSTRAINTS) && node.isEnforced()) {
            warningCollector.add(new PrestoWarning(SEMANTIC_WARNING, format("Constraint %s is set to ENFORCED. This connector does not support enforcement of table constraints", node.getConstraintName().orElse(""))));
        }

        return tableConstraint;
    }

    @Override
    public String getName()
    {
        return "ADD CONSTRAINT";
    }

    @Override
    public ListenableFuture<?> execute(AddConstraint statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, Session session, List<Expression> parameters, WarningCollector warningCollector, String query)
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
                throw new SemanticException(NOT_SUPPORTED, statement, "'%s' is a materialized view, and add constraint is not supported", tableName);
            }
            return immediateFuture(null);
        }

        ConnectorId connectorId = metadata.getCatalogHandle(session, tableName.getCatalogName())
                .orElseThrow(() -> new PrestoException(NOT_FOUND, "Catalog does not exist: " + tableName.getCatalogName()));

        accessControl.checkCanAddConstraints(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), tableName);

        metadata.addConstraint(session, tableHandle.get(), convertToTableConstraint(metadata, session, connectorId, statement.getConstraintSpecification(), warningCollector, query));
        return immediateFuture(null);
    }
}
