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
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.spi.type.UnknownTypeException;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.AddField;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.google.common.util.concurrent.Futures.immediateFuture;

public class AddFieldTask
        implements DDLDefinitionTask<AddField>
{
    @Override
    public String getName()
    {
        return "ADD COLUMN";
    }

    @Override
    public ListenableFuture<?> execute(
            AddField statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            Session session,
            List<Expression> parameters,
            WarningCollector warningCollector,
            String query)
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
                throw new SemanticException(NOT_SUPPORTED, statement, "'%s' is a materialized view, and add column is not supported", tableName);
            }
            return immediateFuture(null);
        }

        accessControl.checkCanAddColumns(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), tableName);

        // Parse the field type
        Type type;
        try {
            type = metadata.getType(parseTypeSignature(statement.getType()));
        }
        catch (IllegalArgumentException | UnknownTypeException e) {
            throw new SemanticException(TYPE_MISMATCH, statement, "Unknown type '%s' for column '%s'", statement.getType(), statement.getFieldName());
        }
        if (type.equals(UNKNOWN)) {
            throw new SemanticException(TYPE_MISMATCH, statement, "Unknown type '%s' for column '%s'", statement.getType(), statement.getFieldName());
        }

        // columnPath is the parent struct path (e.g. ["col"] or ["col", "nested"])
        // fieldName is the new field being added inside that struct.
        // Normalize identifiers through the catalog's case rules, matching what AddColumnTask does.
        List<String> parentPath = statement.getColumnPath().getParts().stream()
                .map(part -> metadata.normalizeIdentifier(session, tableName.getCatalogName(), part))
                .collect(Collectors.toList());
        String fieldName = metadata.normalizeIdentifier(session, tableName.getCatalogName(), statement.getFieldName().getValue());

        metadata.addField(session, tableHandle.get(), parentPath, fieldName, type, statement.isFieldNotExists());

        return immediateFuture(null);
    }
}
