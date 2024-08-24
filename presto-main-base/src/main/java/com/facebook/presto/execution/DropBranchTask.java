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
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.DropBranch;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.metadata.MetadataUtil.getConnectorIdOrThrow;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.google.common.util.concurrent.Futures.immediateFuture;

public class DropBranchTask
        implements DDLDefinitionTask<DropBranch>
{
    @Override
    public String getName()
    {
        return "DROP BRANCH";
    }

    @Override
    public ListenableFuture<?> execute(DropBranch statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, Session session, List<Expression> parameters, WarningCollector warningCollector, String query)
    {
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getTableName(), metadata);
        Optional<TableHandle> tableHandleOptional = metadata.getMetadataResolver(session).getTableHandle(tableName);

        if (!tableHandleOptional.isPresent()) {
            if (!statement.isTableExists()) {
                throw new SemanticException(MISSING_TABLE, statement, "Table '%s' does not exist", tableName);
            }
            return immediateFuture(null);
        }

        Optional<MaterializedViewDefinition> optionalMaterializedView = metadata.getMetadataResolver(session).getMaterializedView(tableName);
        if (optionalMaterializedView.isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, statement, "'%s' is a materialized view, and drop branch is not supported", tableName);
        }

        getConnectorIdOrThrow(session, metadata, tableName.getCatalogName());
        accessControl.checkCanDropBranch(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), tableName);

        metadata.dropBranch(session, tableHandleOptional.get(), statement.getBranchName(), statement.isBranchExists());
        return immediateFuture(null);
    }
}
