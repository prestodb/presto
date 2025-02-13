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
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.RenameView;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_CATALOG;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_VIEW;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.VIEW_ALREADY_EXISTS;
import static com.google.common.util.concurrent.Futures.immediateFuture;

public class RenameViewTask
        implements DDLDefinitionTask<RenameView>
{
    @Override
    public String getName()
    {
        return "RENAME VIEW";
    }

    public ListenableFuture<?> execute(RenameView statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, Session session, List<Expression> parameters, WarningCollector warningCollector, String query)
    {
        QualifiedObjectName viewName = createQualifiedObjectName(session, statement, statement.getSource(), metadata);

        Optional<ViewDefinition> view = metadata.getMetadataResolver(session).getView(viewName);
        if (!view.isPresent()) {
            if (!statement.isExists()) {
                throw new SemanticException(MISSING_VIEW, statement, "View '%s' does not exist", viewName);
            }
            return immediateFuture(null);
        }

        QualifiedObjectName target = createQualifiedObjectName(session, statement, statement.getTarget(), metadata);
        if (!metadata.getCatalogHandle(session, target.getCatalogName()).isPresent()) {
            throw new SemanticException(MISSING_CATALOG, statement, "Target catalog '%s' does not exist", target.getCatalogName());
        }
        if (metadata.getMetadataResolver(session).getView(target).isPresent()) {
            throw new SemanticException(VIEW_ALREADY_EXISTS, statement, "Target view '%s' already exists", target);
        }
        if (!viewName.getSchemaName().equals(target.getSchemaName())) {
            throw new SemanticException(NOT_SUPPORTED, statement, "View rename across schemas is not supported");
        }
        if (!viewName.getCatalogName().equals(target.getCatalogName())) {
            throw new SemanticException(NOT_SUPPORTED, statement, "View rename across catalogs is not supported");
        }

        accessControl.checkCanRenameView(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), viewName, target);

        metadata.renameView(session, viewName, target);

        return immediateFuture(null);
    }
}
