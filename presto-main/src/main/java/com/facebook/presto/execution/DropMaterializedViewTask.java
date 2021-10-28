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
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.ConnectorMaterializedViewDefinition;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.DropMaterializedView;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_MATERIALIZED_VIEW;
import static com.google.common.util.concurrent.Futures.immediateFuture;

public class DropMaterializedViewTask
        implements DataDefinitionTask<DropMaterializedView>
{
    @Override
    public String getName()
    {
        return "DROP MATERIALIZED VIEW";
    }

    @Override
    public ListenableFuture<?> execute(DropMaterializedView statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName name = createQualifiedObjectName(session, statement, statement.getName());

        Optional<ConnectorMaterializedViewDefinition> view = metadata.getMaterializedView(session, name);
        if (!view.isPresent()) {
            if (!statement.isExists()) {
                throw new SemanticException(MISSING_MATERIALIZED_VIEW, statement, "Materialized view '%s' does not exist", name);
            }
            return immediateFuture(null);
        }

        accessControl.checkCanDropTable(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), name);
        accessControl.checkCanDropView(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), name);

        metadata.dropMaterializedView(session, name);

        return immediateFuture(null);
    }
}
