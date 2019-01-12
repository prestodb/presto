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
package io.prestosql.execution;

import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.ViewDefinition;
import io.prestosql.security.AccessControl;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.tree.DropView;
import io.prestosql.sql.tree.Expression;
import io.prestosql.transaction.TransactionManager;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.metadata.MetadataUtil.createQualifiedObjectName;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_TABLE;

public class DropViewTask
        implements DataDefinitionTask<DropView>
{
    @Override
    public String getName()
    {
        return "DROP VIEW";
    }

    @Override
    public ListenableFuture<?> execute(DropView statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName name = createQualifiedObjectName(session, statement, statement.getName());

        Optional<ViewDefinition> view = metadata.getView(session, name);
        if (!view.isPresent()) {
            if (!statement.isExists()) {
                throw new SemanticException(MISSING_TABLE, statement, "View '%s' does not exist", name);
            }
            return immediateFuture(null);
        }

        accessControl.checkCanDropView(session.getRequiredTransactionId(), session.getIdentity(), name);

        metadata.dropView(session, name);

        return immediateFuture(null);
    }
}
