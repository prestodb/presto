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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.ColumnDefinition;
import com.facebook.presto.sql.tree.CreateTableWithFiber;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.TableElement;
import com.facebook.presto.transaction.TransactionManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * presto-root
 *
 * @author guodong
 */
public class CreateTableWithFiberTask
    implements DataDefinitionTask<CreateTableWithFiber>
{
    @Override
    public String getName()
    {
        return "CREATE TABLE WITH FIBER";
    }

    @Override
    public String explain(CreateTableWithFiber statement, List<Expression> parameters)
    {
        return "CREATE TABLE WITH FIBER " + statement.getName();
    }

    @Override
    public CompletableFuture<?> execute(CreateTableWithFiber statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        checkArgument(!statement.getElements().isEmpty(), "no columns for table");
        checkArgument(!statement.getFibK().isEmpty(), "no fiber key for table");
        checkArgument(!statement.getFunction().toString().isEmpty(), "no fiber partition function for table");
        checkArgument(!statement.getTimeK().isEmpty(), "no timestamp key for table");

        Session session = stateMachine.getSession();
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getName());
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);
        if (tableHandle.isPresent()) {
            return completedFuture(null);
        }

        List<ColumnMetadata> columns = new ArrayList<>();
        for (TableElement element : statement.getElements()) {
            if (element instanceof ColumnDefinition) {
                ColumnDefinition column = (ColumnDefinition) element;
                Type type = metadata.getType(TypeSignature.parseTypeSignature(column.getType()));
                if (type == null || type.equals(UNKNOWN)) {
                    throw new SemanticException(TYPE_MISMATCH, column, "Unknown type for column %s' ", column.getName());
                }
                columns.add(new ColumnMetadata(column.getName(), type));
            }
            // TODO if element instanceof LikeClause
            else {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Invalid TableElement: " + element.getClass().getName());
            }
        }

        accessControl.checkCanCreateTable(session.getRequiredTransactionId(), session.getIdentity(), tableName);

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName.asSchemaTableName(), columns);

        metadata.createTableWithFiber(session, tableName.getCatalogName(), tableMetadata, statement.getFibK(), statement.getFunction().toString(), statement.getTimeK());

        return completedFuture(null);
    }
}
