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
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Grant;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PRIVILEGE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.immediateFuture;

public class GrantTask
        implements DataDefinitionTask<Grant>
{
    @Override
    public String getName()
    {
        return "GRANT";
    }

    @Override
    public ListenableFuture<?> execute(Grant statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getTableName());
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);
        if (!tableHandle.isPresent()) {
            throw new SemanticException(MISSING_TABLE, statement, "Table '%s' does not exist", tableName);
        }

        Set<Privilege> privileges;
        if (statement.getPrivileges().isPresent()) {
            privileges = statement.getPrivileges().get().stream()
                    .map(privilege -> parsePrivilege(statement, privilege))
                    .collect(toImmutableSet());
        }
        else {
            // All privileges
            privileges = EnumSet.allOf(Privilege.class);
        }

        // verify current identity has permissions to grant permissions
        for (Privilege privilege : privileges) {
            accessControl.checkCanGrantTablePrivilege(session.getRequiredTransactionId(), session.getIdentity(), privilege, tableName);
        }

        metadata.grantTablePrivileges(session, tableName, privileges, statement.getGrantee(), statement.isWithGrantOption());
        return immediateFuture(null);
    }

    private static Privilege parsePrivilege(Grant statement, String privilegeString)
    {
        for (Privilege privilege : Privilege.values()) {
            if (privilege.name().equalsIgnoreCase(privilegeString)) {
                return privilege;
            }
        }

        throw new SemanticException(INVALID_PRIVILEGE, statement, "Unknown privilege: '%s'", privilegeString);
    }
}
