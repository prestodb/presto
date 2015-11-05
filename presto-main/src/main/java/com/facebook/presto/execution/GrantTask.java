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
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.Grant;

import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedTableName;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;

public class GrantTask
    implements DataDefinitionTask<Grant>
{
    @Override
    public String getName()
    {
        return "GRANT";
    }

    @Override
    public void execute(Grant statement, Session session, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine)
    {
        QualifiedTableName tableName = createQualifiedTableName(session, statement, statement.getTableName());
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);
        if (!tableHandle.isPresent()) {
            throw new SemanticException(MISSING_TABLE, statement, "Table '%s' does not exist", tableName);
        }

        // TODO: check that the user/role exists -- I think that this check can't be performed at this level. Each connector will have
        // to perform this check.

        // check that the current user can grant the requested privilege on the table
        // NOTE: You want to pass the identity from the session and NOT the identity from Grant node. Because the
        // session carries "the current user" (or grantor's) information, where as the Grant node carries the grantee's information.
        accessControl.checkCanGrantTablePrivilege(session.getIdentity(), statement.getPrestoPrivilege(), tableName);

        // TODO: if [WITH GRANT OPTION] is present, check that the current user has the ("GRANT") privilege to grant the privilege

        Privilege privilege = new Privilege(statement.getPrestoPrivilege().getTypeString());

        // I don't fully understand the Identity object; what is user and what is principal? For now, I am passing grantee
        // as the user, but not sure if this is right.
        Identity identity = new Identity(statement.getPrestoIdentity().getName().toString(), Optional.empty());

        metadata.grantTablePrivilege(session, tableName, privilege, identity, statement.isOption());
    }
}
