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

package com.facebook.presto.hive.security;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.Privilege;

import java.util.Set;

import static com.facebook.presto.hive.HiveMetadata.getSourceTableNameForPartitionsTable;
import static com.facebook.presto.hive.HiveMetadata.isPartitionsSystemTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectTable;
import static java.util.Objects.requireNonNull;

public class PartitionsAwareAccessControl
        implements ConnectorAccessControl
{
    private final ConnectorAccessControl delegate;

    public PartitionsAwareAccessControl(ConnectorAccessControl delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public void checkCanCreateSchema(ConnectorTransactionHandle transactionHandle, ConnectorSession session, String schemaName)
    {
        delegate.checkCanCreateSchema(transactionHandle, session, schemaName);
    }

    @Override
    public void checkCanDropSchema(ConnectorTransactionHandle transactionHandle, ConnectorSession session, String schemaName)
    {
        delegate.checkCanDropSchema(transactionHandle, session, schemaName);
    }

    @Override
    public void checkCanRenameSchema(ConnectorTransactionHandle transactionHandle, ConnectorSession session, String schemaName, String newSchemaName)
    {
        delegate.checkCanRenameSchema(transactionHandle, session, schemaName, newSchemaName);
    }

    @Override
    public void checkCanShowSchemas(ConnectorTransactionHandle transactionHandle, ConnectorSession session)
    {
        delegate.checkCanShowSchemas(transactionHandle, session);
    }

    @Override
    public Set<String> filterSchemas(ConnectorTransactionHandle transactionHandle, ConnectorSession session, Set<String> schemaNames)
    {
        return delegate.filterSchemas(transactionHandle, session, schemaNames);
    }

    @Override
    public void checkCanCreateTable(ConnectorTransactionHandle transactionHandle, ConnectorSession session, SchemaTableName tableName)
    {
        delegate.checkCanCreateTable(transactionHandle, session, tableName);
    }

    @Override
    public void checkCanDropTable(ConnectorTransactionHandle transactionHandle, ConnectorSession session, SchemaTableName tableName)
    {
        delegate.checkCanDropTable(transactionHandle, session, tableName);
    }

    @Override
    public void checkCanRenameTable(ConnectorTransactionHandle transactionHandle, ConnectorSession session, SchemaTableName tableName, SchemaTableName newTableName)
    {
        delegate.checkCanRenameTable(transactionHandle, session, tableName, newTableName);
    }

    @Override
    public void checkCanShowTablesMetadata(ConnectorTransactionHandle transactionHandle, ConnectorSession session, String schemaName)
    {
        delegate.checkCanShowTablesMetadata(transactionHandle, session, schemaName);
    }

    @Override
    public Set<SchemaTableName> filterTables(ConnectorTransactionHandle transactionHandle, ConnectorSession session, Set<SchemaTableName> tableNames)
    {
        return delegate.filterTables(transactionHandle, session, tableNames);
    }

    @Override
    public void checkCanAddColumn(ConnectorTransactionHandle transactionHandle, ConnectorSession session, SchemaTableName tableName)
    {
        delegate.checkCanAddColumn(transactionHandle, session, tableName);
    }

    @Override
    public void checkCanDropColumn(ConnectorTransactionHandle transactionHandle, ConnectorSession session, SchemaTableName tableName)
    {
        delegate.checkCanDropColumn(transactionHandle, session, tableName);
    }

    @Override
    public void checkCanRenameColumn(ConnectorTransactionHandle transactionHandle, ConnectorSession session, SchemaTableName tableName)
    {
        delegate.checkCanRenameColumn(transactionHandle, session, tableName);
    }

    @Override
    public void checkCanSelectFromTable(ConnectorTransactionHandle transactionHandle, ConnectorSession session, SchemaTableName tableName)
    {
        if (isPartitionsSystemTable(tableName)) {
            try {
                checkCanSelectFromTable(transactionHandle, session, getSourceTableNameForPartitionsTable(tableName));
                return;
            }
            catch (AccessDeniedException e) {
                denySelectTable(tableName.toString());
            }
        }
        delegate.checkCanSelectFromTable(transactionHandle, session, tableName);
    }

    @Override
    public void checkCanInsertIntoTable(ConnectorTransactionHandle transactionHandle, ConnectorSession session, SchemaTableName tableName)
    {
        delegate.checkCanInsertIntoTable(transactionHandle, session, tableName);
    }

    @Override
    public void checkCanDeleteFromTable(ConnectorTransactionHandle transactionHandle, ConnectorSession session, SchemaTableName tableName)
    {
        delegate.checkCanDeleteFromTable(transactionHandle, session, tableName);
    }

    @Override
    public void checkCanCreateView(ConnectorTransactionHandle transactionHandle, ConnectorSession session, SchemaTableName viewName)
    {
        delegate.checkCanCreateView(transactionHandle, session, viewName);
    }

    @Override
    public void checkCanDropView(ConnectorTransactionHandle transactionHandle, ConnectorSession session, SchemaTableName viewName)
    {
        delegate.checkCanDropView(transactionHandle, session, viewName);
    }

    @Override
    public void checkCanSelectFromView(ConnectorTransactionHandle transactionHandle, ConnectorSession session, SchemaTableName viewName)
    {
        delegate.checkCanSelectFromView(transactionHandle, session, viewName);
    }

    @Override
    public void checkCanCreateViewWithSelectFromTable(ConnectorTransactionHandle transactionHandle, ConnectorSession session, SchemaTableName tableName)
    {
        delegate.checkCanCreateViewWithSelectFromTable(transactionHandle, session, tableName);
    }

    @Override
    public void checkCanCreateViewWithSelectFromView(ConnectorTransactionHandle transactionHandle, ConnectorSession session, SchemaTableName viewName)
    {
        delegate.checkCanCreateViewWithSelectFromView(transactionHandle, session, viewName);
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String propertyName)
    {
        delegate.checkCanSetCatalogSessionProperty(identity, propertyName);
    }

    @Override
    public void checkCanGrantTablePrivilege(ConnectorTransactionHandle transactionHandle, ConnectorSession session, Privilege privilege, SchemaTableName tableName, String grantee, boolean withGrantOption)
    {
        delegate.checkCanGrantTablePrivilege(transactionHandle, session, privilege, tableName, grantee, withGrantOption);
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorTransactionHandle transactionHandle, ConnectorSession session, Privilege privilege, SchemaTableName tableName, String revokee, boolean grantOptionFor)
    {
        delegate.checkCanRevokeTablePrivilege(transactionHandle, session, privilege, tableName, revokee, grantOptionFor);
    }
}
