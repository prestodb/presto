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
package com.facebook.presto.plugin.base.security;

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.Privilege;

import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public abstract class ForwardingConnectorAccessControl
        implements ConnectorAccessControl
{
    public static ConnectorAccessControl of(Supplier<ConnectorAccessControl> connectorAccessControlSupplier)
    {
        requireNonNull(connectorAccessControlSupplier, "connectorAccessControlSupplier is null");
        return new ForwardingConnectorAccessControl()
        {
            @Override
            protected ConnectorAccessControl delegate()
            {
                return connectorAccessControlSupplier.get();
            }
        };
    }

    protected abstract ConnectorAccessControl delegate();

    @Override
    public void checkCanCreateSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String schemaName)
    {
        delegate().checkCanCreateSchema(transactionHandle, identity, context, schemaName);
    }

    @Override
    public void checkCanDropSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String schemaName)
    {
        delegate().checkCanDropSchema(transactionHandle, identity, context, schemaName);
    }

    @Override
    public void checkCanRenameSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String schemaName, String newSchemaName)
    {
        delegate().checkCanRenameSchema(transactionHandle, identity, context, schemaName, newSchemaName);
    }

    @Override
    public void checkCanShowSchemas(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context)
    {
        delegate().checkCanShowSchemas(transactionHandle, identity, context);
    }

    @Override
    public Set<String> filterSchemas(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, Set<String> schemaNames)
    {
        return delegate().filterSchemas(transactionHandle, identity, context, schemaNames);
    }

    @Override
    public void checkCanCreateTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        delegate().checkCanCreateTable(transactionHandle, identity, context, tableName);
    }

    @Override
    public void checkCanDropTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        delegate().checkCanDropTable(transactionHandle, identity, context, tableName);
    }

    @Override
    public void checkCanRenameTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName, SchemaTableName newTableName)
    {
        delegate().checkCanRenameTable(transactionHandle, identity, context, tableName, newTableName);
    }

    @Override
    public void checkCanShowTablesMetadata(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String schemaName)
    {
        delegate().checkCanShowTablesMetadata(transactionHandle, identity, context, schemaName);
    }

    @Override
    public Set<SchemaTableName> filterTables(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, Set<SchemaTableName> tableNames)
    {
        return delegate().filterTables(transactionHandle, identity, context, tableNames);
    }

    @Override
    public void checkCanAddColumn(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        delegate().checkCanAddColumn(transactionHandle, identity, context, tableName);
    }

    @Override
    public void checkCanDropColumn(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        delegate().checkCanDropColumn(transactionHandle, identity, context, tableName);
    }

    @Override
    public void checkCanRenameColumn(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        delegate().checkCanRenameColumn(transactionHandle, identity, context, tableName);
    }

    @Override
    public void checkCanSelectFromColumns(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName, Set<String> columnNames)
    {
        delegate().checkCanSelectFromColumns(transactionHandle, identity, context, tableName, columnNames);
    }

    @Override
    public void checkCanInsertIntoTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        delegate().checkCanInsertIntoTable(transactionHandle, identity, context, tableName);
    }

    @Override
    public void checkCanDeleteFromTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        delegate().checkCanDeleteFromTable(transactionHandle, identity, context, tableName);
    }

    @Override
    public void checkCanCreateView(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName viewName)
    {
        delegate().checkCanCreateView(transactionHandle, identity, context, viewName);
    }

    @Override
    public void checkCanDropView(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName viewName)
    {
        delegate().checkCanDropView(transactionHandle, identity, context, viewName);
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName, Set<String> columnNames)
    {
        delegate().checkCanCreateViewWithSelectFromColumns(transactionHandle, identity, context, tableName, columnNames);
    }

    @Override
    public void checkCanSetCatalogSessionProperty(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String propertyName)
    {
        delegate().checkCanSetCatalogSessionProperty(transactionHandle, identity, context, propertyName);
    }

    @Override
    public void checkCanGrantTablePrivilege(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, Privilege privilege, SchemaTableName tableName, PrestoPrincipal grantee, boolean withGrantOption)
    {
        delegate().checkCanGrantTablePrivilege(transactionHandle, identity, context, privilege, tableName, grantee, withGrantOption);
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, Privilege privilege, SchemaTableName tableName, PrestoPrincipal revokee, boolean grantOptionFor)
    {
        delegate().checkCanGrantTablePrivilege(transactionHandle, identity, context, privilege, tableName, revokee, grantOptionFor);
    }

    @Override
    public void checkCanCreateRole(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String role, Optional<PrestoPrincipal> grantor)
    {
        delegate().checkCanCreateRole(transactionHandle, identity, context, role, grantor);
    }

    @Override
    public void checkCanDropRole(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String role)
    {
        delegate().checkCanDropRole(transactionHandle, identity, context, role);
    }

    @Override
    public void checkCanGrantRoles(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor, String catalogName)
    {
        delegate().checkCanGrantRoles(transactionHandle, identity, context, roles, grantees, withAdminOption, grantor, catalogName);
    }

    @Override
    public void checkCanRevokeRoles(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor, String catalogName)
    {
        delegate().checkCanRevokeRoles(transactionHandle, identity, context, roles, grantees, adminOptionFor, grantor, catalogName);
    }

    @Override
    public void checkCanSetRole(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String role, String catalogName)
    {
        delegate().checkCanSetRole(transactionHandle, identity, context, role, catalogName);
    }

    @Override
    public void checkCanShowRoles(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String catalogName)
    {
        delegate().checkCanShowRoles(transactionHandle, identity, context, catalogName);
    }

    @Override
    public void checkCanShowCurrentRoles(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String catalogName)
    {
        delegate().checkCanShowCurrentRoles(transactionHandle, identity, context, catalogName);
    }

    @Override
    public void checkCanShowRoleGrants(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String catalogName)
    {
        delegate().checkCanShowRoleGrants(transactionHandle, identity, context, catalogName);
    }
}
