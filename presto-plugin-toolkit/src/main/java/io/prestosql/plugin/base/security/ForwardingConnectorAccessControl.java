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
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.Privilege;

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
    public void checkCanCreateSchema(ConnectorTransactionHandle transactionHandle, Identity identity, String schemaName)
    {
        delegate().checkCanCreateSchema(transactionHandle, identity, schemaName);
    }

    @Override
    public void checkCanDropSchema(ConnectorTransactionHandle transactionHandle, Identity identity, String schemaName)
    {
        delegate().checkCanDropSchema(transactionHandle, identity, schemaName);
    }

    @Override
    public void checkCanRenameSchema(ConnectorTransactionHandle transactionHandle, Identity identity, String schemaName, String newSchemaName)
    {
        delegate().checkCanRenameSchema(transactionHandle, identity, schemaName, newSchemaName);
    }

    @Override
    public void checkCanShowSchemas(ConnectorTransactionHandle transactionHandle, Identity identity)
    {
        delegate().checkCanShowSchemas(transactionHandle, identity);
    }

    @Override
    public Set<String> filterSchemas(ConnectorTransactionHandle transactionHandle, Identity identity, Set<String> schemaNames)
    {
        return delegate().filterSchemas(transactionHandle, identity, schemaNames);
    }

    @Override
    public void checkCanCreateTable(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
    {
        delegate().checkCanCreateTable(transactionHandle, identity, tableName);
    }

    @Override
    public void checkCanDropTable(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
    {
        delegate().checkCanDropTable(transactionHandle, identity, tableName);
    }

    @Override
    public void checkCanRenameTable(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName, SchemaTableName newTableName)
    {
        delegate().checkCanRenameTable(transactionHandle, identity, tableName, newTableName);
    }

    @Override
    public void checkCanShowTablesMetadata(ConnectorTransactionHandle transactionHandle, Identity identity, String schemaName)
    {
        delegate().checkCanShowTablesMetadata(transactionHandle, identity, schemaName);
    }

    @Override
    public Set<SchemaTableName> filterTables(ConnectorTransactionHandle transactionHandle, Identity identity, Set<SchemaTableName> tableNames)
    {
        return delegate().filterTables(transactionHandle, identity, tableNames);
    }

    @Override
    public void checkCanAddColumn(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
    {
        delegate().checkCanAddColumn(transactionHandle, identity, tableName);
    }

    @Override
    public void checkCanDropColumn(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
    {
        delegate().checkCanDropColumn(transactionHandle, identity, tableName);
    }

    @Override
    public void checkCanRenameColumn(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
    {
        delegate().checkCanRenameColumn(transactionHandle, identity, tableName);
    }

    @Override
    public void checkCanSelectFromColumns(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName, Set<String> columnNames)
    {
        delegate().checkCanSelectFromColumns(transactionHandle, identity, tableName, columnNames);
    }

    @Override
    public void checkCanInsertIntoTable(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
    {
        delegate().checkCanInsertIntoTable(transactionHandle, identity, tableName);
    }

    @Override
    public void checkCanDeleteFromTable(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
    {
        delegate().checkCanDeleteFromTable(transactionHandle, identity, tableName);
    }

    @Override
    public void checkCanCreateView(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName viewName)
    {
        delegate().checkCanCreateView(transactionHandle, identity, viewName);
    }

    @Override
    public void checkCanDropView(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName viewName)
    {
        delegate().checkCanDropView(transactionHandle, identity, viewName);
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName, Set<String> columnNames)
    {
        delegate().checkCanCreateViewWithSelectFromColumns(transactionHandle, identity, tableName, columnNames);
    }

    @Override
    public void checkCanSetCatalogSessionProperty(ConnectorTransactionHandle transactionHandle, Identity identity, String propertyName)
    {
        delegate().checkCanSetCatalogSessionProperty(transactionHandle, identity, propertyName);
    }

    @Override
    public void checkCanGrantTablePrivilege(ConnectorTransactionHandle transactionHandle, Identity identity, Privilege privilege, SchemaTableName tableName, String grantee, boolean withGrantOption)
    {
        delegate().checkCanGrantTablePrivilege(transactionHandle, identity, privilege, tableName, grantee, withGrantOption);
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorTransactionHandle transactionHandle, Identity identity, Privilege privilege, SchemaTableName tableName, String revokee, boolean grantOptionFor)
    {
        delegate().checkCanGrantTablePrivilege(transactionHandle, identity, privilege, tableName, revokee, grantOptionFor);
    }
}
