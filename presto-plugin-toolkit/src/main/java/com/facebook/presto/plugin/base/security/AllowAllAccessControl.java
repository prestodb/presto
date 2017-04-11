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

public class AllowAllAccessControl
        implements ConnectorAccessControl
{
    @Override
    public void checkCanShowSchemas(ConnectorTransactionHandle transactionHandle, Identity identity)
    {
    }

    @Override
    public Set<String> filterSchemas(ConnectorTransactionHandle transactionHandle, Identity identity, Set<String> schemaNames)
    {
        return schemaNames;
    }

    @Override
    public void checkCanCreateTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanDropTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanRenameTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName, SchemaTableName newTableName)
    {
    }

    @Override
    public void checkCanShowTables(ConnectorTransactionHandle transactionHandle, Identity identity, String schemaName)
    {
    }

    @Override
    public Set<SchemaTableName> filterTables(ConnectorTransactionHandle transactionHandle, Identity identity, Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    @Override
    public void checkCanAddColumn(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanRenameColumn(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanSelectFromTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanInsertIntoTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanDeleteFromTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanCreateView(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName viewName)
    {
    }

    @Override
    public void checkCanDropView(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName viewName)
    {
    }

    @Override
    public void checkCanSelectFromView(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName viewName)
    {
    }

    @Override
    public void checkCanCreateViewWithSelectFromTable(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanCreateViewWithSelectFromView(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName viewName)
    {
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String propertyName)
    {
    }

    @Override
    public void checkCanGrantTablePrivilege(ConnectorTransactionHandle transaction, Identity identity, Privilege privilege, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorTransactionHandle transaction, Identity identity, Privilege privilege, SchemaTableName tableName)
    {
    }
}
