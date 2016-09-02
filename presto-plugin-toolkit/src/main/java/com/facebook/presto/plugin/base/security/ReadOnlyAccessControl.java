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

import static com.facebook.presto.spi.security.AccessDeniedException.denyAddColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDeleteTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static com.facebook.presto.spi.security.AccessDeniedException.denyInsertTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRevokeTablePrivilege;

public class ReadOnlyAccessControl
        implements ConnectorAccessControl
{
    @Override
    public void checkCanAddColumn(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        denyAddColumn(tableName.toString());
    }

    @Override
    public void checkCanCreateTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        denyCreateTable(tableName.toString());
    }

    @Override
    public void checkCanDropTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        denyDropTable(tableName.toString());
    }

    @Override
    public void checkCanRenameTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName, SchemaTableName newTableName)
    {
        denyRenameTable(tableName.toString(), newTableName.toString());
    }

    @Override
    public void checkCanRenameColumn(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        denyRenameColumn(tableName.toString());
    }

    @Override
    public void checkCanSelectFromTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        // allow
    }

    @Override
    public void checkCanInsertIntoTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        denyInsertTable(tableName.toString());
    }

    @Override
    public void checkCanDeleteFromTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        denyDeleteTable(tableName.toString());
    }

    @Override
    public void checkCanCreateView(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName viewName)
    {
        denyCreateView(viewName.toString());
    }

    @Override
    public void checkCanDropView(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName viewName)
    {
        denyDropView(viewName.toString());
    }

    @Override
    public void checkCanSelectFromView(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName viewName)
    {
        // allow
    }

    @Override
    public void checkCanCreateViewWithSelectFromTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        // allow
    }

    @Override
    public void checkCanCreateViewWithSelectFromView(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName viewName)
    {
        // allow
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String propertyName)
    {
        // allow
    }

    @Override
    public void checkCanGrantTablePrivilege(ConnectorTransactionHandle transaction, Identity identity, Privilege privilege, SchemaTableName tableName)
    {
        denyGrantTablePrivilege(privilege.name(), tableName.toString());
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorTransactionHandle transaction, Identity identity, Privilege privilege, SchemaTableName tableName)
    {
        denyRevokeTablePrivilege(privilege.name(), tableName.toString());
    }
}
