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
package com.facebook.presto.security;

import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.spi.security.ConnectorAccessControl;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.SystemAccessControl;
import com.google.common.collect.Sets;

import java.security.Principal;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AccessControlManager
        implements AccessControl
{
    private final Set<SystemAccessControl> systemAccessControllers = Sets.newConcurrentHashSet();
    private final Map<String, ConnectorAccessControl> catalogAccessControl = new ConcurrentHashMap<>();

    public void addSystemAccessControl(SystemAccessControl accessControl)
    {
        requireNonNull(accessControl, "accessControl is null");

        systemAccessControllers.add(accessControl);
    }

    public void addCatalogAccessControl(String catalogName, ConnectorAccessControl accessControl)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(accessControl, "accessControl is null");

        if (catalogAccessControl.putIfAbsent(catalogName, accessControl) != null) {
            throw new IllegalArgumentException(format("Access control for catalog '%s' is already registered", catalogName));
        }
    }

    @Override
    public void checkCanSetUser(Principal principal, String userName)
    {
        requireNonNull(userName, "userName is null");

        for (SystemAccessControl accessControl : systemAccessControllers) {
            accessControl.checkCanSetUser(principal, userName);
        }
    }

    @Override
    public void checkCanCreateTable(Identity identity, QualifiedTableName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(tableName.getCatalogName());
        if (accessControl != null) {
            accessControl.checkCanCreateTable(identity, tableName.asSchemaTableName());
        }
    }

    @Override
    public void checkCanDropTable(Identity identity, QualifiedTableName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(tableName.getCatalogName());
        if (accessControl != null) {
            accessControl.checkCanDropTable(identity, tableName.asSchemaTableName());
        }
    }

    @Override
    public void checkCanRenameTable(Identity identity, QualifiedTableName tableName, QualifiedTableName newTableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(newTableName, "newTableName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(tableName.getCatalogName());
        if (accessControl != null) {
            accessControl.checkCanRenameTable(identity, tableName.asSchemaTableName(), newTableName.asSchemaTableName());
        }
    }

    @Override
    public void checkCanAddColumns(Identity identity, QualifiedTableName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(tableName.getCatalogName());
        if (accessControl != null) {
            accessControl.checkCanAddColumn(identity, tableName.asSchemaTableName());
        }
    }

    @Override
    public void checkCanRenameColumn(Identity identity, QualifiedTableName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(tableName.getCatalogName());
        if (accessControl != null) {
            accessControl.checkCanRenameColumn(identity, tableName.asSchemaTableName());
        }
    }

    @Override
    public void checkCanSelectFromTable(Identity identity, QualifiedTableName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(tableName.getCatalogName());
        if (accessControl != null) {
            accessControl.checkCanSelectFromTable(identity, tableName.asSchemaTableName());
        }
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, QualifiedTableName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(tableName.getCatalogName());
        if (accessControl != null) {
            accessControl.checkCanInsertIntoTable(identity, tableName.asSchemaTableName());
        }
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, QualifiedTableName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(tableName.getCatalogName());
        if (accessControl != null) {
            accessControl.checkCanDeleteFromTable(identity, tableName.asSchemaTableName());
        }
    }

    @Override
    public void checkCanCreateView(Identity identity, QualifiedTableName viewName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(viewName, "viewName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(viewName.getCatalogName());
        if (accessControl != null) {
            accessControl.checkCanCreateView(identity, viewName.asSchemaTableName());
        }
    }

    @Override
    public void checkCanDropView(Identity identity, QualifiedTableName viewName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(viewName, "viewName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(viewName.getCatalogName());
        if (accessControl != null) {
            accessControl.checkCanDropView(identity, viewName.asSchemaTableName());
        }
    }

    @Override
    public void checkCanSelectFromView(Identity identity, QualifiedTableName viewName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(viewName, "viewName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(viewName.getCatalogName());
        if (accessControl != null) {
            accessControl.checkCanSelectFromView(identity, viewName.asSchemaTableName());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromTable(Identity identity, QualifiedTableName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(tableName.getCatalogName());
        if (accessControl != null) {
            accessControl.checkCanCreateViewWithSelectFromTable(identity, tableName.asSchemaTableName());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromView(Identity identity, QualifiedTableName viewName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(viewName, "viewName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(viewName.getCatalogName());
        if (accessControl != null) {
            accessControl.checkCanCreateViewWithSelectFromView(identity, viewName.asSchemaTableName());
        }
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(propertyName, "propertyName is null");

        for (SystemAccessControl accessControl : systemAccessControllers) {
            accessControl.checkCanSetSystemSessionProperty(identity, propertyName);
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String catalogName, String propertyName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(propertyName, "propertyName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(catalogName);
        if (accessControl != null) {
            accessControl.checkCanSetCatalogSessionProperty(identity, propertyName);
        }
    }
}
