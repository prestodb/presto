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

import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.spi.security.Identity;

import java.security.Principal;

import static com.facebook.presto.spi.security.AccessDeniedException.denyAddColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDeleteTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyInsertTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectView;
import static com.facebook.presto.spi.security.AccessDeniedException.denySetCatalogSessionProperty;
import static com.facebook.presto.spi.security.AccessDeniedException.denySetSystemSessionProperty;
import static com.facebook.presto.spi.security.AccessDeniedException.denySetUser;

public class DenyAllAccessControl
        implements AccessControl
{
    @Override
    public void checkCanSetUser(Principal principal, String userName)
    {
        denySetUser(principal, userName);
    }

    @Override
    public void checkCanCreateTable(Identity identity, QualifiedObjectName tableName)
    {
        denyCreateTable(tableName.toString());
    }

    @Override
    public void checkCanDropTable(Identity identity, QualifiedObjectName tableName)
    {
        denyDropTable(tableName.toString());
    }

    @Override
    public void checkCanRenameTable(Identity identity, QualifiedObjectName tableName, QualifiedObjectName newTableName)
    {
        denyRenameTable(tableName.toString(), newTableName.toString());
    }

    @Override
    public void checkCanAddColumns(Identity identity, QualifiedObjectName tableName)
    {
        denyAddColumn(tableName.toString());
    }

    @Override
    public void checkCanRenameColumn(Identity identity, QualifiedObjectName tableName)
    {
        denyRenameColumn(tableName.toString());
    }

    @Override
    public void checkCanSelectFromTable(Identity identity, QualifiedObjectName tableName)
    {
        denySelectTable(tableName.toString());
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, QualifiedObjectName tableName)
    {
        denyInsertTable(tableName.toString());
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, QualifiedObjectName tableName)
    {
        denyDeleteTable(tableName.toString());
    }

    @Override
    public void checkCanCreateView(Identity identity, QualifiedObjectName viewName)
    {
        denyCreateView(viewName.toString());
    }

    @Override
    public void checkCanDropView(Identity identity, QualifiedObjectName viewName)
    {
        denyDropView(viewName.toString());
    }

    @Override
    public void checkCanSelectFromView(Identity identity, QualifiedObjectName viewName)
    {
        denySelectView(viewName.toString());
    }

    @Override
    public void checkCanCreateViewWithSelectFromTable(Identity identity, QualifiedObjectName tableName)
    {
        denySelectTable(tableName.toString());
    }

    @Override
    public void checkCanCreateViewWithSelectFromView(Identity identity, QualifiedObjectName viewName)
    {
        denySelectView(viewName.toString());
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        denySetSystemSessionProperty(propertyName);
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String catalogName, String propertyName)
    {
        denySetCatalogSessionProperty(catalogName, propertyName);
    }
}
