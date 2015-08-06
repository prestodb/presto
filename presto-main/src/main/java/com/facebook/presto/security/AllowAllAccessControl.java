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
import com.facebook.presto.spi.security.Identity;

import java.security.Principal;

public class AllowAllAccessControl
        implements AccessControl
{
    @Override
    public void checkCanSetUser(Principal principal, String userName)
    {
    }

    @Override
    public void checkCanCreateTable(Identity identity, QualifiedTableName tableName)
    {
    }

    @Override
    public void checkCanDropTable(Identity identity, QualifiedTableName tableName)
    {
    }

    @Override
    public void checkCanRenameTable(Identity identity, QualifiedTableName tableName, QualifiedTableName newTableName)
    {
    }

    @Override
    public void checkCanAddColumns(Identity identity, QualifiedTableName tableName)
    {
    }

    @Override
    public void checkCanRenameColumn(Identity identity, QualifiedTableName tableName)
    {
    }

    @Override
    public void checkCanSelectFromTable(Identity identity, QualifiedTableName tableName)
    {
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, QualifiedTableName tableName)
    {
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, QualifiedTableName tableName)
    {
    }

    @Override
    public void checkCanCreateView(Identity identity, QualifiedTableName viewName)
    {
    }

    @Override
    public void checkCanDropView(Identity identity, QualifiedTableName viewName)
    {
    }

    @Override
    public void checkCanSelectFromView(Identity identity, QualifiedTableName viewName)
    {
    }

    @Override
    public void checkCanCreateViewWithSelectFromTable(Identity identity, QualifiedTableName tableName)
    {
    }

    @Override
    public void checkCanCreateViewWithSelectFromView(Identity identity, QualifiedTableName viewName)
    {
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String catalogName, String propertyName)
    {
    }
}
