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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.spi.CatalogSchemaName;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.transaction.TransactionId;

import java.security.Principal;
import java.util.Set;

public class AllowAllAccessControl
        implements AccessControl
{
    @Override
    public void checkCanSetUser(Principal principal, String userName)
    {
    }

    @Override
    public Set<String> filterCatalogs(Session session, Set<String> catalogs)
    {
        return catalogs;
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, String catalogName)
    {
    }

    @Override
    public void checkCanCreateSchema(Session session, CatalogSchemaName schemaName)
    {
    }

    @Override
    public void checkCanDropSchema(Session session, CatalogSchemaName schemaName)
    {
    }

    @Override
    public void checkCanRenameSchema(Session session, CatalogSchemaName schemaName, String newSchemaName)
    {
    }

    @Override
    public void checkCanShowSchemas(Session session, String catalogName)
    {
    }

    @Override
    public Set<String> filterSchemas(Session session, String catalogName, Set<String> schemaNames)
    {
        return schemaNames;
    }

    @Override
    public void checkCanCreateTable(Session session, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanDropTable(Session session, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanRenameTable(Session session, QualifiedObjectName tableName, QualifiedObjectName newTableName)
    {
    }

    @Override
    public void checkCanShowTablesMetadata(Session session, CatalogSchemaName schema)
    {
    }

    @Override
    public Set<SchemaTableName> filterTables(Session session, String catalogName, Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    @Override
    public void checkCanAddColumns(Session session, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanDropColumn(Session session, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanRenameColumn(Session session, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanSelectFromTable(Session session, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanInsertIntoTable(Session session, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanDeleteFromTable(Session session, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanCreateView(Session session, QualifiedObjectName viewName)
    {
    }

    @Override
    public void checkCanDropView(Session session, QualifiedObjectName viewName)
    {
    }

    @Override
    public void checkCanSelectFromView(Session session, QualifiedObjectName viewName)
    {
    }

    @Override
    public void checkCanCreateViewWithSelectFromTable(Session session, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanCreateViewWithSelectFromView(Session session, QualifiedObjectName viewName)
    {
    }

    @Override
    public void checkCanGrantTablePrivilege(Session session, Privilege privilege, QualifiedObjectName tableName, String grantee, boolean withGrantOption)
    {
    }

    @Override
    public void checkCanRevokeTablePrivilege(Session session, Privilege privilege, QualifiedObjectName tableName, String revokee, boolean grantOptionFor)
    {
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
    }

    @Override
    public void checkCanSetCatalogSessionProperty(TransactionId transactionId, Identity identity, String catalogName, String propertyName)
    {
    }
}
