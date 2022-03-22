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

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.transaction.TransactionId;

import java.security.Principal;
import java.util.Optional;
import java.util.Set;

public class AllowAllAccessControl
        implements AccessControl
{
    @Override
    public void checkCanSetUser(Identity identity, AccessControlContext context, Optional<Principal> principal, String userName)
    {
    }

    @Override
    public void checkQueryIntegrity(Identity identity, AccessControlContext context, String query)
    {
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, AccessControlContext context, Set<String> catalogs)
    {
        return catalogs;
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, AccessControlContext context, String catalogName)
    {
    }

    @Override
    public void checkCanCreateSchema(TransactionId transactionId, Identity identity, AccessControlContext context, CatalogSchemaName schemaName)
    {
    }

    @Override
    public void checkCanDropSchema(TransactionId transactionId, Identity identity, AccessControlContext context, CatalogSchemaName schemaName)
    {
    }

    @Override
    public void checkCanRenameSchema(TransactionId transactionId, Identity identity, AccessControlContext context, CatalogSchemaName schemaName, String newSchemaName)
    {
    }

    @Override
    public void checkCanShowSchemas(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName)
    {
    }

    @Override
    public Set<String> filterSchemas(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName, Set<String> schemaNames)
    {
        return schemaNames;
    }

    @Override
    public void checkCanCreateTable(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanDropTable(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanRenameTable(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName, QualifiedObjectName newTableName)
    {
    }

    @Override
    public void checkCanShowTablesMetadata(TransactionId transactionId, Identity identity, AccessControlContext context, CatalogSchemaName schema)
    {
    }

    @Override
    public Set<SchemaTableName> filterTables(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    @Override
    public void checkCanAddColumns(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanDropColumn(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanRenameColumn(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanInsertIntoTable(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanDeleteFromTable(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanCreateView(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName viewName)
    {
    }

    @Override
    public void checkCanDropView(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName viewName)
    {
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {
    }

    @Override
    public void checkCanGrantTablePrivilege(TransactionId transactionId, Identity identity, AccessControlContext context, Privilege privilege, QualifiedObjectName tableName, PrestoPrincipal grantee, boolean withGrantOption)
    {
    }

    @Override
    public void checkCanRevokeTablePrivilege(TransactionId transactionId, Identity identity, AccessControlContext context, Privilege privilege, QualifiedObjectName tableName, PrestoPrincipal revokee, boolean grantOptionFor)
    {
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, AccessControlContext context, String propertyName)
    {
    }

    @Override
    public void checkCanSetCatalogSessionProperty(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName, String propertyName)
    {
    }

    @Override
    public void checkCanSelectFromColumns(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {
    }

    @Override
    public void checkCanCreateRole(TransactionId transactionId, Identity identity, AccessControlContext context, String role, Optional<PrestoPrincipal> grantor, String catalogName)
    {
    }

    @Override
    public void checkCanDropRole(TransactionId transactionId, Identity identity, AccessControlContext context, String role, String catalogName)
    {
    }

    @Override
    public void checkCanGrantRoles(TransactionId transactionId, Identity identity, AccessControlContext context, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor, String catalogName)
    {
    }

    @Override
    public void checkCanRevokeRoles(TransactionId transactionId, Identity identity, AccessControlContext context, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor, String catalogName)
    {
    }

    @Override
    public void checkCanSetRole(TransactionId requiredTransactionId, Identity identity, AccessControlContext context, String role, String catalog)
    {
    }

    @Override
    public void checkCanShowRoles(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName)
    {
    }

    @Override
    public void checkCanShowCurrentRoles(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName)
    {
    }

    @Override
    public void checkCanShowRoleGrants(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName)
    {
    }
}
