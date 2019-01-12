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
package io.prestosql.security;

import com.google.common.collect.ImmutableSet;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.Privilege;
import io.prestosql.transaction.TransactionId;

import java.security.Principal;
import java.util.Optional;
import java.util.Set;

import static io.prestosql.spi.security.AccessDeniedException.denyAddColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyCatalogAccess;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateSchema;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateTable;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateView;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static io.prestosql.spi.security.AccessDeniedException.denyDeleteTable;
import static io.prestosql.spi.security.AccessDeniedException.denyDropColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyDropSchema;
import static io.prestosql.spi.security.AccessDeniedException.denyDropTable;
import static io.prestosql.spi.security.AccessDeniedException.denyDropView;
import static io.prestosql.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static io.prestosql.spi.security.AccessDeniedException.denyInsertTable;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameSchema;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameTable;
import static io.prestosql.spi.security.AccessDeniedException.denyRevokeTablePrivilege;
import static io.prestosql.spi.security.AccessDeniedException.denySelectColumns;
import static io.prestosql.spi.security.AccessDeniedException.denySetCatalogSessionProperty;
import static io.prestosql.spi.security.AccessDeniedException.denySetSystemSessionProperty;
import static io.prestosql.spi.security.AccessDeniedException.denySetUser;
import static io.prestosql.spi.security.AccessDeniedException.denyShowSchemas;
import static io.prestosql.spi.security.AccessDeniedException.denyShowTablesMetadata;

public class DenyAllAccessControl
        implements AccessControl
{
    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        denySetUser(principal, userName);
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, Set<String> catalogs)
    {
        return ImmutableSet.of();
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, String catalogName)
    {
        denyCatalogAccess(catalogName);
    }

    @Override
    public void checkCanCreateSchema(TransactionId transactionId, Identity identity, CatalogSchemaName schemaName)
    {
        denyCreateSchema(schemaName.toString());
    }

    @Override
    public void checkCanDropSchema(TransactionId transactionId, Identity identity, CatalogSchemaName schemaName)
    {
        denyDropSchema(schemaName.toString());
    }

    @Override
    public void checkCanRenameSchema(TransactionId transactionId, Identity identity, CatalogSchemaName schemaName, String newSchemaName)
    {
        denyRenameSchema(schemaName.toString(), newSchemaName);
    }

    @Override
    public void checkCanCreateTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        denyCreateTable(tableName.toString());
    }

    @Override
    public void checkCanDropTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        denyDropTable(tableName.toString());
    }

    @Override
    public void checkCanRenameTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName, QualifiedObjectName newTableName)
    {
        denyRenameTable(tableName.toString(), newTableName.toString());
    }

    @Override
    public void checkCanShowTablesMetadata(TransactionId transactionId, Identity identity, CatalogSchemaName schema)
    {
        denyShowTablesMetadata(schema.toString());
    }

    @Override
    public Set<SchemaTableName> filterTables(TransactionId transactionId, Identity identity, String catalogName, Set<SchemaTableName> tableNames)
    {
        return ImmutableSet.of();
    }

    @Override
    public void checkCanShowSchemas(TransactionId transactionId, Identity identity, String catalogName)
    {
        denyShowSchemas();
    }

    @Override
    public Set<String> filterSchemas(TransactionId transactionId, Identity identity, String catalogName, Set<String> schemaNames)
    {
        return ImmutableSet.of();
    }

    @Override
    public void checkCanAddColumns(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        denyAddColumn(tableName.toString());
    }

    @Override
    public void checkCanRenameColumn(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        denyRenameColumn(tableName.toString());
    }

    @Override
    public void checkCanDropColumn(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        denyDropColumn(tableName.toString());
    }

    @Override
    public void checkCanInsertIntoTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        denyInsertTable(tableName.toString());
    }

    @Override
    public void checkCanDeleteFromTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        denyDeleteTable(tableName.toString());
    }

    @Override
    public void checkCanCreateView(TransactionId transactionId, Identity identity, QualifiedObjectName viewName)
    {
        denyCreateView(viewName.toString());
    }

    @Override
    public void checkCanDropView(TransactionId transactionId, Identity identity, QualifiedObjectName viewName)
    {
        denyDropView(viewName.toString());
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(TransactionId transactionId, Identity identity, QualifiedObjectName tableName, Set<String> columnNames)
    {
        denyCreateViewWithSelect(tableName.toString(), identity);
    }

    @Override
    public void checkCanGrantTablePrivilege(TransactionId transactionId, Identity identity, Privilege privilege, QualifiedObjectName tableName, String grantee, boolean withGrantOption)
    {
        denyGrantTablePrivilege(privilege.name(), tableName.toString());
    }

    @Override
    public void checkCanRevokeTablePrivilege(TransactionId transactionId, Identity identity, Privilege privilege, QualifiedObjectName tableName, String revokee, boolean grantOptionFor)
    {
        denyRevokeTablePrivilege(privilege.name(), tableName.toString());
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        denySetSystemSessionProperty(propertyName);
    }

    @Override
    public void checkCanSetCatalogSessionProperty(TransactionId transactionId, Identity identity, String catalogName, String propertyName)
    {
        denySetCatalogSessionProperty(catalogName, propertyName);
    }

    @Override
    public void checkCanSelectFromColumns(TransactionId transactionId, Identity identity, QualifiedObjectName tableName, Set<String> columnNames)
    {
        denySelectColumns(tableName.toString(), columnNames);
    }
}
