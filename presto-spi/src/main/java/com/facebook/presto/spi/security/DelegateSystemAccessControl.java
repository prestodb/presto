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
package com.facebook.presto.spi.security;

import com.facebook.presto.spi.CatalogSchemaName;
import com.facebook.presto.spi.CatalogSchemaTableName;
import com.facebook.presto.spi.SchemaTableName;

import java.security.Principal;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class DelegateSystemAccessControl
        implements SystemAccessControl
{
    private final Supplier<SystemAccessControl> delegate;

    public DelegateSystemAccessControl(SystemAccessControl delegate)
    {
        this(() -> requireNonNull(delegate, "delegate is null"));
    }

    public DelegateSystemAccessControl(Supplier<SystemAccessControl> delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        getDelegate().checkCanSetUser(principal, userName);
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        getDelegate().checkCanSetSystemSessionProperty(identity, propertyName);
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, String catalogName)
    {
        getDelegate().checkCanAccessCatalog(identity, catalogName);
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, Set<String> catalogs)
    {
        return getDelegate().filterCatalogs(identity, catalogs);
    }

    @Override
    public void checkCanCreateSchema(Identity identity, CatalogSchemaName schema)
    {
        getDelegate().checkCanCreateSchema(identity, schema);
    }

    @Override
    public void checkCanDropSchema(Identity identity, CatalogSchemaName schema)
    {
        getDelegate().checkCanDropSchema(identity, schema);
    }

    @Override
    public void checkCanRenameSchema(Identity identity, CatalogSchemaName schema, String newSchemaName)
    {
        getDelegate().checkCanRenameSchema(identity, schema, newSchemaName);
    }

    @Override
    public void checkCanShowSchemas(Identity identity, String catalogName)
    {
        getDelegate().checkCanShowSchemas(identity, catalogName);
    }

    @Override
    public Set<String> filterSchemas(Identity identity, String catalogName, Set<String> schemaNames)
    {
        return getDelegate().filterSchemas(identity, catalogName, schemaNames);
    }

    @Override
    public void checkCanCreateTable(Identity identity, CatalogSchemaTableName table)
    {
        getDelegate().checkCanCreateTable(identity, table);
    }

    @Override
    public void checkCanDropTable(Identity identity, CatalogSchemaTableName table)
    {
        getDelegate().checkCanDropTable(identity, table);
    }

    @Override
    public void checkCanRenameTable(Identity identity, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        getDelegate().checkCanRenameTable(identity, table, newTable);
    }

    @Override
    public void checkCanShowTablesMetadata(Identity identity, CatalogSchemaName schema)
    {
        getDelegate().checkCanShowTablesMetadata(identity, schema);
    }

    @Override
    public Set<SchemaTableName> filterTables(Identity identity, String catalogName, Set<SchemaTableName> tableNames)
    {
        return getDelegate().filterTables(identity, catalogName, tableNames);
    }

    @Override
    public void checkCanAddColumn(Identity identity, CatalogSchemaTableName table)
    {
        getDelegate().checkCanAddColumn(identity, table);
    }

    @Override
    public void checkCanDropColumn(Identity identity, CatalogSchemaTableName table)
    {
        getDelegate().checkCanDropColumn(identity, table);
    }

    @Override
    public void checkCanRenameColumn(Identity identity, CatalogSchemaTableName table)
    {
        getDelegate().checkCanRenameColumn(identity, table);
    }

    @Override
    public void checkCanSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns)
    {
        getDelegate().checkCanSelectFromColumns(identity, table, columns);
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, CatalogSchemaTableName table)
    {
        getDelegate().checkCanInsertIntoTable(identity, table);
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, CatalogSchemaTableName table)
    {
        getDelegate().checkCanDeleteFromTable(identity, table);
    }

    @Override
    public void checkCanCreateView(Identity identity, CatalogSchemaTableName view)
    {
        getDelegate().checkCanCreateView(identity, view);
    }

    @Override
    public void checkCanDropView(Identity identity, CatalogSchemaTableName view)
    {
        getDelegate().checkCanDropView(identity, view);
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns)
    {
        getDelegate().checkCanCreateViewWithSelectFromColumns(identity, table, columns);
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String catalogName, String propertyName)
    {
        getDelegate().checkCanSetCatalogSessionProperty(identity, catalogName, propertyName);
    }

    @Override
    public void checkCanGrantTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table, String grantee, boolean withGrantOption)
    {
        getDelegate().checkCanGrantTablePrivilege(identity, privilege, table, grantee, withGrantOption);
    }

    @Override
    public void checkCanRevokeTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table, String revokee, boolean grantOptionFor)
    {
        getDelegate().checkCanRevokeTablePrivilege(identity, privilege, table, revokee, grantOptionFor);
    }

    private SystemAccessControl getDelegate()
    {
        return delegate.get();
    }
}
