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

import com.facebook.presto.spi.CatalogSchemaName;
import com.facebook.presto.spi.CatalogSchemaTableName;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.security.SystemAccessControl;

import java.security.Principal;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public abstract class ForwardingSystemAccessControl
        implements SystemAccessControl
{
    public static SystemAccessControl of(Supplier<SystemAccessControl> systemAccessControlSupplier)
    {
        requireNonNull(systemAccessControlSupplier, "systemAccessControlSupplier is null");
        return new ForwardingSystemAccessControl()
        {
            @Override
            protected SystemAccessControl delegate()
            {
                return systemAccessControlSupplier.get();
            }
        };
    }

    protected abstract SystemAccessControl delegate();

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        delegate().checkCanSetUser(principal, userName);
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        delegate().checkCanSetSystemSessionProperty(identity, propertyName);
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, String catalogName)
    {
        delegate().checkCanAccessCatalog(identity, catalogName);
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, Set<String> catalogs)
    {
        return delegate().filterCatalogs(identity, catalogs);
    }

    @Override
    public void checkCanCreateSchema(Identity identity, CatalogSchemaName schema)
    {
        delegate().checkCanCreateSchema(identity, schema);
    }

    @Override
    public void checkCanDropSchema(Identity identity, CatalogSchemaName schema)
    {
        delegate().checkCanDropSchema(identity, schema);
    }

    @Override
    public void checkCanRenameSchema(Identity identity, CatalogSchemaName schema, String newSchemaName)
    {
        delegate().checkCanRenameSchema(identity, schema, newSchemaName);
    }

    @Override
    public void checkCanShowSchemas(Identity identity, String catalogName)
    {
        delegate().checkCanShowSchemas(identity, catalogName);
    }

    @Override
    public Set<String> filterSchemas(Identity identity, String catalogName, Set<String> schemaNames)
    {
        return delegate().filterSchemas(identity, catalogName, schemaNames);
    }

    @Override
    public void checkCanCreateTable(Identity identity, CatalogSchemaTableName table)
    {
        delegate().checkCanCreateTable(identity, table);
    }

    @Override
    public void checkCanDropTable(Identity identity, CatalogSchemaTableName table)
    {
        delegate().checkCanDropTable(identity, table);
    }

    @Override
    public void checkCanRenameTable(Identity identity, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        delegate().checkCanRenameTable(identity, table, newTable);
    }

    @Override
    public void checkCanShowTablesMetadata(Identity identity, CatalogSchemaName schema)
    {
        delegate().checkCanShowTablesMetadata(identity, schema);
    }

    @Override
    public Set<SchemaTableName> filterTables(Identity identity, String catalogName, Set<SchemaTableName> tableNames)
    {
        return delegate().filterTables(identity, catalogName, tableNames);
    }

    @Override
    public void checkCanAddColumn(Identity identity, CatalogSchemaTableName table)
    {
        delegate().checkCanAddColumn(identity, table);
    }

    @Override
    public void checkCanDropColumn(Identity identity, CatalogSchemaTableName table)
    {
        delegate().checkCanDropColumn(identity, table);
    }

    @Override
    public void checkCanRenameColumn(Identity identity, CatalogSchemaTableName table)
    {
        delegate().checkCanRenameColumn(identity, table);
    }

    @Override
    public void checkCanSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns)
    {
        delegate().checkCanSelectFromColumns(identity, table, columns);
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, CatalogSchemaTableName table)
    {
        delegate().checkCanInsertIntoTable(identity, table);
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, CatalogSchemaTableName table)
    {
        delegate().checkCanDeleteFromTable(identity, table);
    }

    @Override
    public void checkCanCreateView(Identity identity, CatalogSchemaTableName view)
    {
        delegate().checkCanCreateView(identity, view);
    }

    @Override
    public void checkCanDropView(Identity identity, CatalogSchemaTableName view)
    {
        delegate().checkCanDropView(identity, view);
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns)
    {
        delegate().checkCanCreateViewWithSelectFromColumns(identity, table, columns);
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String catalogName, String propertyName)
    {
        delegate().checkCanSetCatalogSessionProperty(identity, catalogName, propertyName);
    }

    @Override
    public void checkCanGrantTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table, String grantee, boolean withGrantOption)
    {
        delegate().checkCanGrantTablePrivilege(identity, privilege, table, grantee, withGrantOption);
    }

    @Override
    public void checkCanRevokeTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table, String revokee, boolean grantOptionFor)
    {
        delegate().checkCanRevokeTablePrivilege(identity, privilege, table, revokee, grantOptionFor);
    }
}
