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

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.CatalogSchemaTableName;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.AuthorizedIdentity;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.security.SystemAccessControl;
import com.facebook.presto.spi.security.ViewExpression;

import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
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
    public void checkCanSetUser(Identity identity, AccessControlContext context, Optional<Principal> principal, String userName)
    {
        delegate().checkCanSetUser(identity, context, principal, userName);
    }

    @Override
    public AuthorizedIdentity selectAuthorizedIdentity(Identity identity, AccessControlContext context, String userName, List<X509Certificate> certificates)
    {
        return delegate().selectAuthorizedIdentity(identity, context, userName, certificates);
    }

    @Override
    public void checkQueryIntegrity(Identity identity, AccessControlContext context, String query, Map<QualifiedObjectName, ViewDefinition> viewDefinitions, Map<QualifiedObjectName, MaterializedViewDefinition> materializedViewDefinitions)
    {
        delegate().checkQueryIntegrity(identity, context, query, viewDefinitions, materializedViewDefinitions);
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, AccessControlContext context, String propertyName)
    {
        delegate().checkCanSetSystemSessionProperty(identity, context, propertyName);
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, AccessControlContext context, String catalogName)
    {
        delegate().checkCanAccessCatalog(identity, context, catalogName);
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, AccessControlContext context, Set<String> catalogs)
    {
        return delegate().filterCatalogs(identity, context, catalogs);
    }

    @Override
    public void checkCanCreateSchema(Identity identity, AccessControlContext context, CatalogSchemaName schema)
    {
        delegate().checkCanCreateSchema(identity, context, schema);
    }

    @Override
    public void checkCanDropSchema(Identity identity, AccessControlContext context, CatalogSchemaName schema)
    {
        delegate().checkCanDropSchema(identity, context, schema);
    }

    @Override
    public void checkCanRenameSchema(Identity identity, AccessControlContext context, CatalogSchemaName schema, String newSchemaName)
    {
        delegate().checkCanRenameSchema(identity, context, schema, newSchemaName);
    }

    @Override
    public void checkCanShowSchemas(Identity identity, AccessControlContext context, String catalogName)
    {
        delegate().checkCanShowSchemas(identity, context, catalogName);
    }

    @Override
    public Set<String> filterSchemas(Identity identity, AccessControlContext context, String catalogName, Set<String> schemaNames)
    {
        return delegate().filterSchemas(identity, context, catalogName, schemaNames);
    }

    @Override
    public void checkCanShowCreateTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        delegate().checkCanShowCreateTable(identity, context, table);
    }

    @Override
    public void checkCanCreateTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        delegate().checkCanCreateTable(identity, context, table);
    }

    @Override
    public void checkCanSetTableProperties(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        delegate().checkCanSetTableProperties(identity, context, table);
    }

    @Override
    public void checkCanDropTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        delegate().checkCanDropTable(identity, context, table);
    }

    @Override
    public void checkCanRenameTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        delegate().checkCanRenameTable(identity, context, table, newTable);
    }

    @Override
    public void checkCanShowTablesMetadata(Identity identity, AccessControlContext context, CatalogSchemaName schema)
    {
        delegate().checkCanShowTablesMetadata(identity, context, schema);
    }

    @Override
    public Set<SchemaTableName> filterTables(Identity identity, AccessControlContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        return delegate().filterTables(identity, context, catalogName, tableNames);
    }

    @Override
    public void checkCanShowColumnsMetadata(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        delegate().checkCanShowColumnsMetadata(identity, context, table);
    }

    @Override
    public List<ColumnMetadata> filterColumns(Identity identity, AccessControlContext context, CatalogSchemaTableName table, List<ColumnMetadata> columns)
    {
        return delegate().filterColumns(identity, context, table, columns);
    }

    @Override
    public void checkCanAddColumn(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        delegate().checkCanAddColumn(identity, context, table);
    }

    @Override
    public void checkCanDropColumn(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        delegate().checkCanDropColumn(identity, context, table);
    }

    @Override
    public void checkCanRenameColumn(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        delegate().checkCanRenameColumn(identity, context, table);
    }

    @Override
    public void checkCanSelectFromColumns(Identity identity, AccessControlContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        delegate().checkCanSelectFromColumns(identity, context, table, columns);
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        delegate().checkCanInsertIntoTable(identity, context, table);
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        delegate().checkCanDeleteFromTable(identity, context, table);
    }

    @Override
    public void checkCanTruncateTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        delegate().checkCanTruncateTable(identity, context, table);
    }

    @Override
    public void checkCanUpdateTableColumns(Identity identity, AccessControlContext context, CatalogSchemaTableName table, Set<String> updatedColumnNames)
    {
        delegate().checkCanUpdateTableColumns(identity, context, table, updatedColumnNames);
    }

    @Override
    public void checkCanCreateView(Identity identity, AccessControlContext context, CatalogSchemaTableName view)
    {
        delegate().checkCanCreateView(identity, context, view);
    }

    @Override
    public void checkCanRenameView(Identity identity, AccessControlContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView)
    {
        delegate().checkCanRenameView(identity, context, view, newView);
    }

    @Override
    public void checkCanDropView(Identity identity, AccessControlContext context, CatalogSchemaTableName view)
    {
        delegate().checkCanDropView(identity, context, view);
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(Identity identity, AccessControlContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        delegate().checkCanCreateViewWithSelectFromColumns(identity, context, table, columns);
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, AccessControlContext context, String catalogName, String propertyName)
    {
        delegate().checkCanSetCatalogSessionProperty(identity, context, catalogName, propertyName);
    }

    @Override
    public void checkCanGrantTablePrivilege(Identity identity, AccessControlContext context, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal grantee, boolean withGrantOption)
    {
        delegate().checkCanGrantTablePrivilege(identity, context, privilege, table, grantee, withGrantOption);
    }

    @Override
    public void checkCanRevokeTablePrivilege(Identity identity, AccessControlContext context, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal revokee, boolean grantOptionFor)
    {
        delegate().checkCanRevokeTablePrivilege(identity, context, privilege, table, revokee, grantOptionFor);
    }

    @Override
    public void checkCanDropBranch(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        delegate().checkCanDropBranch(identity, context, table);
    }

    @Override
    public void checkCanDropTag(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        delegate().checkCanDropTag(identity, context, table);
    }

    @Override
    public void checkCanDropConstraint(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        delegate().checkCanDropConstraint(identity, context, table);
    }

    @Override
    public void checkCanAddConstraint(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        delegate().checkCanAddConstraint(identity, context, table);
    }

    @Override
    public List<ViewExpression> getRowFilters(Identity identity, AccessControlContext context, CatalogSchemaTableName tableName)
    {
        return delegate().getRowFilters(identity, context, tableName);
    }

    @Override
    public Map<ColumnMetadata, ViewExpression> getColumnMasks(Identity identity, AccessControlContext context, CatalogSchemaTableName tableName, List<ColumnMetadata> columns)
    {
        return delegate().getColumnMasks(identity, context, tableName, columns);
    }
}
