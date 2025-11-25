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
import com.facebook.presto.common.RuntimeUnit;
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
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

public final class StatsRecordingSystemAccessControl
        implements SystemAccessControl
{
    private final Stats stats = new Stats();
    private final AtomicReference<SystemAccessControl> delegate = new AtomicReference<>();

    public StatsRecordingSystemAccessControl(SystemAccessControl delegate)
    {
        updateDelegate(delegate);
    }

    public void updateDelegate(SystemAccessControl delegate)
    {
        this.delegate.set(requireNonNull(delegate, "delegate is null"));
    }

    public Stats getStats()
    {
        return stats;
    }

    @Override
    public void checkCanSetUser(Identity identity, AccessControlContext context, Optional<Principal> principal, String userName)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanSetUser(identity, context, principal, userName);
        }
        catch (RuntimeException e) {
            stats.checkCanSetUser.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanSetUser", RuntimeUnit.NANO, duration);
            stats.checkCanSetUser.record(duration);
        }
    }

    @Override
    public AuthorizedIdentity selectAuthorizedIdentity(Identity identity, AccessControlContext context, String userName, List<X509Certificate> certificates)
    {
        long start = System.nanoTime();
        try {
            return delegate.get().selectAuthorizedIdentity(identity, context, userName, certificates);
        }
        catch (RuntimeException e) {
            stats.selectAuthorizedIdentity.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.selectAuthorizedIdentity", RuntimeUnit.NANO, duration);
            stats.selectAuthorizedIdentity.record(duration);
        }
    }

    @Override
    public void checkQueryIntegrity(Identity identity, AccessControlContext context, String query, Map<QualifiedObjectName, ViewDefinition> viewDefinitions, Map<QualifiedObjectName, MaterializedViewDefinition> materializedViewDefinitions)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkQueryIntegrity(identity, context, query, viewDefinitions, materializedViewDefinitions);
        }
        catch (RuntimeException e) {
            stats.checkQueryIntegrity.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkQueryIntegrity", RuntimeUnit.NANO, duration);
            stats.checkQueryIntegrity.record(duration);
        }
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, AccessControlContext context, String propertyName)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanSetSystemSessionProperty(identity, context, propertyName);
        }
        catch (RuntimeException e) {
            stats.checkCanSetSystemSessionProperty.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanSetSystemSessionProperty", RuntimeUnit.NANO, duration);
            stats.checkCanSetSystemSessionProperty.record(duration);
        }
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, AccessControlContext context, String catalogName)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanAccessCatalog(identity, context, catalogName);
        }
        catch (RuntimeException e) {
            stats.checkCanAccessCatalog.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanAccessCatalog", RuntimeUnit.NANO, duration);
            stats.checkCanAccessCatalog.record(duration);
        }
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, AccessControlContext context, Set<String> catalogs)
    {
        long start = System.nanoTime();
        try {
            return delegate.get().filterCatalogs(identity, context, catalogs);
        }
        catch (RuntimeException e) {
            stats.filterCatalogs.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.filterCatalogs", RuntimeUnit.NANO, duration);
            stats.filterCatalogs.record(duration);
        }
    }

    @Override
    public void checkCanCreateSchema(Identity identity, AccessControlContext context, CatalogSchemaName schema)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanCreateSchema(identity, context, schema);
        }
        catch (RuntimeException e) {
            stats.checkCanCreateSchema.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanCreateSchema", RuntimeUnit.NANO, duration);
            stats.checkCanCreateSchema.record(duration);
        }
    }

    @Override
    public void checkCanDropSchema(Identity identity, AccessControlContext context, CatalogSchemaName schema)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanDropSchema(identity, context, schema);
        }
        catch (RuntimeException e) {
            stats.checkCanDropSchema.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanDropSchema", RuntimeUnit.NANO, duration);
            stats.checkCanDropSchema.record(duration);
        }
    }

    @Override
    public void checkCanRenameSchema(Identity identity, AccessControlContext context, CatalogSchemaName schema, String newSchemaName)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanRenameSchema(identity, context, schema, newSchemaName);
        }
        catch (RuntimeException e) {
            stats.checkCanRenameSchema.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanRenameSchema", RuntimeUnit.NANO, duration);
            stats.checkCanRenameSchema.record(duration);
        }
    }

    @Override
    public void checkCanShowSchemas(Identity identity, AccessControlContext context, String catalogName)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanShowSchemas(identity, context, catalogName);
        }
        catch (RuntimeException e) {
            stats.checkCanShowSchemas.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanShowSchemas", RuntimeUnit.NANO, duration);
            stats.checkCanShowSchemas.record(duration);
        }
    }

    @Override
    public Set<String> filterSchemas(Identity identity, AccessControlContext context, String catalogName, Set<String> schemaNames)
    {
        long start = System.nanoTime();
        try {
            return delegate.get().filterSchemas(identity, context, catalogName, schemaNames);
        }
        catch (RuntimeException e) {
            stats.filterSchemas.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.filterSchemas", RuntimeUnit.NANO, duration);
            stats.filterSchemas.record(duration);
        }
    }

    @Override
    public void checkCanShowCreateTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanShowCreateTable(identity, context, table);
        }
        catch (RuntimeException e) {
            stats.checkCanShowCreateTable.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanShowCreateTable", RuntimeUnit.NANO, duration);
            stats.checkCanShowCreateTable.record(duration);
        }
    }

    @Override
    public void checkCanCreateTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanCreateTable(identity, context, table);
        }
        catch (RuntimeException e) {
            stats.checkCanCreateTable.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanCreateTable", RuntimeUnit.NANO, duration);
            stats.checkCanCreateTable.record(duration);
        }
    }

    @Override
    public void checkCanSetTableProperties(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanSetTableProperties(identity, context, table);
        }
        catch (RuntimeException e) {
            stats.checkCanSetTableProperties.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanSetTableProperties", RuntimeUnit.NANO, duration);
            stats.checkCanSetTableProperties.record(duration);
        }
    }

    @Override
    public void checkCanDropTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanDropTable(identity, context, table);
        }
        catch (RuntimeException e) {
            stats.checkCanDropTable.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanDropTable", RuntimeUnit.NANO, duration);
            stats.checkCanDropTable.record(duration);
        }
    }

    @Override
    public void checkCanRenameTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanRenameTable(identity, context, table, newTable);
        }
        catch (RuntimeException e) {
            stats.checkCanRenameTable.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanRenameTable", RuntimeUnit.NANO, duration);
            stats.checkCanRenameTable.record(duration);
        }
    }

    @Override
    public void checkCanShowTablesMetadata(Identity identity, AccessControlContext context, CatalogSchemaName schema)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanShowTablesMetadata(identity, context, schema);
        }
        catch (RuntimeException e) {
            stats.checkCanShowTablesMetadata.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanShowTablesMetadata", RuntimeUnit.NANO, duration);
            stats.checkCanShowTablesMetadata.record(duration);
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(Identity identity, AccessControlContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        long start = System.nanoTime();
        try {
            return delegate.get().filterTables(identity, context, catalogName, tableNames);
        }
        catch (RuntimeException e) {
            stats.filterTables.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.filterTables", RuntimeUnit.NANO, duration);
            stats.filterTables.record(duration);
        }
    }

    @Override
    public void checkCanShowColumnsMetadata(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanShowColumnsMetadata(identity, context, table);
        }
        catch (RuntimeException e) {
            stats.checkCanShowColumnsMetadata.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanShowColumnsMetadata", RuntimeUnit.NANO, duration);
            stats.checkCanShowColumnsMetadata.record(duration);
        }
    }

    @Override
    public List<ColumnMetadata> filterColumns(Identity identity, AccessControlContext context, CatalogSchemaTableName table, List<ColumnMetadata> columns)
    {
        long start = System.nanoTime();
        try {
            return delegate.get().filterColumns(identity, context, table, columns);
        }
        catch (RuntimeException e) {
            stats.filterColumns.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.filterColumns", RuntimeUnit.NANO, duration);
            stats.filterColumns.record(duration);
        }
    }

    @Override
    public void checkCanAddColumn(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanAddColumn(identity, context, table);
        }
        catch (RuntimeException e) {
            stats.checkCanAddColumn.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanAddColumn", RuntimeUnit.NANO, duration);
            stats.checkCanAddColumn.record(duration);
        }
    }

    @Override
    public void checkCanDropColumn(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanDropColumn(identity, context, table);
        }
        catch (RuntimeException e) {
            stats.checkCanDropColumn.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanDropColumn", RuntimeUnit.NANO, duration);
            stats.checkCanDropColumn.record(duration);
        }
    }

    @Override
    public void checkCanRenameColumn(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanRenameColumn(identity, context, table);
        }
        catch (RuntimeException e) {
            stats.checkCanRenameColumn.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanRenameColumn", RuntimeUnit.NANO, duration);
            stats.checkCanRenameColumn.record(duration);
        }
    }

    @Override
    public void checkCanSelectFromColumns(Identity identity, AccessControlContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanSelectFromColumns(identity, context, table, columns);
        }
        catch (RuntimeException e) {
            stats.checkCanSelectFromColumns.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanSelectFromColumns", RuntimeUnit.NANO, duration);
            stats.checkCanSelectFromColumns.record(duration);
        }
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanInsertIntoTable(identity, context, table);
        }
        catch (RuntimeException e) {
            stats.checkCanInsertIntoTable.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanInsertIntoTable", RuntimeUnit.NANO, duration);
            stats.checkCanInsertIntoTable.record(duration);
        }
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanDeleteFromTable(identity, context, table);
        }
        catch (RuntimeException e) {
            stats.checkCanDeleteFromTable.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanDeleteFromTable", RuntimeUnit.NANO, duration);
            stats.checkCanDeleteFromTable.record(duration);
        }
    }

    @Override
    public void checkCanTruncateTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanTruncateTable(identity, context, table);
        }
        catch (RuntimeException e) {
            stats.checkCanTruncateTable.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanTruncateTable", RuntimeUnit.NANO, duration);
            stats.checkCanTruncateTable.record(duration);
        }
    }

    @Override
    public void checkCanUpdateTableColumns(Identity identity, AccessControlContext context, CatalogSchemaTableName table, Set<String> updatedColumnNames)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanUpdateTableColumns(identity, context, table, updatedColumnNames);
        }
        catch (RuntimeException e) {
            stats.checkCanUpdateTableColumns.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanUpdateTableColumns", RuntimeUnit.NANO, duration);
            stats.checkCanUpdateTableColumns.record(duration);
        }
    }

    @Override
    public void checkCanCreateView(Identity identity, AccessControlContext context, CatalogSchemaTableName view)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanCreateView(identity, context, view);
        }
        catch (RuntimeException e) {
            stats.checkCanCreateView.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanCreateView", RuntimeUnit.NANO, duration);
            stats.checkCanCreateView.record(duration);
        }
    }

    @Override
    public void checkCanRenameView(Identity identity, AccessControlContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanRenameView(identity, context, view, newView);
        }
        catch (RuntimeException e) {
            stats.checkCanRenameView.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanRenameView", RuntimeUnit.NANO, duration);
            stats.checkCanRenameView.record(duration);
        }
    }

    @Override
    public void checkCanDropView(Identity identity, AccessControlContext context, CatalogSchemaTableName view)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanDropView(identity, context, view);
        }
        catch (RuntimeException e) {
            stats.checkCanDropView.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanDropView", RuntimeUnit.NANO, duration);
            stats.checkCanDropView.record(duration);
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(Identity identity, AccessControlContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanCreateViewWithSelectFromColumns(identity, context, table, columns);
        }
        catch (RuntimeException e) {
            stats.checkCanCreateViewWithSelectFromColumns.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanCreateViewWithSelectFromColumns", RuntimeUnit.NANO, duration);
            stats.checkCanCreateViewWithSelectFromColumns.record(duration);
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, AccessControlContext context, String catalogName, String propertyName)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanSetCatalogSessionProperty(identity, context, catalogName, propertyName);
        }
        catch (RuntimeException e) {
            stats.checkCanSetCatalogSessionProperty.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanSetCatalogSessionProperty", RuntimeUnit.NANO, duration);
            stats.checkCanSetCatalogSessionProperty.record(duration);
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(Identity identity, AccessControlContext context, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal grantee, boolean withGrantOption)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanGrantTablePrivilege(identity, context, privilege, table, grantee, withGrantOption);
        }
        catch (RuntimeException e) {
            stats.checkCanGrantTablePrivilege.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanGrantTablePrivilege", RuntimeUnit.NANO, duration);
            stats.checkCanGrantTablePrivilege.record(duration);
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(Identity identity, AccessControlContext context, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal revokee, boolean grantOptionFor)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanRevokeTablePrivilege(identity, context, privilege, table, revokee, grantOptionFor);
        }
        catch (RuntimeException e) {
            stats.checkCanRevokeTablePrivilege.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanRevokeTablePrivilege", RuntimeUnit.NANO, duration);
            stats.checkCanRevokeTablePrivilege.record(duration);
        }
    }

    @Override
    public void checkCanDropBranch(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanDropBranch(identity, context, table);
        }
        catch (RuntimeException e) {
            stats.checkCanDropBranch.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanDropBranch", RuntimeUnit.NANO, duration);
            stats.checkCanDropBranch.record(duration);
        }
    }

    @Override
    public void checkCanDropTag(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanDropTag(identity, context, table);
        }
        catch (RuntimeException e) {
            stats.checkCanDropTag.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanDropTag", RuntimeUnit.NANO, duration);
            stats.checkCanDropTag.record(duration);
        }
    }

    @Override
    public void checkCanDropConstraint(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanDropConstraint(identity, context, table);
        }
        catch (RuntimeException e) {
            stats.checkCanDropConstraint.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanDropConstraint", RuntimeUnit.NANO, duration);
            stats.checkCanDropConstraint.record(duration);
        }
    }

    @Override
    public void checkCanAddConstraint(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        long start = System.nanoTime();
        try {
            delegate.get().checkCanAddConstraint(identity, context, table);
        }
        catch (RuntimeException e) {
            stats.checkCanAddConstraint.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.checkCanAddConstraint", RuntimeUnit.NANO, duration);
            stats.checkCanAddConstraint.record(duration);
        }
    }

    @Override
    public List<ViewExpression> getRowFilters(Identity identity, AccessControlContext context, CatalogSchemaTableName tableName)
    {
        long start = System.nanoTime();
        try {
            return delegate.get().getRowFilters(identity, context, tableName);
        }
        catch (RuntimeException e) {
            stats.getRowFilters.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.getRowFilters", RuntimeUnit.NANO, duration);
            stats.getRowFilters.record(duration);
        }
    }

    @Override
    public Map<ColumnMetadata, ViewExpression> getColumnMasks(Identity identity, AccessControlContext context, CatalogSchemaTableName tableName, List<ColumnMetadata> columns)
    {
        long start = System.nanoTime();
        try {
            return delegate.get().getColumnMasks(identity, context, tableName, columns);
        }
        catch (RuntimeException e) {
            stats.getColumnMasks.recordFailure();
            throw e;
        }
        finally {
            long duration = System.nanoTime() - start;
            context.getRuntimeStats().addMetricValue("systemAccessControl.getColumnMasks", RuntimeUnit.NANO, duration);
            stats.getColumnMasks.record(duration);
        }
    }

    public static class Stats
    {
        final SystemAccessControlStats checkCanSetUser = new SystemAccessControlStats();
        final SystemAccessControlStats selectAuthorizedIdentity = new SystemAccessControlStats();
        final SystemAccessControlStats checkQueryIntegrity = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanSetSystemSessionProperty = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanAccessCatalog = new SystemAccessControlStats();
        final SystemAccessControlStats filterCatalogs = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanCreateSchema = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanDropSchema = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanRenameSchema = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanShowSchemas = new SystemAccessControlStats();
        final SystemAccessControlStats filterSchemas = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanShowCreateTable = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanCreateTable = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanSetTableProperties = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanDropTable = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanRenameTable = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanShowTablesMetadata = new SystemAccessControlStats();
        final SystemAccessControlStats filterTables = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanShowColumnsMetadata = new SystemAccessControlStats();
        final SystemAccessControlStats filterColumns = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanAddColumn = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanDropColumn = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanRenameColumn = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanSelectFromColumns = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanInsertIntoTable = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanDeleteFromTable = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanTruncateTable = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanUpdateTableColumns = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanCreateView = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanRenameView = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanDropView = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanCreateViewWithSelectFromColumns = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanSetCatalogSessionProperty = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanGrantTablePrivilege = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanRevokeTablePrivilege = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanDropBranch = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanDropTag = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanDropConstraint = new SystemAccessControlStats();
        final SystemAccessControlStats checkCanAddConstraint = new SystemAccessControlStats();
        final SystemAccessControlStats getRowFilters = new SystemAccessControlStats();
        final SystemAccessControlStats getColumnMasks = new SystemAccessControlStats();

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanSetUser()
        {
            return checkCanSetUser;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getSelectAuthorizedIdentity()
        {
            return selectAuthorizedIdentity;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckQueryIntegrity()
        {
            return checkQueryIntegrity;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanSetSystemSessionProperty()
        {
            return checkCanSetSystemSessionProperty;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanAccessCatalog()
        {
            return checkCanAccessCatalog;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getFilterCatalogs()
        {
            return filterCatalogs;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanCreateSchema()
        {
            return checkCanCreateSchema;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanDropSchema()
        {
            return checkCanDropSchema;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanRenameSchema()
        {
            return checkCanRenameSchema;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanShowSchemas()
        {
            return checkCanShowSchemas;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getFilterSchemas()
        {
            return filterSchemas;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanShowCreateTable()
        {
            return checkCanShowCreateTable;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanCreateTable()
        {
            return checkCanCreateTable;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanSetTableProperties()
        {
            return checkCanSetTableProperties;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanDropTable()
        {
            return checkCanDropTable;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanRenameTable()
        {
            return checkCanRenameTable;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanShowTablesMetadata()
        {
            return checkCanShowTablesMetadata;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getFilterTables()
        {
            return filterTables;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanShowColumnsMetadata()
        {
            return checkCanShowColumnsMetadata;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getFilterColumns()
        {
            return filterColumns;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanAddColumn()
        {
            return checkCanAddColumn;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanDropColumn()
        {
            return checkCanDropColumn;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanRenameColumn()
        {
            return checkCanRenameColumn;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanSelectFromColumns()
        {
            return checkCanSelectFromColumns;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanInsertIntoTable()
        {
            return checkCanInsertIntoTable;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanDeleteFromTable()
        {
            return checkCanDeleteFromTable;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanTruncateTable()
        {
            return checkCanTruncateTable;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanUpdateTableColumns()
        {
            return checkCanUpdateTableColumns;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanCreateView()
        {
            return checkCanCreateView;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanRenameView()
        {
            return checkCanRenameView;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanDropView()
        {
            return checkCanDropView;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanCreateViewWithSelectFromColumns()
        {
            return checkCanCreateViewWithSelectFromColumns;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanSetCatalogSessionProperty()
        {
            return checkCanSetCatalogSessionProperty;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanGrantTablePrivilege()
        {
            return checkCanGrantTablePrivilege;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanRevokeTablePrivilege()
        {
            return checkCanRevokeTablePrivilege;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanDropConstraint()
        {
            return checkCanDropConstraint;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getCheckCanAddConstraint()
        {
            return checkCanAddConstraint;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getGetRowFilters()
        {
            return getRowFilters;
        }

        @Managed
        @Nested
        public SystemAccessControlStats getGetColumnMasks()
        {
            return getColumnMasks;
        }
    }
}
