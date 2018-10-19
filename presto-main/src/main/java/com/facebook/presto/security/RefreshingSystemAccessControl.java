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

import com.facebook.presto.spi.CatalogSchemaName;
import com.facebook.presto.spi.CatalogSchemaTableName;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.security.SystemAccessControl;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.log.Logger;

import java.security.Principal;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class RefreshingSystemAccessControl
        implements SystemAccessControl
{
    private static final Logger LOG = Logger.get(RefreshingSystemAccessControl.class);

    static final String REFRESH_PERIOD_SEC = "security.refresh-sec";
    private static final String DEFAULT_REFRESH_PERIOD_SEC = "300"; // 5 mins
    static final String REFRESH_ENABLED = "security.refresh-enabled";
    private static final String DEFAULT_REFRESH = "false";
    private final ScheduledThreadPoolExecutor service;

    private AtomicReference<SystemAccessControl> delegate = new AtomicReference<>();

    /**
     * Add optional refreshing to the {@link SystemAccessControl} provided by the factory function. The previous
     * {@link SystemAccessControl} is provided as an {@link Optional} input - null, when there is no previous element
     * (as with the first time). If refreshing is not enabled, then the factory function is just used to create the
     * {@link SystemAccessControl} and returned.
     *
     * @param config access control configuration from
     *         {@link com.facebook.presto.spi.security.SystemAccessControlFactory#create(Map)}
     * @param factory create the next {@link SystemAccessControl}, optionally informed by the previous
     * @return a {@link SystemAccessControl} that might be able to refresh itself
     */
    static SystemAccessControl optionallyRefresh(Map<String, String> config,
            Supplier<Optional<SystemAccessControl>> factory)
    {
        if (Boolean.valueOf(config.getOrDefault(REFRESH_ENABLED, DEFAULT_REFRESH))) {
            long period = Long.valueOf(config.getOrDefault(REFRESH_PERIOD_SEC, DEFAULT_REFRESH_PERIOD_SEC));
            return new RefreshingSystemAccessControl(factory, period);
        }
        return factory.get().orElseThrow(() -> new IllegalStateException("Access Control factory did not provide an " +
                "initial SystemAccessControl."));
    }

    private RefreshingSystemAccessControl(Supplier<Optional<SystemAccessControl>> factory, long refreshPeriodSec)
    {
        Runnable command = () -> {
            try {
                factory.get().ifPresent(delegate::set);
            }
            catch (Exception e) {
                LOG.error("Failed to reload configuration", e);
            }
        };
        // run it the first time and ensure we have an access control
        command.run();
        Preconditions.checkState(this.delegate.get() != null, "No initial system control loaded!");
        this.service = new ScheduledThreadPoolExecutor(1,
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("refreshing-access-control-%d").build());
        service.scheduleWithFixedDelay(command, refreshPeriodSec, refreshPeriodSec, TimeUnit.SECONDS);
    }

    AtomicReference<SystemAccessControl> getDelegateForTesting()
    {
        return delegate;
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        delegate.get().checkCanSetUser(principal, userName);
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity,
            String propertyName)
    {
        delegate.get().checkCanSetSystemSessionProperty(identity, propertyName);
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, String catalogName)
    {
        delegate.get().checkCanAccessCatalog(identity, catalogName);
    }

    @Override
    public Set<String> filterCatalogs(Identity identity,
            Set<String> catalogs)
    {
        return delegate.get().filterCatalogs(identity, catalogs);
    }

    @Override
    public void checkCanCreateSchema(Identity identity,
            CatalogSchemaName schema)
    {
        delegate.get().checkCanCreateSchema(identity, schema);
    }

    @Override
    public void checkCanDropSchema(Identity identity,
            CatalogSchemaName schema)
    {
        delegate.get().checkCanDropSchema(identity, schema);
    }

    @Override
    public void checkCanRenameSchema(Identity identity,
            CatalogSchemaName schema, String newSchemaName)
    {
        delegate.get().checkCanRenameSchema(identity, schema, newSchemaName);
    }

    @Override
    public void checkCanShowSchemas(Identity identity, String catalogName)
    {
        delegate.get().checkCanShowSchemas(identity, catalogName);
    }

    @Override
    public Set<String> filterSchemas(Identity identity,
            String catalogName, Set<String> schemaNames)
    {
        return delegate.get().filterSchemas(identity, catalogName, schemaNames);
    }

    @Override
    public void checkCanCreateTable(Identity identity,
            CatalogSchemaTableName table)
    {
        delegate.get().checkCanCreateTable(identity, table);
    }

    @Override
    public void checkCanDropTable(Identity identity,
            CatalogSchemaTableName table)
    {
        delegate.get().checkCanDropTable(identity, table);
    }

    @Override
    public void checkCanRenameTable(Identity identity,
            CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        delegate.get().checkCanRenameTable(identity, table, newTable);
    }

    @Override
    public void checkCanShowTablesMetadata(Identity identity,
            CatalogSchemaName schema)
    {
        delegate.get().checkCanShowTablesMetadata(identity, schema);
    }

    @Override
    public Set<SchemaTableName> filterTables(
            Identity identity, String catalogName,
            Set<SchemaTableName> tableNames)
    {
        return delegate.get().filterTables(identity, catalogName, tableNames);
    }

    @Override
    public void checkCanAddColumn(Identity identity,
            CatalogSchemaTableName table)
    {
        delegate.get().checkCanAddColumn(identity, table);
    }

    @Override
    public void checkCanDropColumn(Identity identity,
            CatalogSchemaTableName table)
    {
        delegate.get().checkCanDropColumn(identity, table);
    }

    @Override
    public void checkCanRenameColumn(Identity identity,
            CatalogSchemaTableName table)
    {
        delegate.get().checkCanRenameColumn(identity, table);
    }

    @Override
    public void checkCanSelectFromColumns(Identity identity,
            CatalogSchemaTableName table, Set<String> columns)
    {
        delegate.get().checkCanSelectFromColumns(identity, table, columns);
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity,
            CatalogSchemaTableName table)
    {
        delegate.get().checkCanInsertIntoTable(identity, table);
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity,
            CatalogSchemaTableName table)
    {
        delegate.get().checkCanDeleteFromTable(identity, table);
    }

    @Override
    public void checkCanCreateView(Identity identity,
            CatalogSchemaTableName view)
    {
        delegate.get().checkCanCreateView(identity, view);
    }

    @Override
    public void checkCanDropView(Identity identity,
            CatalogSchemaTableName view)
    {
        delegate.get().checkCanDropView(identity, view);
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(Identity identity,
            CatalogSchemaTableName table, Set<String> columns)
    {
        delegate.get().checkCanCreateViewWithSelectFromColumns(identity, table, columns);
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity,
            String catalogName, String propertyName)
    {
        delegate.get().checkCanSetCatalogSessionProperty(identity, catalogName, propertyName);
    }

    @Override
    public void checkCanGrantTablePrivilege(Identity identity,
            Privilege privilege, CatalogSchemaTableName table,
            String grantee, boolean withGrantOption)
    {
        delegate.get().checkCanGrantTablePrivilege(identity, privilege, table, grantee, withGrantOption);
    }

    @Override
    public void checkCanRevokeTablePrivilege(Identity identity,
            Privilege privilege, CatalogSchemaTableName table,
            String revokee, boolean grantOptionFor)
    {
        delegate.get().checkCanRevokeTablePrivilege(identity, privilege, table, revokee, grantOptionFor);
    }
}
