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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.plugin.base.security.ForwardingSystemAccessControl;
import com.facebook.presto.plugin.base.security.SchemaAccessControlRule;
import com.facebook.presto.security.CatalogAccessControlRule.AccessMode;
import com.facebook.presto.spi.CatalogSchemaTableName;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.security.SystemAccessControl;
import com.facebook.presto.spi.security.SystemAccessControlFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;

import java.nio.file.Paths;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.facebook.presto.plugin.base.JsonUtils.parseJson;
import static com.facebook.presto.plugin.base.security.FileBasedAccessControlConfig.SECURITY_CONFIG_FILE;
import static com.facebook.presto.plugin.base.security.FileBasedAccessControlConfig.SECURITY_REFRESH_PERIOD;
import static com.facebook.presto.security.CatalogAccessControlRule.AccessMode.ALL;
import static com.facebook.presto.security.CatalogAccessControlRule.AccessMode.READ_ONLY;
import static com.facebook.presto.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static com.facebook.presto.spi.security.AccessDeniedException.denyAddColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCatalogAccess;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateSchema;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDeleteTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropSchema;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static com.facebook.presto.spi.security.AccessDeniedException.denyInsertTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameSchema;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRevokeTablePrivilege;
import static com.facebook.presto.spi.security.AccessDeniedException.denySetUser;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FileBasedSystemAccessControl
        implements SystemAccessControl
{
    private static final Logger log = Logger.get(FileBasedSystemAccessControl.class);

    public static final String NAME = "file";

    private final List<CatalogAccessControlRule> catalogRules;
    private final Optional<List<PrincipalUserMatchRule>> principalUserMatchRules;
    private final Optional<List<SchemaAccessControlRule>> schemaRules;

    private FileBasedSystemAccessControl(List<CatalogAccessControlRule> catalogRules, Optional<List<PrincipalUserMatchRule>> principalUserMatchRules, Optional<List<SchemaAccessControlRule>> schemaRules)
    {
        this.catalogRules = catalogRules;
        this.principalUserMatchRules = principalUserMatchRules;
        this.schemaRules = schemaRules;
    }

    public static class Factory
            implements SystemAccessControlFactory
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public SystemAccessControl create(Map<String, String> config)
        {
            requireNonNull(config, "config is null");

            String configFileName = config.get(SECURITY_CONFIG_FILE);
            checkState(configFileName != null, "Security configuration must contain the '%s' property", SECURITY_CONFIG_FILE);

            if (config.containsKey(SECURITY_REFRESH_PERIOD)) {
                Duration refreshPeriod;
                try {
                    refreshPeriod = Duration.valueOf(config.get(SECURITY_REFRESH_PERIOD));
                }
                catch (IllegalArgumentException e) {
                    throw invalidRefreshPeriodException(config, configFileName);
                }
                if (refreshPeriod.toMillis() == 0) {
                    throw invalidRefreshPeriodException(config, configFileName);
                }
                return ForwardingSystemAccessControl.of(memoizeWithExpiration(
                        () -> {
                            log.info("Refreshing system access control from %s", configFileName);
                            return create(configFileName);
                        },
                        refreshPeriod.toMillis(),
                        MILLISECONDS));
            }
            return create(configFileName);
        }

        private PrestoException invalidRefreshPeriodException(Map<String, String> config, String configFileName)
        {
            return new PrestoException(
                    CONFIGURATION_INVALID,
                    format("Invalid duration value '%s' for property '%s' in '%s'", config.get(SECURITY_REFRESH_PERIOD), SECURITY_REFRESH_PERIOD, configFileName));
        }

        private SystemAccessControl create(String configFileName)
        {
            FileBasedSystemAccessControlRules rules = parseJson(Paths.get(configFileName), FileBasedSystemAccessControlRules.class);

            ImmutableList.Builder<CatalogAccessControlRule> catalogRulesBuilder = ImmutableList.builder();
            catalogRulesBuilder.addAll(rules.getCatalogRules());

            // Hack to allow Presto Admin to access the "system" catalog for retrieving server status.
            // todo Change userRegex from ".*" to one particular user that Presto Admin will be restricted to run as
            catalogRulesBuilder.add(new CatalogAccessControlRule(
                    ALL,
                    Optional.of(Pattern.compile(".*")),
                    Optional.of(Pattern.compile("system"))));

            return new FileBasedSystemAccessControl(catalogRulesBuilder.build(), rules.getPrincipalUserMatchRules(), rules.getSchemaRules());
        }
    }

    @Override
    public void checkCanSetUser(Identity identity, AccessControlContext context, Optional<Principal> principal, String userName)
    {
        requireNonNull(principal, "principal is null");
        requireNonNull(userName, "userName is null");

        if (!principalUserMatchRules.isPresent()) {
            return;
        }

        if (!principal.isPresent()) {
            denySetUser(principal, userName);
        }

        String principalName = principal.get().getName();

        for (PrincipalUserMatchRule rule : principalUserMatchRules.get()) {
            Optional<Boolean> allowed = rule.match(principalName, userName);
            if (allowed.isPresent()) {
                if (allowed.get()) {
                    return;
                }
                denySetUser(principal, userName);
            }
        }

        denySetUser(principal, userName);
    }

    @Override
    public void checkQueryIntegrity(Identity identity, AccessControlContext context, String query)
    {
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, AccessControlContext context, String propertyName)
    {
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, AccessControlContext context, String catalogName)
    {
        if (!canAccessCatalog(identity, catalogName, READ_ONLY)) {
            denyCatalogAccess(catalogName);
        }
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, AccessControlContext context, Set<String> catalogs)
    {
        ImmutableSet.Builder<String> filteredCatalogs = ImmutableSet.builder();
        for (String catalog : catalogs) {
            if (canAccessCatalog(identity, catalog, READ_ONLY)) {
                filteredCatalogs.add(catalog);
            }
        }
        return filteredCatalogs.build();
    }

    private boolean canAccessCatalog(Identity identity, String catalogName, AccessMode requiredAccess)
    {
        for (CatalogAccessControlRule rule : catalogRules) {
            Optional<AccessMode> accessMode = rule.match(identity.getUser(), catalogName);
            if (accessMode.isPresent()) {
                return accessMode.get().implies(requiredAccess);
            }
        }
        return false;
    }

    @Override
    public void checkCanCreateSchema(Identity identity, AccessControlContext context, CatalogSchemaName schema)
    {
        if (!isSchemaOwner(identity, schema)) {
            denyCreateSchema(schema.toString());
        }
    }

    @Override
    public void checkCanDropSchema(Identity identity, AccessControlContext context, CatalogSchemaName schema)
    {
        if (!isSchemaOwner(identity, schema)) {
            denyDropSchema(schema.toString());
        }
    }

    @Override
    public void checkCanRenameSchema(Identity identity, AccessControlContext context, CatalogSchemaName schema, String newSchemaName)
    {
        if (!isSchemaOwner(identity, schema) || !isSchemaOwner(identity, new CatalogSchemaName(schema.getCatalogName(), newSchemaName))) {
            denyRenameSchema(schema.toString(), newSchemaName);
        }
    }

    @Override
    public void checkCanShowSchemas(Identity identity, AccessControlContext context, String catalogName)
    {
    }

    @Override
    public Set<String> filterSchemas(Identity identity, AccessControlContext context, String catalogName, Set<String> schemaNames)
    {
        if (!canAccessCatalog(identity, catalogName, READ_ONLY)) {
            return ImmutableSet.of();
        }

        return schemaNames;
    }

    @Override
    public void checkCanCreateTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(identity, table.getCatalogName(), ALL)) {
            denyCreateTable(table.toString());
        }
    }

    @Override
    public void checkCanDropTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(identity, table.getCatalogName(), ALL)) {
            denyDropTable(table.toString());
        }
    }

    @Override
    public void checkCanRenameTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        if (!canAccessCatalog(identity, table.getCatalogName(), ALL)) {
            denyRenameTable(table.toString(), newTable.toString());
        }
    }

    @Override
    public void checkCanShowTablesMetadata(Identity identity, AccessControlContext context, CatalogSchemaName schema)
    {
    }

    @Override
    public Set<SchemaTableName> filterTables(Identity identity, AccessControlContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        if (!canAccessCatalog(identity, catalogName, READ_ONLY)) {
            return ImmutableSet.of();
        }

        return tableNames;
    }

    @Override
    public void checkCanAddColumn(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(identity, table.getCatalogName(), ALL)) {
            denyAddColumn(table.toString());
        }
    }

    @Override
    public void checkCanDropColumn(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(identity, table.getCatalogName(), ALL)) {
            denyDropColumn(table.toString());
        }
    }

    @Override
    public void checkCanRenameColumn(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(identity, table.getCatalogName(), ALL)) {
            denyRenameColumn(table.toString());
        }
    }

    @Override
    public void checkCanSelectFromColumns(Identity identity, AccessControlContext context, CatalogSchemaTableName table, Set<String> columns)
    {
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(identity, table.getCatalogName(), ALL)) {
            denyInsertTable(table.toString());
        }
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(identity, table.getCatalogName(), ALL)) {
            denyDeleteTable(table.toString());
        }
    }

    @Override
    public void checkCanCreateView(Identity identity, AccessControlContext context, CatalogSchemaTableName view)
    {
        if (!canAccessCatalog(identity, view.getCatalogName(), ALL)) {
            denyCreateView(view.toString());
        }
    }

    @Override
    public void checkCanDropView(Identity identity, AccessControlContext context, CatalogSchemaTableName view)
    {
        if (!canAccessCatalog(identity, view.getCatalogName(), ALL)) {
            denyDropView(view.toString());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(Identity identity, AccessControlContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        if (!canAccessCatalog(identity, table.getCatalogName(), ALL)) {
            denyCreateViewWithSelect(table.toString(), identity);
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, AccessControlContext context, String catalogName, String propertyName)
    {
    }

    @Override
    public void checkCanGrantTablePrivilege(Identity identity, AccessControlContext context, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal grantee, boolean withGrantOption)
    {
        if (!canAccessCatalog(identity, table.getCatalogName(), ALL)) {
            denyGrantTablePrivilege(privilege.toString(), table.toString());
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(Identity identity, AccessControlContext context, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal revokee, boolean grantOptionFor)
    {
        if (!canAccessCatalog(identity, table.getCatalogName(), ALL)) {
            denyRevokeTablePrivilege(privilege.toString(), table.toString());
        }
    }

    private boolean isSchemaOwner(Identity identity, CatalogSchemaName schema)
    {
        if (!canAccessCatalog(identity, schema.getCatalogName(), ALL)) {
            return false;
        }

        if (!schemaRules.isPresent()) {
            return true;
        }

        for (SchemaAccessControlRule rule : schemaRules.get()) {
            Optional<Boolean> owner = rule.match(identity.getUser(), schema.getSchemaName());
            if (owner.isPresent()) {
                return owner.get();
            }
        }
        return false;
    }
}
