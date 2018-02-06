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
import com.facebook.presto.spi.security.SystemAccessControlFactory;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.security.Principal;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.spi.security.AccessDeniedException.denySetUser;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class UsernamePrincipalMatchingAccessControl
        implements SystemAccessControl
{
    public static final String NAME = "username-principal-matching";

    private static final UsernamePrincipalMatchingAccessControl INSTANCE = new UsernamePrincipalMatchingAccessControl();

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
            checkArgument(config.isEmpty(), "This access controller does not support any configuration properties");
            return INSTANCE;
        }
    }

    @Override
    public void checkCanSetUser(Principal principal, String userName)
    {
        if (principal == null) {
            return;
        }

        if (principal instanceof KerberosPrincipal) {
            KerberosPrincipal kerberosPrincipal = (KerberosPrincipal) principal;
            String kerberosUserName = kerberosPrincipal.getName();
            String kerberosRealmName = kerberosPrincipal.getRealm();

            if (kerberosUserName.contains("/")) {
                kerberosUserName = kerberosUserName.substring(0, kerberosUserName.indexOf("/"));
            }
            else {
                kerberosUserName = kerberosUserName.substring(0, kerberosUserName.length() - kerberosRealmName.length() - 1);
            }

            if (!userName.equals(kerberosUserName)) {
                denySetUser(principal, userName);
            }
        }
        else {
            if (!userName.equals(principal.getName())) {
                denySetUser(principal, userName);
            }
        }
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, String catalogName)
    {
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, Set<String> catalogs)
    {
        return catalogs;
    }

    @Override
    public void checkCanCreateSchema(Identity identity, CatalogSchemaName schema)
    {
    }

    @Override
    public void checkCanDropSchema(Identity identity, CatalogSchemaName schema)
    {
    }

    @Override
    public void checkCanRenameSchema(Identity identity, CatalogSchemaName schema, String newSchemaName)
    {
    }

    @Override
    public void checkCanShowSchemas(Identity identity, String catalogName)
    {
    }

    @Override
    public Set<String> filterSchemas(Identity identity, String catalogName, Set<String> schemaNames)
    {
        return schemaNames;
    }

    @Override
    public void checkCanCreateTable(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanDropTable(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanRenameTable(Identity identity, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
    }

    @Override
    public void checkCanShowTablesMetadata(Identity identity, CatalogSchemaName schema)
    {
    }

    @Override
    public Set<SchemaTableName> filterTables(Identity identity, String catalogName, Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    @Override
    public void checkCanAddColumn(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanDropColumn(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanRenameColumn(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanSelectFromTable(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanCreateView(Identity identity, CatalogSchemaTableName view)
    {
    }

    @Override
    public void checkCanDropView(Identity identity, CatalogSchemaTableName view)
    {
    }

    @Override
    public void checkCanSelectFromView(Identity identity, CatalogSchemaTableName view)
    {
    }

    @Override
    public void checkCanCreateViewWithSelectFromTable(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanCreateViewWithSelectFromView(Identity identity, CatalogSchemaTableName view)
    {
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String catalogName, String propertyName)
    {
    }

    @Override
    public void checkCanGrantTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table, String grantee, boolean withGrantOption)
    {
    }

    @Override
    public void checkCanRevokeTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table, String revokee, boolean grantOptionFor)
    {
    }
}
