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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.security.SystemAccessControl;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.security.Principal;

import static com.facebook.presto.spi.StandardErrorCode.PERMISSION_DENIED;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;

public class KerberosExactMatchAccessControl
        implements SystemAccessControl
{
    @Override
    public void checkCanSetUser(Principal principal, String userName)
    {
        // Allow unauthenticated access.
        if (principal == null) {
            return;
        }

        /*
         * This is almost certainly a configuration issue. If people are authenticating with e.g. LDAP,
         * this is not the SystemAccessControl plugin you should be using.
         */
        if (!(principal instanceof KerberosPrincipal)) {
            throw new PrestoException(
                    PERMISSION_DENIED,
                    "The 'kerberos-exact-match' SystemAccessControl plugin only works with " +
                    "Kerberos authentication. Contact your Presto administrator.");
        }

        KerberosPrincipal kerberosPrincipal = (KerberosPrincipal) principal;

        String realmName = kerberosPrincipal.getRealm();
        String kerberosUserName = kerberosPrincipal.getName();

        if (!isNullOrEmpty(realmName)) {
            kerberosUserName = kerberosUserName.substring(0, kerberosUserName.length() - realmName.length() - 1);
        }

        if (!userName.equals(kerberosUserName)) {
            throw new PrestoException(PERMISSION_DENIED, format("Principal %s may not set user %s", principal.getName(), userName));
        }
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
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
    public void checkCanAddColumn(Identity identity, CatalogSchemaTableName table)
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
    public void checkCanGrantTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanRevokeTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table)
    {
    }
}
