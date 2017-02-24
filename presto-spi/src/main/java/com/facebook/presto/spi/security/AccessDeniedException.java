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

import com.facebook.presto.spi.PrestoException;

import java.security.Principal;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.lang.String.format;

public class AccessDeniedException
        extends PrestoException
{
    public AccessDeniedException(String message)
    {
        super(PERMISSION_DENIED, "Access Denied: " + message);
    }

    public static void denySetUser(Principal principal, String userName)
    {
        denySetUser(principal, userName, null);
    }

    public static void denySetUser(Principal principal, String userName, String extraInfo)
    {
        throw new AccessDeniedException(format("Principal %s cannot become user %s%s", principal, userName, formatExtraInfo(extraInfo)));
    }

    public static void denyCreateSchema(String schemaName)
    {
        denyCreateSchema(schemaName, null);
    }

    public static void denyCreateSchema(String schemaName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot create schema %s%s", schemaName, formatExtraInfo(extraInfo)));
    }

    public static void denyDropSchema(String schemaName)
    {
        denyDropSchema(schemaName, null);
    }

    public static void denyDropSchema(String schemaName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot drop schema %s%s", schemaName, formatExtraInfo(extraInfo)));
    }

    public static void denyRenameSchema(String schemaName, String newSchemaName)
    {
        denyRenameSchema(schemaName, newSchemaName, null);
    }

    public static void denyRenameSchema(String schemaName, String newSchemaName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot rename schema from %s to %s%s", schemaName, newSchemaName, formatExtraInfo(extraInfo)));
    }

    public static void denyShowSchemas()
    {
        denyShowSchemas(null);
    }

    public static void denyShowSchemas(String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot show schemas%s", formatExtraInfo(extraInfo)));
    }

    public static void denyCreateTable(String tableName)
    {
        denyCreateTable(tableName, null);
    }

    public static void denyCreateTable(String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot create table %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyDropTable(String tableName)
    {
        denyDropTable(tableName, null);
    }

    public static void denyDropTable(String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot drop table %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyRenameTable(String tableName, String newTableName)
    {
        denyRenameTable(tableName, newTableName, null);
    }

    public static void denyRenameTable(String tableName, String newTableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot rename table from %s to %s%s", tableName, newTableName, formatExtraInfo(extraInfo)));
    }

    public static void denyShowTables(String schemaName)
    {
        denyShowTables(schemaName, null);
    }

    public static void denyShowTables(String schemaName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot show tables in %s%s", schemaName, formatExtraInfo(extraInfo)));
    }

    public static void denyAddColumn(String tableName)
    {
        denyAddColumn(tableName, null);
    }

    public static void denyAddColumn(String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot add a column to table %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyRenameColumn(String tableName)
    {
        denyRenameColumn(tableName, null);
    }

    public static void denyRenameColumn(String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot rename a column in table %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denySelectTable(String tableName)
    {
        denySelectTable(tableName, null);
    }

    public static void denySelectTable(String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot select from table %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyInsertTable(String tableName)
    {
        denyInsertTable(tableName, null);
    }

    public static void denyInsertTable(String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot insert into table %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyDeleteTable(String tableName)
    {
        denyDeleteTable(tableName, null);
    }

    public static void denyDeleteTable(String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot delete from table %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyCreateView(String viewName)
    {
        denyCreateView(viewName, null);
    }

    public static void denyCreateView(String viewName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot create view %s%s", viewName, formatExtraInfo(extraInfo)));
    }

    public static void denyCreateViewWithSelect(String sourceName)
    {
        denyCreateViewWithSelect(sourceName, null);
    }

    public static void denyCreateViewWithSelect(String sourceName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot create view that selects from %s%s", sourceName, formatExtraInfo(extraInfo)));
    }

    public static void denyDropView(String viewName)
    {
        denyDropView(viewName, null);
    }

    public static void denyDropView(String viewName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot drop view %s%s", viewName, formatExtraInfo(extraInfo)));
    }

    public static void denySelectView(String viewName)
    {
        denySelectView(viewName, null);
    }

    public static void denySelectView(String viewName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot select from view %s%s", viewName, formatExtraInfo(extraInfo)));
    }

    public static void denyGrantTablePrivilege(String privilege, String tableName)
    {
        denyGrantTablePrivilege(privilege, tableName, null);
    }

    public static void denyGrantTablePrivilege(String privilege, String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot grant privilege %s on table %s%s", privilege, tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyRevokeTablePrivilege(String privilege, String tableName)
    {
        denyRevokeTablePrivilege(privilege, tableName, null);
    }

    public static void denyRevokeTablePrivilege(String privilege, String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot revoke privilege %s on table %s%s", privilege, tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyShowGrants(String qualifiedTablePrefix)
    {
        denyShowGrants(qualifiedTablePrefix, null);
    }

    public static void denyShowGrants(String qualifiedTablePrefix, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot show grants on %s%s", qualifiedTablePrefix, formatExtraInfo(extraInfo)));
    }

    public static void denySetSystemSessionProperty(String propertyName)
    {
        denySetSystemSessionProperty(propertyName, null);
    }

    public static void denySetSystemSessionProperty(String propertyName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot set system session property %s%s", propertyName, formatExtraInfo(extraInfo)));
    }

    public static void denySetCatalogSessionProperty(String catalogName, String propertyName)
    {
        denySetCatalogSessionProperty(catalogName, propertyName, null);
    }

    public static void denySetCatalogSessionProperty(String catalogName, String propertyName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot set catalog session property %s.%s%s", catalogName, propertyName, formatExtraInfo(extraInfo)));
    }

    public static void denySetCatalogSessionProperty(String propertyName)
    {
        throw new AccessDeniedException(format("Cannot set catalog session property %s", propertyName));
    }

    public static void denyCreateRole(String roleName)
    {
        throw new AccessDeniedException(format("Cannot create role %s", roleName));
    }

    public static void denyDropRole(String roleName)
    {
        throw new AccessDeniedException(format("Cannot drop role %s", roleName));
    }

    public static void denyGrantRoles(Set<String> roles, Set<PrestoPrincipal> grantees)
    {
        throw new AccessDeniedException(format("Cannot grant roles %s to %s ", roles, grantees));
    }

    public static void denyRevokeRoles(Set<String> roles, Set<PrestoPrincipal> grantees)
    {
        throw new AccessDeniedException(format("Cannot revoke roles %s from %s ", roles, grantees));
    }

    public static void denySetRole(String role)
    {
        throw new AccessDeniedException(format("Cannot set role %s", role));
    }

    private static Object formatExtraInfo(String extraInfo)
    {
        if (extraInfo == null || extraInfo.isEmpty()) {
            return "";
        }
        return ": " + extraInfo;
    }
}
