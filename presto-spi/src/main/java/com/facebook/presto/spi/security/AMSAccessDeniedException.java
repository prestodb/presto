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

import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;

import java.security.Principal;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_ACCESS_CATALOG;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_ADD_COLUMN_TO_TABLE;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_CREATE_ROLE;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_CREATE_SCHEMA;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_CREATE_TABLE;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_CREATE_VIEW;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_DELETE_FROM_TABLE;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_DROP_COLUMN_FROM_TABLE;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_DROP_ROLE;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_DROP_SCHEMA;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_DROP_TABLE;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_DROP_VIEW;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_GRANT_PRIVILEGE_ON_TABLE;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_GRANT_ROLES;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_INSERT_INTO_TABLE;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_RENAME_COLUMN_IN_TABLE;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_RENAME_SCHEMA;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_RENAME_TABLE;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_REVOKE_PRIVILEGE_ON_TABLE;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_REVOKE_ROLES;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_SELECT_FROM_COLUMNS_IN_TABLE;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_SELECT_FROM_TABLE;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_SELECT_FROM_VIEW;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_SET_CATALOG_SESSION_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_SET_ROLE;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_SET_SYSTEM_SESSION_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_SHOW_COLUMNS_OF_TABLE;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_SHOW_CREATE_TABLE;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_SHOW_CURRENT_ROLES_FROM_CATALOG;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_SHOW_METADATA_OF_TABLE;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_SHOW_ROLES_FROM_CATALOG;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_SHOW_ROLE_GRANTS_FROM_CATALOG;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_SHOW_SCHEMA;
import static com.facebook.presto.spi.StandardErrorCode.AMS_CANNOT_TRUNCATE_TABLE;
import static com.facebook.presto.spi.StandardErrorCode.AMS_PRINCIPAL_CANNOT_BECOME_USER;
import static com.facebook.presto.spi.StandardErrorCode.AMS_QUERY_INTEGRITY_CHECK_FAIL;
import static com.facebook.presto.spi.StandardErrorCode.AMS_VIEW_OWNER_CANNOT_CREATE_VIEW;
import static java.lang.String.format;

public class AMSAccessDeniedException
        extends PrestoException
{
    public AMSAccessDeniedException(ErrorCodeSupplier errorCode, String message)
    {
        super(errorCode, "Access Denied: " + message);
    }

    public static void denySetUser(Optional<Principal> principal, String userName)
    {
        denySetUser(principal, userName, null);
    }

    public static void denyQueryIntegrityCheck()
    {
        throw new AMSAccessDeniedException(AMS_QUERY_INTEGRITY_CHECK_FAIL, "Query integrity check failed.");
    }

    public static void denySetUser(Optional<Principal> principal, String userName, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_PRINCIPAL_CANNOT_BECOME_USER, format("Principal %s cannot become user %s%s", principal.orElse(null), userName, formatExtraInfo(extraInfo)));
    }

    public static void denyCatalogAccess(String catalogName)
    {
        denyCatalogAccess(catalogName, null);
    }

    public static void denyCatalogAccess(String catalogName, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_ACCESS_CATALOG, format("Cannot access catalog %s%s", catalogName, formatExtraInfo(extraInfo)));
    }

    public static void denyCreateSchema(String schemaName)
    {
        denyCreateSchema(schemaName, null);
    }

    public static void denyCreateSchema(String schemaName, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_CREATE_SCHEMA, format("Cannot create schema %s%s", schemaName, formatExtraInfo(extraInfo)));
    }

    public static void denyDropSchema(String schemaName)
    {
        denyDropSchema(schemaName, null);
    }

    public static void denyDropSchema(String schemaName, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_DROP_SCHEMA, format("Cannot drop schema %s%s", schemaName, formatExtraInfo(extraInfo)));
    }

    public static void denyRenameSchema(String schemaName, String newSchemaName)
    {
        denyRenameSchema(schemaName, newSchemaName, null);
    }

    public static void denyRenameSchema(String schemaName, String newSchemaName, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_RENAME_SCHEMA, format("Cannot rename schema from %s to %s%s", schemaName, newSchemaName, formatExtraInfo(extraInfo)));
    }

    public static void denyShowSchemas()
    {
        denyShowSchemas(null);
    }

    public static void denyShowSchemas(String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_SHOW_SCHEMA, format("Cannot show schemas%s", formatExtraInfo(extraInfo)));
    }

    public static void denyShowCreateTable(String tableName)
    {
        denyShowCreateTable(tableName, null);
    }

    public static void denyShowCreateTable(String tableName, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_SHOW_CREATE_TABLE, format("Cannot show create table for %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyCreateTable(String tableName)
    {
        denyCreateTable(tableName, null);
    }

    public static void denyCreateTable(String tableName, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_CREATE_TABLE, format("Cannot create table %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyDropTable(String tableName)
    {
        denyDropTable(tableName, null);
    }

    public static void denyDropTable(String tableName, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_DROP_TABLE, format("Cannot drop table %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyRenameTable(String tableName, String newTableName)
    {
        denyRenameTable(tableName, newTableName, null);
    }

    public static void denyRenameTable(String tableName, String newTableName, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_RENAME_TABLE, format("Cannot rename table from %s to %s%s", tableName, newTableName, formatExtraInfo(extraInfo)));
    }

    public static void denyShowTablesMetadata(String schemaName)
    {
        denyShowTablesMetadata(schemaName, null);
    }

    public static void denyShowTablesMetadata(String schemaName, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_SHOW_METADATA_OF_TABLE, format("Cannot show metadata of tables in %s%s", schemaName, formatExtraInfo(extraInfo)));
    }

    public static void denyShowColumnsMetadata(String tableName)
    {
        denyShowColumnsMetadata(tableName, null);
    }

    public static void denyShowColumnsMetadata(String tableName, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_SHOW_COLUMNS_OF_TABLE, format("Cannot show columns of table %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyAddColumn(String tableName)
    {
        denyAddColumn(tableName, null);
    }

    public static void denyAddColumn(String tableName, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_ADD_COLUMN_TO_TABLE, format("Cannot add a column to table %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyDropColumn(String tableName)
    {
        denyDropColumn(tableName, null);
    }

    public static void denyDropColumn(String tableName, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_DROP_COLUMN_FROM_TABLE, format("Cannot drop a column from table %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyRenameColumn(String tableName)
    {
        denyRenameColumn(tableName, null);
    }

    public static void denyRenameColumn(String tableName, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_RENAME_COLUMN_IN_TABLE, format("Cannot rename a column in table %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denySelectTable(String tableName)
    {
        denySelectTable(tableName, null);
    }

    public static void denySelectTable(String tableName, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_SELECT_FROM_TABLE, format("Cannot select from table %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyInsertTable(String tableName)
    {
        denyInsertTable(tableName, null);
    }

    public static void denyInsertTable(String tableName, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_INSERT_INTO_TABLE, format("Cannot insert into table %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyDeleteTable(String tableName)
    {
        denyDeleteTable(tableName, null);
    }

    public static void denyDeleteTable(String tableName, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_DELETE_FROM_TABLE, format("Cannot delete from table %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyTruncateTable(String tableName)
    {
        denyTruncateTable(tableName, null);
    }

    public static void denyTruncateTable(String tableName, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_TRUNCATE_TABLE, format("Cannot truncate table %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyCreateView(String viewName)
    {
        denyCreateView(viewName, null);
    }

    public static void denyCreateView(String viewName, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_CREATE_VIEW, format("Cannot create view %s%s", viewName, formatExtraInfo(extraInfo)));
    }

    public static void denyCreateViewWithSelect(String sourceName, Identity identity)
    {
        denyCreateViewWithSelect(sourceName, identity.toConnectorIdentity());
    }

    public static void denyCreateViewWithSelect(String sourceName, ConnectorIdentity identity)
    {
        denyCreateViewWithSelect(sourceName, identity, null);
    }

    public static void denyCreateViewWithSelect(String sourceName, ConnectorIdentity identity, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_VIEW_OWNER_CANNOT_CREATE_VIEW, format("View owner '%s' cannot create view that selects from %s%s", identity.getUser(), sourceName, formatExtraInfo(extraInfo)));
    }

    public static void denyDropView(String viewName)
    {
        denyDropView(viewName, null);
    }

    public static void denyDropView(String viewName, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_DROP_VIEW, format("Cannot drop view %s%s", viewName, formatExtraInfo(extraInfo)));
    }

    public static void denySelectView(String viewName)
    {
        denySelectView(viewName, null);
    }

    public static void denySelectView(String viewName, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_SELECT_FROM_VIEW, format("Cannot select from view %s%s", viewName, formatExtraInfo(extraInfo)));
    }

    public static void denyGrantTablePrivilege(String privilege, String tableName)
    {
        denyGrantTablePrivilege(privilege, tableName, null);
    }

    public static void denyGrantTablePrivilege(String privilege, String tableName, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_GRANT_PRIVILEGE_ON_TABLE, format("Cannot grant privilege %s on table %s%s", privilege, tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyRevokeTablePrivilege(String privilege, String tableName)
    {
        denyRevokeTablePrivilege(privilege, tableName, null);
    }

    public static void denyRevokeTablePrivilege(String privilege, String tableName, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_REVOKE_PRIVILEGE_ON_TABLE, format("Cannot revoke privilege %s on table %s%s", privilege, tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyShowRoles(String catalogName)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_SHOW_ROLES_FROM_CATALOG, format("Cannot show roles from catalog %s", catalogName));
    }

    public static void denyShowCurrentRoles(String catalogName)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_SHOW_CURRENT_ROLES_FROM_CATALOG, format("Cannot show current roles from catalog %s", catalogName));
    }

    public static void denyShowRoleGrants(String catalogName)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_SHOW_ROLE_GRANTS_FROM_CATALOG, format("Cannot show role grants from catalog %s", catalogName));
    }

    public static void denySetSystemSessionProperty(String propertyName)
    {
        denySetSystemSessionProperty(propertyName, null);
    }

    public static void denySetSystemSessionProperty(String propertyName, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_SET_SYSTEM_SESSION_PROPERTY, format("Cannot set system session property %s%s", propertyName, formatExtraInfo(extraInfo)));
    }

    public static void denySetCatalogSessionProperty(String catalogName, String propertyName)
    {
        denySetCatalogSessionProperty(catalogName, propertyName, null);
    }

    public static void denySetCatalogSessionProperty(String catalogName, String propertyName, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_SET_CATALOG_SESSION_PROPERTY, format("Cannot set catalog session property %s.%s%s", catalogName, propertyName, formatExtraInfo(extraInfo)));
    }

    public static void denySetCatalogSessionProperty(String propertyName)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_SET_CATALOG_SESSION_PROPERTY, format("Cannot set catalog session property %s", propertyName));
    }

    public static void denySelectColumns(String tableName, Collection<String> columnNames)
    {
        denySelectColumns(tableName, columnNames, null);
    }

    public static void denySelectColumns(String tableName, Collection<String> columnNames, String extraInfo)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_SELECT_FROM_COLUMNS_IN_TABLE, format("Cannot select from columns %s in table or view %s%s", columnNames, tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyCreateRole(String roleName)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_CREATE_ROLE, format("Cannot create role %s", roleName));
    }

    public static void denyDropRole(String roleName)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_DROP_ROLE, format("Cannot drop role %s", roleName));
    }

    public static void denyGrantRoles(Set<String> roles, Set<PrestoPrincipal> grantees)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_GRANT_ROLES, format("Cannot grant roles %s to %s ", roles, grantees));
    }

    public static void denyRevokeRoles(Set<String> roles, Set<PrestoPrincipal> grantees)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_REVOKE_ROLES, format("Cannot revoke roles %s from %s ", roles, grantees));
    }

    public static void denySetRole(String role)
    {
        throw new AMSAccessDeniedException(AMS_CANNOT_SET_ROLE, format("Cannot set role %s", role));
    }

    private static Object formatExtraInfo(String extraInfo)
    {
        if (extraInfo == null || extraInfo.isEmpty()) {
            return "";
        }
        return ": " + extraInfo;
    }
}
