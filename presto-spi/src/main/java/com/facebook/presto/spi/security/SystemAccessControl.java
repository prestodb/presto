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

import com.facebook.presto.spi.CatalogSchemaTableName;

import java.security.Principal;

import static com.facebook.presto.spi.security.AccessDeniedException.denyAddColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDeleteTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static com.facebook.presto.spi.security.AccessDeniedException.denyInsertTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRevokeTablePrivilege;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectView;
import static com.facebook.presto.spi.security.AccessDeniedException.denySetCatalogSessionProperty;

public interface SystemAccessControl
{
    /**
     * Check if the principal is allowed to be the specified user.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanSetUser(Principal principal, String userName);

    /**
     * Check if identity is allowed to set the specified system property.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanSetSystemSessionProperty(Identity identity, String propertyName);

    /**
     * Check if identity is allowed to create the specified table in a catalog.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanCreateTable(Identity identity, CatalogSchemaTableName table)
    {
        denyCreateTable(table.toString());
    }

    /**
     * Check if identity is allowed to drop the specified table in a catalog.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanDropTable(Identity identity, CatalogSchemaTableName table)
    {
        denyDropTable(table.toString());
    }

    /**
     * Check if identity is allowed to rename the specified table in a catalog.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanRenameTable(Identity identity, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        denyRenameTable(table.toString(), newTable.toString());
    }

    /**
     * Check if identity is allowed to add columns to the specified table in a catalog.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanAddColumn(Identity identity, CatalogSchemaTableName table)
    {
        denyAddColumn(table.toString());
    }

    /**
     * Check if identity is allowed to rename a column in the specified table in a catalog.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanRenameColumn(Identity identity, CatalogSchemaTableName table)
    {
        denyRenameColumn(table.toString());
    }

    /**
     * Check if identity is allowed to select from the specified table in a catalog.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanSelectFromTable(Identity identity, CatalogSchemaTableName table)
    {
        denySelectTable(table.toString());
    }

    /**
     * Check if identity is allowed to insert into the specified table in a catalog.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanInsertIntoTable(Identity identity, CatalogSchemaTableName table)
    {
        denyInsertTable(table.toString());
    }

    /**
     * Check if identity is allowed to delete from the specified table in a catalog.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanDeleteFromTable(Identity identity, CatalogSchemaTableName table)
    {
        denyDeleteTable(table.toString());
    }

    /**
     * Check if identity is allowed to create the specified view in a catalog.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanCreateView(Identity identity, CatalogSchemaTableName view)
    {
        denyCreateView(view.toString());
    }

    /**
     * Check if identity is allowed to drop the specified view in a catalog.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanDropView(Identity identity, CatalogSchemaTableName view)
    {
        denyDropView(view.toString());
    }

    /**
     * Check if identity is allowed to select from the specified view in a catalog.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanSelectFromView(Identity identity, CatalogSchemaTableName view)
    {
        denySelectView(view.toString());
    }

    /**
     * Check if identity is allowed to create a view that selects from the specified table in a catalog.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanCreateViewWithSelectFromTable(Identity identity, CatalogSchemaTableName table)
    {
        denyCreateViewWithSelect(table.toString());
    }

    /**
     * Check if identity is allowed to create a view that selects from the specified view in a catalog.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanCreateViewWithSelectFromView(Identity identity, CatalogSchemaTableName view)
    {
        denyCreateViewWithSelect(view.toString());
    }

    /**
     * Check if identity is allowed to set the specified property in a catalog.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanSetCatalogSessionProperty(Identity identity, String catalogName, String propertyName)
    {
        denySetCatalogSessionProperty(propertyName);
    }

    /**
     * Check if identity is allowed to grant to any other user the specified privilege on the specified table.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanGrantTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table)
    {
        denyGrantTablePrivilege(privilege.toString(), table.toString());
    }

    /**
     * Check if identity is allowed to revoke the specified privilege on the specified table from any user.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanRevokeTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table)
    {
        denyRevokeTablePrivilege(privilege.toString(), table.toString());
    }
}
