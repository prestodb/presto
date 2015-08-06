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

import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.spi.security.Identity;

import java.security.Principal;

public interface AccessControl
{
    /**
     * Check if the principal is allowed to be the specified user.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanSetUser(Principal principal, String userName);

    /**
     * Check if identity is allowed to create the specified table.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanCreateTable(Identity identity, QualifiedTableName tableName);

    /**
     * Check if identity is allowed to drop the specified table.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanDropTable(Identity identity, QualifiedTableName tableName);

    /**
     * Check if identity is allowed to rename the specified table.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanRenameTable(Identity identity, QualifiedTableName tableName, QualifiedTableName newTableName);

    /**
     * Check if identity is allowed to add columns to the specified table.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanAddColumns(Identity identity, QualifiedTableName tableName);

    /**
     * Check if identity is allowed to rename a column in the specified table.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanRenameColumn(Identity identity, QualifiedTableName tableName);

    /**
     * Check if identity is allowed to select from the specified table.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanSelectFromTable(Identity identity, QualifiedTableName tableName);

    /**
     * Check if identity is allowed to insert into the specified table.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanInsertIntoTable(Identity identity, QualifiedTableName tableName);

    /**
     * Check if identity is allowed to delete from the specified table.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanDeleteFromTable(Identity identity, QualifiedTableName tableName);

    /**
     * Check if identity is allowed to create the specified view.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanCreateView(Identity identity, QualifiedTableName viewName);

    /**
     * Check if identity is allowed to drop the specified view.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanDropView(Identity identity, QualifiedTableName viewName);

    /**
     * Check if identity is allowed to select from the specified view.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanSelectFromView(Identity identity, QualifiedTableName viewName);

    /**
     * Check if identity is allowed to create a view that selects from the specified table.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanCreateViewWithSelectFromTable(Identity identity, QualifiedTableName tableName);

    /**
     * Check if identity is allowed to create a view that selects from the specified view.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanCreateViewWithSelectFromView(Identity identity, QualifiedTableName viewName);

    /**
     * Check if identity is allowed to set the specified system property.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanSetSystemSessionProperty(Identity identity, String propertyName);

    /**
     * Check if identity is allowed to set the specified catalog property.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanSetCatalogSessionProperty(Identity identity, String catalogName, String propertyName);
}
