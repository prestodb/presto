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

import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.spi.CatalogSchemaName;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.GrantInfo;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.transaction.TransactionId;

import java.security.Principal;
import java.util.Optional;
import java.util.Set;

public interface AccessControl
{
    /**
     * Check if the principal is allowed to be the specified user.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanSetUser(Principal principal, String userName);

    /**
     * Filter the list of catalogs to those visible to the identity.
     */
    Set<String> filterCatalogs(Identity identity, Set<String> catalogs);

    /**
     * Check if identity is allowed to create the specified schema.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanCreateSchema(TransactionId transactionId, Identity identity, CatalogSchemaName schemaName);

    /**
     * Check if identity is allowed to drop the specified schema.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanDropSchema(TransactionId transactionId, Identity identity, CatalogSchemaName schemaName);

    /**
     * Check if identity is allowed to rename the specified schema.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanRenameSchema(TransactionId transactionId, Identity identity, CatalogSchemaName schemaName, String newSchemaName);

    /**
     * Check if identity is allowed to execute SHOW SCHEMAS in a catalog.
     *
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterSchemas} method must filter all results for unauthorized users,
     * since there are multiple ways to list schemas.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanShowSchemas(TransactionId transactionId, Identity identity, String catalogName);

    /**
     * Filter the list of schemas in a catalog to those visible to the identity.
     */
    Set<String> filterSchemas(TransactionId transactionId, Identity identity, String catalogName, Set<String> schemaNames);

    /**
     * Check if identity is allowed to create the specified table.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanCreateTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to drop the specified table.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanDropTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to rename the specified table.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanRenameTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName, QualifiedObjectName newTableName);

    /**
     * Check if identity is allowed to execute SHOW TABLES in a catalog.
     *
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterTables} method must filter all results for unauthorized users,
     * since there are multiple ways to list tables.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanShowTables(TransactionId transactionId, Identity identity, CatalogSchemaName schema);

    /**
     * Filter the list of tables and views to those visible to the identity.
     */
    Set<SchemaTableName> filterTables(TransactionId transactionId, Identity identity, String catalogName, Set<SchemaTableName> tableNames);

    /**
     * Check if identity is allowed to add columns to the specified table.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanAddColumns(TransactionId transactionId, Identity identity, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to rename a column in the specified table.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanRenameColumn(TransactionId transactionId, Identity identity, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to select from the specified table.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanSelectFromTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to insert into the specified table.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanInsertIntoTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to delete from the specified table.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanDeleteFromTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to create the specified view.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanCreateView(TransactionId transactionId, Identity identity, QualifiedObjectName viewName);

    /**
     * Check if identity is allowed to drop the specified view.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanDropView(TransactionId transactionId, Identity identity, QualifiedObjectName viewName);

    /**
     * Check if identity is allowed to select from the specified view.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanSelectFromView(TransactionId transactionId, Identity identity, QualifiedObjectName viewName);

    /**
     * Check if identity is allowed to create a view that selects from the specified table.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanCreateViewWithSelectFromTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to create a view that selects from the specified view.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanCreateViewWithSelectFromView(TransactionId transactionId, Identity identity, QualifiedObjectName viewName);

    /**
     * Check if identity is allowed to grant the specified privilege on the specified table.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanGrantTablePrivilege(TransactionId transactionId, Identity identity, Privilege privilege, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to revoke the specified privilege on the specified table.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanRevokeTablePrivilege(TransactionId transactionId, Identity identity, Privilege privilege, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to show grants on the specified catalog or schema or table.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanShowGrants(TransactionId transactionId, Identity identity, QualifiedTablePrefix prefix);

    /**
     * Filter the list of grants to those visible to the identity.
     */
    Set<GrantInfo> filterGrants(TransactionId transactionId, Identity identity, QualifiedTablePrefix prefix, Set<GrantInfo> privilegeInfos);

    /**
     * Check if identity is allowed to set the specified system property.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanSetSystemSessionProperty(Identity identity, String propertyName);

    /**
     * Check if identity is allowed to set the specified catalog property.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanSetCatalogSessionProperty(TransactionId transactionId, Identity identity, String catalogName, String propertyName);

    /**
     * Check if identity is allowed to create the specified role.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanCreateRole(TransactionId transactionId, Identity identity, String role, Optional<PrestoPrincipal> grantor, String catalogName);

    /**
     * Check if identity is allowed to drop the specified role.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanDropRole(TransactionId transactionId, Identity identity, String role, String catalogName);

    /**
     * Check if identity is allowed to grant the specified roles to the specified principals.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanGrantRoles(TransactionId transactionId, Identity identity, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor, String catalogName);

    /**
     * Check if identity is allowed to revoke the specified roles from the specified principals.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanRevokeRoles(TransactionId transactionId, Identity identity, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor, String catalogName);

    /**
     * Check if identity is allowed to set role for specified catalog.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanSetRole(TransactionId requiredTransactionId, Identity identity, String role, String catalog);

    /**
     * Check if identity is allowed to show roles on the specified catalog.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanShowRoles(TransactionId transactionId, Identity identity, String catalogName);

    /**
     * Filter the list of roles to those visible to the identity in the given catalog.
     */
    Set<String> filterRoles(TransactionId transactionId, Identity identity, String catalogName, Set<String> roles);
}
