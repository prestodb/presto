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
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.AccessControlContext;
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
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanSetUser(Identity identity, AccessControlContext accessControlContext, Optional<Principal> principal, String userName);

    /**
     * Check if the query is unexpectedly modified using the credentials passed in the identity.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if query is modified.
     */
    void checkQueryIntegrity(Identity identity, AccessControlContext context, String query);

    /**
     * Filter the list of catalogs to those visible to the identity.
     */
    Set<String> filterCatalogs(Identity identity, AccessControlContext context, Set<String> catalogs);

    /**
     * Check whether identity is allowed to access catalog
     */
    void checkCanAccessCatalog(Identity identity, AccessControlContext context, String catalogName);

    /**
     * Check if identity is allowed to create the specified schema.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanCreateSchema(TransactionId transactionId, Identity identity, AccessControlContext context, CatalogSchemaName schemaName);

    /**
     * Check if identity is allowed to drop the specified schema.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanDropSchema(TransactionId transactionId, Identity identity, AccessControlContext context, CatalogSchemaName schemaName);

    /**
     * Check if identity is allowed to rename the specified schema.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanRenameSchema(TransactionId transactionId, Identity identity, AccessControlContext context, CatalogSchemaName schemaName, String newSchemaName);

    /**
     * Check if identity is allowed to execute SHOW SCHEMAS in a catalog.
     * <p>
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterSchemas} method must filter all results for unauthorized users,
     * since there are multiple ways to list schemas.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanShowSchemas(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName);

    /**
     * Filter the list of schemas in a catalog to those visible to the identity.
     */
    Set<String> filterSchemas(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName, Set<String> schemaNames);

    /**
     * Check if identity is allowed to create the specified table.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanCreateTable(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to drop the specified table.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanDropTable(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to rename the specified table.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanRenameTable(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName, QualifiedObjectName newTableName);

    /**
     * Check if identity is allowed to show metadata of tables by executing SHOW TABLES, SHOW GRANTS etc. in a catalog.
     * <p>
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterTables} method must filter all results for unauthorized users,
     * since there are multiple ways to list tables.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanShowTablesMetadata(TransactionId transactionId, Identity identity, AccessControlContext context, CatalogSchemaName schema);

    /**
     * Filter the list of tables and views to those visible to the identity.
     */
    Set<SchemaTableName> filterTables(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName, Set<SchemaTableName> tableNames);

    /**
     * Check if identity is allowed to add columns to the specified table.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanAddColumns(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to drop columns from the specified table.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanDropColumn(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to rename a column in the specified table.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanRenameColumn(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to insert into the specified table.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanInsertIntoTable(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to delete from the specified table.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanDeleteFromTable(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to create the specified view.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanCreateView(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName viewName);

    /**
     * Check if identity is allowed to drop the specified view.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanDropView(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName viewName);

    /**
     * Check if identity is allowed to create a view that selects from the specified columns.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanCreateViewWithSelectFromColumns(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName, Set<String> columnNames);

    /**
     * Check if identity is allowed to grant a privilege to the grantee on the specified table.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanGrantTablePrivilege(TransactionId transactionId, Identity identity, AccessControlContext context, Privilege privilege, QualifiedObjectName tableName, PrestoPrincipal grantee, boolean withGrantOption);

    /**
     * Check if identity is allowed to revoke a privilege from the revokee on the specified table.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanRevokeTablePrivilege(TransactionId transactionId, Identity identity, AccessControlContext context, Privilege privilege, QualifiedObjectName tableName, PrestoPrincipal revokee, boolean grantOptionFor);

    /**
     * Check if identity is allowed to set the specified system property.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanSetSystemSessionProperty(Identity identity, AccessControlContext context, String propertyName);

    /**
     * Check if identity is allowed to set the specified catalog property.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanSetCatalogSessionProperty(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName, String propertyName);

    /**
     * Check if identity is allowed to select from the specified columns.  The column set can be empty.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanSelectFromColumns(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName, Set<String> columnNames);

    /**
     * Check if identity is allowed to create the specified role.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanCreateRole(TransactionId transactionId, Identity identity, AccessControlContext context, String role, Optional<PrestoPrincipal> grantor, String catalogName);

    /**
     * Check if identity is allowed to drop the specified role.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanDropRole(TransactionId transactionId, Identity identity, AccessControlContext context, String role, String catalogName);

    /**
     * Check if identity is allowed to grant the specified roles to the specified principals.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanGrantRoles(TransactionId transactionId, Identity identity, AccessControlContext context, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor, String catalogName);

    /**
     * Check if identity is allowed to revoke the specified roles from the specified principals.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanRevokeRoles(TransactionId transactionId, Identity identity, AccessControlContext context, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor, String catalogName);

    /**
     * Check if identity is allowed to set role for specified catalog.
     *
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanSetRole(TransactionId requiredTransactionId, Identity identity, AccessControlContext context, String role, String catalog);

    /**
     * Check if identity is allowed to show roles on the specified catalog.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanShowRoles(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName);

    /**
     * Check if identity is allowed to show current roles on the specified catalog.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanShowCurrentRoles(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName);

    /**
     * Check if identity is allowed to show its own role grants on the specified catalog.
     * @throws com.facebook.presto.spi.security.AccessDeniedException if not allowed
     */
    void checkCanShowRoleGrants(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName);
}
