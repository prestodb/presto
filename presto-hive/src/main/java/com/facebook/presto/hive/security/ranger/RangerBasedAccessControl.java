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
package com.facebook.presto.hive.security.ranger;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_RANGER_SERVER_ERROR;
import static com.facebook.presto.hive.security.ranger.RangerBasedAccessControlConfig.RANGER_REST_POLICY_MGR_DOWNLOAD_URL;
import static com.facebook.presto.hive.security.ranger.RangerBasedAccessControlConfig.RANGER_REST_USER_GROUP_URL;
import static com.facebook.presto.hive.security.ranger.RangerBasedAccessControlConfig.RANGER_REST_USER_ROLES_URL;
import static com.facebook.presto.spi.security.AccessDeniedException.denyAddColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateSchema;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDeleteTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropSchema;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyInsertTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectColumns;
import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Connector access control which uses existing Ranger policies for authorizations
 */

public class RangerBasedAccessControl
        implements ConnectorAccessControl
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final JsonCodec<Users> USER_INFO_CODEC = jsonCodec(Users.class);
    private static final JsonCodec<List<String>> ROLES_INFO_CODEC = listJsonCodec(String.class);

    private final RangerAuthorizer rangerAuthorizer;
    private final Supplier<Map<String, Set<String>>> userRolesMapping;
    private final Supplier<Map<String, Set<String>>> userGroupsMapping;
    private final Supplier<ServicePolicies> servicePolicies;
    private final HttpClient httpClient;

    @Inject
    public RangerBasedAccessControl(RangerBasedAccessControlConfig config, @ForRangerInfo HttpClient httpClient)
    {
        requireNonNull(config, "config is null");
        requireNonNull(config.getRangerHttpEndPoint(), "Ranger service http end point is null");
        requireNonNull(config.getRangerHiveServiceName(), "Ranger hive service name is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");

        try {
            servicePolicies = memoizeWithExpiration(
                    () -> getHiveServicePolicies(config),
                    config.getRefreshPeriod().toMillis(),
                    MILLISECONDS);

            userGroupsMapping = memoizeWithExpiration(
                    () -> getUserGroupsMappings(config),
                    config.getRefreshPeriod().toMillis(),
                    MILLISECONDS);

            userRolesMapping = memoizeWithExpiration(
                    () -> getRolesForUserList(config),
                    config.getRefreshPeriod().toMillis(),
                    MILLISECONDS);

            rangerAuthorizer = new RangerAuthorizer(servicePolicies, config);
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to query ranger service ", e);
        }
    }

    private ServicePolicies getHiveServicePolicies(RangerBasedAccessControlConfig config)
    {
        URI uri = uriBuilderFrom(URI.create(config.getRangerHttpEndPoint()))
                .appendPath(RANGER_REST_POLICY_MGR_DOWNLOAD_URL + "/" + config.getRangerHiveServiceName())
                .build();
        Request request = setContentTypeHeaders(prepareGet())
                .setUri(uri)
                .build();
        try {
            return OBJECT_MAPPER.readValue(httpClient.execute(request, createStringResponseHandler()).getBody(), ServicePolicies.class);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_RANGER_SERVER_ERROR, format("Unable to fetch policies from %s hive service end point", config.getRangerHiveServiceName()));
        }
    }

    private Users getUsers(RangerBasedAccessControlConfig config)
    {
        URI uri = uriBuilderFrom(URI.create(config.getRangerHttpEndPoint()))
                .appendPath(RANGER_REST_USER_GROUP_URL)
                .build();
        Request request = setContentTypeHeaders(prepareGet())
                .setUri(uri)
                .build();

        return httpClient.execute(request, createJsonResponseHandler(USER_INFO_CODEC));
    }

    private static Request.Builder setContentTypeHeaders(Request.Builder requestBuilder)
    {
        return requestBuilder
                .setHeader("Accept", "application/json");
    }

    private Map<String, Set<String>> getRolesForUserList(RangerBasedAccessControlConfig config)
    {
        Users users = getUsers(config);
        ImmutableMap.Builder<String, Set<String>> userRolesMapping = ImmutableMap.builder();
        for (VXUser vxUser : users.getvXUsers()) {
            userRolesMapping.put(vxUser.getName(), getRolesForUser(vxUser.getName(), config));
        }
        return userRolesMapping.build();
    }

    private Set<String> getRolesForUser(String userName, RangerBasedAccessControlConfig config)
    {
        URI uri = uriBuilderFrom(URI.create(config.getRangerHttpEndPoint()))
                .appendPath(RANGER_REST_USER_ROLES_URL + "/" + userName)
                .build();
        Request request = setContentTypeHeaders(prepareGet())
                .setUri(uri)
                .build();

        return ImmutableSet.copyOf(httpClient.execute(request, createJsonResponseHandler(ROLES_INFO_CODEC)));
    }

    private Map<String, Set<String>> getUserGroupsMappings(RangerBasedAccessControlConfig config)
    {
        Users users = getUsers(config);
        ImmutableMap.Builder<String, Set<String>> userGroupsMapping = ImmutableMap.builder();
        for (VXUser vxUser : users.getvXUsers()) {
            if (!(isNull(vxUser.getGroupNameList()) || vxUser.getGroupNameList().isEmpty())) {
                userGroupsMapping.put(vxUser.getName(), ImmutableSet.copyOf(vxUser.getGroupNameList()));
            }
        }
        return userGroupsMapping.build();
    }

    private Set<String> getGroupsForUser(String username)
    {
        try {
            return userGroupsMapping.get().get(username);
        }
        catch (Exception ex) {
            throw new PrestoException(HIVE_RANGER_SERVER_ERROR, "Unable to fetch user groups information from ranger", ex);
        }
    }

    private Set<String> getRolesForUser(String username)
    {
        try {
            return userRolesMapping.get().get(username);
        }
        catch (Exception ex) {
            throw new PrestoException(HIVE_RANGER_SERVER_ERROR, "Unable to fetch user roles information from ranger", ex);
        }
    }

    enum HiveAccessType
    {
        NONE, CREATE, ALTER, DROP, INDEX, LOCK, SELECT, UPDATE, USE, ALL, ADMIN
    }

    private boolean checkAccess(ConnectorIdentity identity, SchemaTableName tableName, String column, HiveAccessType accessType)
    {
        return rangerAuthorizer.authorizeHiveResource(tableName.getSchemaName(), tableName.getTableName(), column,
                accessType.toString(), identity.getUser(), getGroupsForUser(identity.getUser()), getRolesForUser(identity.getUser()));
    }

    /**
     * Check if identity is allowed to create the specified schema in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    public void checkCanCreateSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String schemaName)
    {
        if (!rangerAuthorizer.authorizeHiveResource(schemaName, null, null,
                HiveAccessType.CREATE.toString(), identity.getUser(), getGroupsForUser(identity.getUser()), getRolesForUser(identity.getUser()))) {
            denyCreateSchema(schemaName, format("Access denied - User [ %s ] does not have [CREATE] " +
                    "privilege on [ %s ] ", identity.getUser(), schemaName));
        }
    }

    /**
     * Check if identity is allowed to drop the specified schema in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    public void checkCanDropSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String schemaName)
    {
        if (!rangerAuthorizer.authorizeHiveResource(schemaName, null, null,
                HiveAccessType.DROP.toString(), identity.getUser(), getGroupsForUser(identity.getUser()), getRolesForUser(identity.getUser()))) {
            denyDropSchema(schemaName, format("Access denied - User [ %s ] does not have [DROP] " +
                    "privilege on [ %s ] ", identity.getUser(), schemaName));
        }
    }

    /**
     * Check if identity is allowed to execute SHOW SCHEMAS in a catalog.
     * <p>
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterSchemas} method must handle filter all results for unauthorized users,
     * since there are multiple way to list schemas.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanShowSchemas(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context)
    {
    }

    /**
     * Filter the list of schemas to those visible to the identity.
     */
    @Override
    public Set<String> filterSchemas(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, Set<String> schemaNames)
    {
        Set<String> allowedSchemas = new HashSet<>();
        Set<String> groups = getGroupsForUser(identity.getUser());
        Set<String> roles = getRolesForUser(identity.getUser());

        for (String schema : schemaNames) {
            if (rangerAuthorizer.authorizeHiveResource(schema, null, null, RangerPolicyEngine.ANY_ACCESS, identity.getUser(), groups, roles)) {
                allowedSchemas.add(schema);
            }
        }
        return allowedSchemas;
    }

    /**
     * Check if identity is allowed to create the specified table in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanCreateTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        if (!checkAccess(identity, tableName, null, HiveAccessType.CREATE)) {
            denyCreateTable(tableName.getTableName(), format("Access denied - User [ %s ] does not have [CREATE] " +
                    "privilege on [ %s ] ", identity.getUser(), tableName.getSchemaName()));
        }
    }

    /**
     * Filter the list of tables and views to those visible to the identity.
     */
    @Override
    public Set<SchemaTableName> filterTables(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, Set<SchemaTableName> tableNames)
    {
        Set<SchemaTableName> allowedTables = new HashSet<>();
        Set<String> groups = getGroupsForUser(identity.getUser());
        Set<String> roles = getRolesForUser(identity.getUser());

        for (SchemaTableName table : tableNames) {
            if (rangerAuthorizer.authorizeHiveResource(table.getSchemaName(), table.getTableName(), null, RangerPolicyEngine.ANY_ACCESS, identity.getUser(), groups, roles)) {
                allowedTables.add(table);
            }
        }
        return allowedTables;
    }

    /**
     * Check if identity is allowed to add columns to the specified table in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanAddColumn(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        if (!checkAccess(identity, tableName, null, HiveAccessType.ALTER)) {
            denyAddColumn(tableName.getTableName(), format("Access denied - User [ %s ] does not have [ALTER] " +
                    "privilege on [ %s/%s ] ", identity.getUser(), tableName.getSchemaName(), tableName.getTableName()));
        }
    }

    /**
     * Check if identity is allowed to drop columns from the specified table in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanDropColumn(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        if (!checkAccess(identity, tableName, null, HiveAccessType.ALTER)) {
            denyDropColumn(tableName.getTableName(), format("Access denied - User [ %s ] does not have [ALTER] " +
                    "privilege on [ %s/%s ] ", identity.getUser(), tableName.getSchemaName(), tableName.getTableName()));
        }
    }

    /**
     * Check if identity is allowed to rename a column in the specified table in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanRenameColumn(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        if (!checkAccess(identity, tableName, null, HiveAccessType.ALTER)) {
            denyRenameColumn(tableName.getTableName(), format("Access denied - User [ %s ] does not have [ALTER] " +
                    "privilege on [ %s/%s ] ", identity.getUser(), tableName.getSchemaName(), tableName.getTableName()));
        }
    }

    /**
     * Check if identity is allowed to select from the specified columns in a relation.  The column set can be empty.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanSelectFromColumns(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName, Set<Subfield> columnOrSubfieldNames)
    {
        Set<String> deniedColumns = new HashSet<>();
        for (String column : columnOrSubfieldNames.stream().map(column -> column.getRootName()).collect(toImmutableSet())) {
            if (!checkAccess(identity, tableName, column, HiveAccessType.SELECT)) {
                deniedColumns.add(column);
            }
        }
        if (deniedColumns.size() > 0) {
            denySelectColumns(tableName.getTableName(), columnOrSubfieldNames.stream().map(column -> column.getRootName()).collect(toImmutableSet()), format("Access denied - User [ %s ] does not have [SELECT] " +
                    "privilege on all mentioned columns of [ %s/%s ] ", identity.getUser(), tableName.getSchemaName(), tableName.getTableName()));
        }
    }

    /**
     * Check if identity is allowed to drop the specified table in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanDropTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        if (!checkAccess(identity, tableName, null, HiveAccessType.DROP)) {
            denyDropTable(tableName.getTableName(), format("Access denied - User [ %s ] does not have [DROP] " +
                    "privilege on [ %s/%s ] ", identity.getUser(), tableName.getSchemaName(), tableName.getTableName()));
        }
    }

    /**
     * Check if identity is allowed to rename the specified table in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanRenameTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName, SchemaTableName newTableName)
    {
        if (!checkAccess(identity, tableName, null, HiveAccessType.ALTER)) {
            denyRenameTable(tableName.getTableName(), format("Access denied - User [ %s ] does not have [ALTER] " +
                    "privilege on [ %s/%s ] ", identity.getUser(), tableName.getSchemaName(), tableName.getTableName()));
        }
    }

    /**
     * Check if identity is allowed to show metadata of tables by executing SHOW TABLES, SHOW GRANTS etc. in a catalog.
     * <p>
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterTables} method must filter all results for unauthorized users,
     * since there are multiple ways to list tables.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanShowTablesMetadata(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String schemaName)
    {
    }

    /**
     * Check if identity is allowed to insert into the specified table in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanInsertIntoTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        if (!checkAccess(identity, tableName, null, HiveAccessType.UPDATE)) {
            denyInsertTable(tableName.getTableName(), format("Access denied - User [ %s ] does not have [UPDATE] " +
                    "privilege on [ %s/%s ] ", identity.getUser(), tableName.getSchemaName(), tableName.getTableName()));
        }
    }

    /**
     * Check if identity is allowed to delete from the specified table in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanDeleteFromTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        if (!checkAccess(identity, tableName, null, HiveAccessType.UPDATE)) {
            denyDeleteTable(tableName.getTableName(), format("Access denied - User [ %s ] does not have [UPDATE] " +
                    "privilege on [ %s/%s ] ", identity.getUser(), tableName.getSchemaName(), tableName.getTableName()));
        }
    }

    /**
     * Check if identity is allowed to create the specified view in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanCreateView(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName viewName)
    {
        if (!checkAccess(identity, viewName, null, HiveAccessType.CREATE)) {
            denyCreateView(viewName.getTableName(), format("Access denied - User [ %s ] does not have [CREATE] " +
                    "privilege on [ %s/%s ] ", identity.getUser(), viewName.getSchemaName(), viewName.getTableName()));
        }
    }

    /**
     * Check if identity is allowed to drop the specified view in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanDropView(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName viewName)
    {
        if (!checkAccess(identity, viewName, null, HiveAccessType.DROP)) {
            denyDropView(viewName.getTableName(), format("Access denied - User [ %s ] does not have [DROP] " +
                    "privilege on [ %s/%s ] ", identity.getUser(), viewName.getSchemaName(), viewName.getTableName()));
        }
    }

    /**
     * Check if identity is allowed to create a view that selects from the specified columns in a relation.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanCreateViewWithSelectFromColumns(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName, Set<String> columnNames)
    {
        if (!checkAccess(identity, tableName, null, HiveAccessType.CREATE)) {
            denyCreateView(tableName.getTableName(), format("Access denied - User [ %s ] does not have [CREATE] " +
                    "privilege on [ %s/%s ] ", identity.getUser(), tableName.getSchemaName(), tableName.getTableName()));
        }

        Set<String> deniedColumns = new HashSet<>();
        for (String column : columnNames) {
            if (!checkAccess(identity, tableName, column, HiveAccessType.SELECT)) {
                deniedColumns.add(column);
            }
        }
        if (deniedColumns.size() > 0) {
            denyCreateViewWithSelect(tableName.getTableName(), identity);
        }
    }

    /**
     * Check if identity is allowed to set the specified property in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanSetCatalogSessionProperty(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String propertyName)
    {
    }
}
