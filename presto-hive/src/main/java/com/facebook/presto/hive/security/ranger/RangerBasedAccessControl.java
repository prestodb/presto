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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.client.JsonResponse;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Credentials;
import okhttp3.HttpUrl;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.jetbrains.annotations.TestOnly;

import javax.inject.Inject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.hive.security.ranger.RangerBasedAccessControlConfig.RANGER_REST_POLICY_MGR_DOWNLOAD_URL;
import static com.facebook.presto.hive.security.ranger.RangerBasedAccessControlConfig.RANGER_REST_USER_GROUP_URL;
import static com.facebook.presto.spi.security.AccessDeniedException.denyAddColumn;
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
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Connector access control which uses existing Ranger policies for authorizations
 */

public class RangerBasedAccessControl
        implements ConnectorAccessControl
{
    private static final Logger log = Logger.get(RangerBasedAccessControl.class);

    private RangerAuthorizer rangerAuthorizer;
    private Users users;

    @TestOnly
    public RangerBasedAccessControl()
    {
    }

    @Inject
    public RangerBasedAccessControl(RangerBasedAccessControlConfig config)
    {
        requireNonNull(config.getRangerHttpEndPoint(), "Ranger service http end point is null");
        requireNonNull(config.getRangerHiveServiceName(), "Ranger hive service name is null");

        OkHttpClient httpClient = null;
        ServicePolicies servicePolicies;
        try {
            OkHttpClient client = getAuthHttpClient(config.getBasicAuthUser(), config.getBasicAuthPassword());
            HttpUrl hiveServicePolicyUrl = requireNonNull(HttpUrl.get(URI.create(config.getRangerHttpEndPoint())))
                    .newBuilder()
                    .encodedPath(RANGER_REST_POLICY_MGR_DOWNLOAD_URL + "/" + config.getRangerHiveServiceName()).build();

            HttpUrl getUsersUrl = HttpUrl.get(URI.create(config.getRangerHttpEndPoint()))
                    .newBuilder()
                    .encodedPath(RANGER_REST_USER_GROUP_URL)
                    .build();

            servicePolicies = getHiveServicePolicies(client, hiveServicePolicyUrl);
            users = getUsers(client, getUsersUrl);
            rangerAuthorizer = new RangerAuthorizer(servicePolicies);
        }
        catch (Exception e) {
            log.error("Exception while querying ranger service " + e);
            throw new AccessDeniedException("Exception while querying ranger service ");
        }
    }

    private ServicePolicies getHiveServicePolicies(OkHttpClient client, HttpUrl httpUrl)
            throws IOException
    {
        Response response = doRequest(client, httpUrl);
        if (!response.isSuccessful()) {
            throw new RuntimeException(format("Request to %s failed: [Error: %s]", httpUrl, response));
        }

        return jsonParse(response, ServicePolicies.class);
    }

    private Users getUsers(OkHttpClient client, HttpUrl endPtUri)
    {
        Request request = new Request.Builder().url(endPtUri).header("Accept", "application/json").build();

        JsonCodec<Users> usersJsonCodec = jsonCodec(Users.class);
        JsonResponse<Users> users = JsonResponse.execute(usersJsonCodec, client, request);
        if (!users.hasValue()) {
            throw new RuntimeException(format("Request to %s failed: %s [Error: %s]", endPtUri, users, users.getResponseBody()));
        }

        return users.getValue();
    }

    @TestOnly
    public void setRangerAuthorizer(RangerAuthorizer rangerAuthorizer)
    {
        this.rangerAuthorizer = rangerAuthorizer;
    }

    @TestOnly
    public void setUsers(Users users)
    {
        this.users = users;
    }

    private static <T> T jsonParse(Response response, Class<T> clazz)
            throws IOException
    {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(response.body().byteStream()));
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper.readValue(bufferedReader, clazz);
    }

    private boolean userExists(String userName)
    {
        return users.getvXUsers().stream()
                .anyMatch(user -> userName.equalsIgnoreCase(user.getName()));
    }

    private Set<String> getGroupsForUser(String username)
    {
        if (userExists(username)) {
            return new HashSet<>(users.getvXUsers().stream()
                    .filter(user -> username.equalsIgnoreCase(user.getName()))
                    .findFirst()
                    .get()
                    .getGroupNameList());
        }
        return null;
    }

    enum HiveAccessType
    {
        NONE, CREATE, ALTER, DROP, INDEX, LOCK, SELECT, UPDATE, USE, ALL, ADMIN
    }

    private boolean checkAccess(ConnectorIdentity identity, SchemaTableName tableName, String column, HiveAccessType accessType)
    {
        return rangerAuthorizer.authorizeHiveResource(tableName.getSchemaName(), tableName.getTableName(), column,
                accessType.toString(), identity.getUser(), getGroupsForUser(identity.getUser()));
    }

    /**
     * Check if identity is allowed to create the specified schema in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    public void checkCanCreateSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String schemaName)
    {
    }

    /**
     * Check if identity is allowed to drop the specified schema in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    public void checkCanDropSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String schemaName)
    {
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, schemaName);
        if (!checkAccess(identity, schemaTableName, null, HiveAccessType.DROP)) {
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

        for (String schema : schemaNames) {
            if (rangerAuthorizer.authorizeHiveResource(schema, null, null, HiveAccessType.SELECT.toString(), identity.getUser(), groups)) {
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

        for (SchemaTableName table : tableNames) {
            if (rangerAuthorizer.authorizeHiveResource(table.getSchemaName(), table.getTableName(), null, HiveAccessType.SELECT.toString(), identity.getUser(), groups)) {
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
    public void checkCanSelectFromColumns(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName, Set<String> columnNames)
    {
        Set<String> deniedColumns = new HashSet<>();
        for (String column : columnNames) {
            if (!checkAccess(identity, tableName, column, HiveAccessType.SELECT)) {
                deniedColumns.add(column);
            }
        }
        if (deniedColumns.size() > 0) {
            denySelectColumns(tableName.getTableName(), columnNames, format("Access denied - User [ %s ] does not have [SELECT] " +
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
        if (!checkAccess(identity, null, null, HiveAccessType.CREATE)) {
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

    private static Response doRequest(OkHttpClient httpClient, HttpUrl anyURL)
            throws IOException
    {
        Request request = new Request.Builder().url(anyURL).header("Accept", "application/json").build();
        Response response = httpClient.newCall(request).execute();
        if (!response.isSuccessful()) {
            throw new IOException("Unexpected code " + response);
        }
        return response;
    }

    public static Interceptor basicAuth(String user, String password)
    {
        requireNonNull(user, "user is null");
        requireNonNull(password, "password is null");

        String credential = Credentials.basic(user, password);
        return chain -> chain.proceed(chain.request().newBuilder()
                .header(AUTHORIZATION, credential)
                .build());
    }

    private static OkHttpClient getAuthHttpClient(String username, String password)
    {
        OkHttpClient httpClient = new OkHttpClient.Builder().build();
        OkHttpClient.Builder builder = httpClient.newBuilder();
        builder.addInterceptor(basicAuth(username, password));
        return builder.build();
    }
}
