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
import com.facebook.airlift.http.client.HttpStatus;
import com.facebook.airlift.http.client.testing.TestingHttpClient;
import com.facebook.airlift.http.client.testing.TestingResponse;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.PrincipalType;
import com.facebook.presto.spi.security.Privilege;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Optional;

import static com.facebook.presto.hive.security.ranger.RangerBasedAccessControlConfig.RANGER_REST_POLICY_MGR_DOWNLOAD_URL;
import static com.facebook.presto.hive.security.ranger.RangerBasedAccessControlConfig.RANGER_REST_USER_GROUP_URL;
import static com.facebook.presto.hive.security.ranger.RangerBasedAccessControlConfig.RANGER_REST_USER_ROLES_URL;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertThrows;

public class TestRangerBasedAccessControl
{
    public static final ConnectorTransactionHandle TRANSACTION_HANDLE = new ConnectorTransactionHandle() {};
    public static final AccessControlContext CONTEXT = new AccessControlContext(new QueryId("query_id"), Optional.empty(), Optional.empty());

    @Test
    public void testTablePriviledgesRolesNotAllowed()
    {
        ConnectorAccessControl accessControl = createRangerAccessControl("default-allow-all.json", "user_groups.json");
        assertDenied(() -> accessControl.checkCanRevokeTablePrivilege(TRANSACTION_HANDLE, user("anyuser"), CONTEXT, Privilege.SELECT,
                new SchemaTableName("foodmart", "test"), new PrestoPrincipal(PrincipalType.ROLE, "role"), true));
        assertDenied(() -> accessControl.checkCanGrantTablePrivilege(TRANSACTION_HANDLE, user("anyuser"), CONTEXT, Privilege.SELECT,
                new SchemaTableName("foodmart", "test"), new PrestoPrincipal(PrincipalType.ROLE, "role"), true));
        assertDenied(() -> accessControl.checkCanCreateRole(TRANSACTION_HANDLE, user("anyuser"), CONTEXT, "schemaName", Optional.empty()));
        assertDenied(() -> accessControl.checkCanDropRole(TRANSACTION_HANDLE, user("anyuser"), CONTEXT, "schemaName"));
        assertDenied(() -> accessControl.checkCanGrantRoles(TRANSACTION_HANDLE, user("anyuser"), CONTEXT, ImmutableSet.of(""),
                ImmutableSet.of(new PrestoPrincipal(PrincipalType.ROLE, "role")), true, Optional.empty(), ""));
        assertDenied(() -> accessControl.checkCanSetRole(TRANSACTION_HANDLE, user("anyuser"), CONTEXT, "schemaName", ""));
    }

    @Test
    public void testDefaultAccessAllowedNotChecked()
    {
        ConnectorAccessControl accessControl = createRangerAccessControl("default-allow-all.json", "user_groups.json");
        accessControl.checkCanShowTablesMetadata(TRANSACTION_HANDLE, user("anyuser"), CONTEXT, "schemaName");
        accessControl.checkCanSetCatalogSessionProperty(TRANSACTION_HANDLE, user("anyuser"), CONTEXT, "schemaName");
        accessControl.checkCanCreateSchema(TRANSACTION_HANDLE, user("anyuser"), CONTEXT, "schemaName");
        accessControl.checkCanShowSchemas(TRANSACTION_HANDLE, user("anyuser"), CONTEXT);
    }

    @Test
    public void testDefaultTableAccessIfNotDefined()
    {
        ConnectorAccessControl accessControl = createRangerAccessControl("default-allow-all.json", "user_groups.json");
        accessControl.checkCanCreateTable(TRANSACTION_HANDLE, user("admin"), CONTEXT, new SchemaTableName("test", "test"));
        accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("alice"), CONTEXT, new SchemaTableName("test", "test"), ImmutableSet.of());
        accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("bobschema", "bobtable"), ImmutableSet.of());
        accessControl.checkCanRenameTable(TRANSACTION_HANDLE, user("admin"), CONTEXT, new SchemaTableName("test", "test"), new SchemaTableName("test1", "test1"));
        accessControl.checkCanInsertIntoTable(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("bobschema", "bobtable"));
        accessControl.checkCanDeleteFromTable(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("bobschema", "bobtable"));
        accessControl.checkCanCreateViewWithSelectFromColumns(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("bobschema", "bobtable"), ImmutableSet.of());
    }

    @Test
    public void testTableOperations()
    {
        ConnectorAccessControl accessControl = createRangerAccessControl("default-schema-level-access.json", "user_groups.json");
        // 'etladmin' group have all access {group - etladmin, user - alice}
        accessControl.checkCanCreateTable(TRANSACTION_HANDLE, user("alice"), CONTEXT, new SchemaTableName("foodmart", "test"));
        accessControl.checkCanRenameTable(TRANSACTION_HANDLE, user("alice"), CONTEXT, new SchemaTableName("foodmart", "test"), new SchemaTableName("foodmart", "test1"));
        accessControl.checkCanDropTable(TRANSACTION_HANDLE, user("alice"), CONTEXT, new SchemaTableName("foodmart", "test"));
        accessControl.checkCanDropSchema(TRANSACTION_HANDLE, user("alice"), CONTEXT, "foodmart");
        accessControl.checkCanAddColumn(TRANSACTION_HANDLE, user("alice"), CONTEXT, new SchemaTableName("foodmart", "test"));
        accessControl.checkCanDropColumn(TRANSACTION_HANDLE, user("alice"), CONTEXT, new SchemaTableName("foodmart", "test"));
        accessControl.checkCanRenameColumn(TRANSACTION_HANDLE, user("alice"), CONTEXT, new SchemaTableName("foodmart", "test"));

        // 'analyst' group have all but drop access {group - analyst, user - joe}
        accessControl.checkCanCreateTable(TRANSACTION_HANDLE, user("joe"), CONTEXT, new SchemaTableName("foodmart", "test"));
        assertDenied(() -> accessControl.checkCanRenameTable(TRANSACTION_HANDLE, user("joe"), CONTEXT, new SchemaTableName("foodmart", "test"), new SchemaTableName("foodmart", "test1")));
        assertDenied(() -> accessControl.checkCanDropTable(TRANSACTION_HANDLE, user("joe"), CONTEXT, new SchemaTableName("foodmart", "test")));
        assertDenied(() -> accessControl.checkCanDropSchema(TRANSACTION_HANDLE, user("joe"), CONTEXT, "foodmart"));
        assertDenied(() -> accessControl.checkCanAddColumn(TRANSACTION_HANDLE, user("joe"), CONTEXT, new SchemaTableName("foodmart", "test")));
        assertDenied(() -> accessControl.checkCanDropColumn(TRANSACTION_HANDLE, user("joe"), CONTEXT, new SchemaTableName("foodmart", "test")));
        assertDenied(() -> accessControl.checkCanRenameColumn(TRANSACTION_HANDLE, user("joe"), CONTEXT, new SchemaTableName("foodmart", "test")));

        //  Access denied to others {group - readall, user - bob}
        assertDenied(() -> accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("foodmart", "test"), ImmutableSet.of(new Subfield("column1"))));
        assertDenied(() -> accessControl.checkCanCreateTable(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("foodmart", "test")));
        assertDenied(() -> accessControl.checkCanRenameTable(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("foodmart", "test"), new SchemaTableName("foodmart", "test1")));
        assertDenied(() -> accessControl.checkCanDropTable(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("foodmart", "test")));
        assertDenied(() -> accessControl.checkCanAddColumn(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("foodmart", "test")));
        assertDenied(() -> accessControl.checkCanDropColumn(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("foodmart", "test")));
        assertDenied(() -> accessControl.checkCanRenameColumn(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("foodmart", "test")));
    }

    @Test
    public void testSelectUpdateAccess()
    {
        ConnectorAccessControl accessControl = createRangerAccessControl("default-table-select-update.json", "user_groups.json");
        // 'etladmin' group have all access {group - etladmin, user - alice}
        accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("alice"), CONTEXT, new SchemaTableName("foodmart", "test"), ImmutableSet.of(new Subfield("column1")));
        accessControl.checkCanInsertIntoTable(TRANSACTION_HANDLE, user("alice"), CONTEXT, new SchemaTableName("foodmart", "test"));

        // 'analyst' group have SELECT, UPDATE {group - analyst, user - joe}
        accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("joe"), CONTEXT, new SchemaTableName("foodmart", "test"), ImmutableSet.of(new Subfield("column1")));
        accessControl.checkCanInsertIntoTable(TRANSACTION_HANDLE, user("joe"), CONTEXT, new SchemaTableName("foodmart", "test"));

        //  Access denied to others {group - readall, user - bob}
        accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("foodmart", "test"), ImmutableSet.of(new Subfield("column1")));
        assertDenied(() -> accessControl.checkCanInsertIntoTable(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("foodmart", "test")));
    }

    @Test
    public void testColumnLevelAccess()
    {
        ConnectorAccessControl accessControl = createRangerAccessControl("default-table-column-access.json", "user_groups.json");
        // 'analyst' group have read access {group - analyst, user - joe}
        accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("joe"), CONTEXT, new SchemaTableName("foodmart", "salary"), ImmutableSet.of(new Subfield("salary_paid"), new Subfield("overtime_paid")));

        //  Access denied to others {group - readall, user - bob}
        assertDenied(() -> accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("foodmart", "salary"), ImmutableSet.of(new Subfield("currency_id"), new Subfield("overtime_paid"))));
    }

    @Test
    public void testRoleBasedAccess()
    {
        ConnectorAccessControl accessControl = createRangerAccessControl("ranger-role-based-access.json", "user_groups.json");

        // 'admin_role' role have all access {user - raj, group - admin, role - admin_role}
        accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("raj"), CONTEXT, new SchemaTableName("default", "customer"), ImmutableSet.of(new Subfield("column1")));
        accessControl.checkCanInsertIntoTable(TRANSACTION_HANDLE, user("raj"), CONTEXT, new SchemaTableName("default", "customer"));
        accessControl.checkCanDropTable(TRANSACTION_HANDLE, user("raj"), CONTEXT, new SchemaTableName("default", "customer"));

        // 'etl_role' role have all access {user - maria, group - etldev, role - etl_role}
        accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("maria"), CONTEXT, new SchemaTableName("default", "orders"), ImmutableSet.of(new Subfield("column1")));
        accessControl.checkCanInsertIntoTable(TRANSACTION_HANDLE, user("maria"), CONTEXT, new SchemaTableName("default", "orders"));
        accessControl.checkCanDropTable(TRANSACTION_HANDLE, user("maria"), CONTEXT, new SchemaTableName("default", "orders"));
        //  Access denied to columns other than name & comment {user - maria, group - etldev, role - etl_role}
        assertDenied(() -> accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("maria"), CONTEXT, new SchemaTableName("default", "customer"), ImmutableSet.of(new Subfield("column1"))));

        // 'analyst_role' role have all access {user - sam, group - analyst, role - analyst_role}
        accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("sam"), CONTEXT, new SchemaTableName("default", "lineitem"), ImmutableSet.of(new Subfield("column1")));
        accessControl.checkCanInsertIntoTable(TRANSACTION_HANDLE, user("sam"), CONTEXT, new SchemaTableName("default", "lineitem"));
        accessControl.checkCanDropTable(TRANSACTION_HANDLE, user("sam"), CONTEXT, new SchemaTableName("default", "lineitem"));
        //  Access denied {user - sam, group - analyst, role - analyst_role}
        assertDenied(() -> accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("sam"), CONTEXT, new SchemaTableName("default", "customer"), ImmutableSet.of(new Subfield("column1"))));
        assertDenied(() -> accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("sam"), CONTEXT, new SchemaTableName("default", "supplier"), ImmutableSet.of(new Subfield("column1"))));
    }

    private static ConnectorIdentity user(String name)
    {
        return new ConnectorIdentity(name, Optional.empty(), Optional.empty());
    }

    private ConnectorAccessControl createRangerAccessControl(String policyFile, String usersFile)
    {
        String policyFilePath = "com.facebook.presto.hive.security.ranger/" + policyFile;
        String usersFilePath = "com.facebook.presto.hive.security.ranger/" + usersFile;

        HttpClient httpClient = new TestingHttpClient(httpRequest -> {
            String uriPath = httpRequest.getUri().getPath();
            if (uriPath.contains(RANGER_REST_POLICY_MGR_DOWNLOAD_URL)) {
                return makeHttpResponse(ByteStreams.toByteArray(this.getClass().getClassLoader().getResourceAsStream(policyFilePath)));
            }
            else if (uriPath.contains(RANGER_REST_USER_GROUP_URL)) {
                return makeHttpResponse(ByteStreams.toByteArray(this.getClass().getClassLoader().getResourceAsStream(usersFilePath)));
            }
            else if (uriPath.contains(RANGER_REST_USER_ROLES_URL)) {
                if (uriPath.contains("raj")) {
                    return makeHttpResponse("[\"admin_role\"]".getBytes(UTF_8));
                }
                else if (uriPath.contains("maria")) {
                    return makeHttpResponse("[\"etl_role\"]".getBytes(UTF_8));
                }
                else if (uriPath.contains("sam")) {
                    return makeHttpResponse("[\"analyst_role\"]".getBytes(UTF_8));
                }
                else {
                    return makeHttpResponse("[\"dev_role\"]".getBytes(UTF_8));
                }
            }
            throw new IllegalStateException("Testing client is not configured correctly");
        });

        RangerBasedAccessControlConfig config = new RangerBasedAccessControlConfig()
                .setRangerHttpEndPoint("http://test")
                .setRangerHiveServiceName("dummy");
        RangerBasedAccessControl rangerBasedAccessControl = new RangerBasedAccessControl(config, httpClient);
        return rangerBasedAccessControl;
    }

    private TestingResponse makeHttpResponse(byte[] answerJson)
    {
        ImmutableListMultimap.Builder<String, String> headers = ImmutableListMultimap.builder();
        headers.put("Content-Type", "application/json");
        return new TestingResponse(HttpStatus.OK, headers.build(), answerJson);
    }

    private static <T> T jsonParse(File file, Class<T> clazz)
    {
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            return mapper.readValue(bufferedReader, clazz);
        }
        catch (IOException e) {
            throw new IllegalArgumentException(format("Invalid JSON file '%s'", file.getPath()), e);
        }
    }

    private static void assertDenied(ThrowingRunnable runnable)
    {
        assertThrows(AccessDeniedException.class, runnable);
    }
}
