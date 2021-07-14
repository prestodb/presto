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
import com.google.common.collect.ImmutableSet;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.testng.Assert.assertThrows;

public class TestRangerBasedAccessControl
{
    public static final ConnectorTransactionHandle TRANSACTION_HANDLE = new ConnectorTransactionHandle() {};
    public static final AccessControlContext CONTEXT = new AccessControlContext(new QueryId("query_id"), Optional.empty(), Optional.empty());

    @Test
    public void testTablePriviledgesRolesNotAllowed()
            throws IOException
    {
        ConnectorAccessControl accessControl = createRangerAccessControl("default-allow-all.json", "user_groups.json", "user_roles.json");
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
            throws IOException
    {
        ConnectorAccessControl accessControl = createRangerAccessControl("default-allow-all.json", "user_groups.json", "user_roles.json");
        accessControl.checkCanShowTablesMetadata(TRANSACTION_HANDLE, user("anyuser"), CONTEXT, "schemaName");
        accessControl.checkCanSetCatalogSessionProperty(TRANSACTION_HANDLE, user("anyuser"), CONTEXT, "schemaName");
        accessControl.checkCanCreateSchema(TRANSACTION_HANDLE, user("anyuser"), CONTEXT, "schemaName");
        accessControl.checkCanShowSchemas(TRANSACTION_HANDLE, user("anyuser"), CONTEXT);
    }

    @Test
    public void testDefaultTableAccessIfNotDefined()
            throws IOException
    {
        ConnectorAccessControl accessControl = createRangerAccessControl("default-allow-all.json", "user_groups.json", "user_roles.json");
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
            throws IOException
    {
        ConnectorAccessControl accessControl = createRangerAccessControl("default-schema-level-access.json", "user_groups.json", "user_roles.json");
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
        assertDenied(() -> accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("foodmart", "test"), ImmutableSet.of("column1")));
        assertDenied(() -> accessControl.checkCanCreateTable(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("foodmart", "test")));
        assertDenied(() -> accessControl.checkCanRenameTable(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("foodmart", "test"), new SchemaTableName("foodmart", "test1")));
        assertDenied(() -> accessControl.checkCanDropTable(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("foodmart", "test")));
        assertDenied(() -> accessControl.checkCanAddColumn(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("foodmart", "test")));
        assertDenied(() -> accessControl.checkCanDropColumn(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("foodmart", "test")));
        assertDenied(() -> accessControl.checkCanRenameColumn(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("foodmart", "test")));
    }

    @Test
    public void testSelectUpdateAccess()
            throws IOException
    {
        ConnectorAccessControl accessControl = createRangerAccessControl("default-table-select-update.json", "user_groups.json", "user_roles.json");
        // 'etladmin' group have all access {group - etladmin, user - alice}
        accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("alice"), CONTEXT, new SchemaTableName("foodmart", "test"), ImmutableSet.of("column1"));
        accessControl.checkCanInsertIntoTable(TRANSACTION_HANDLE, user("alice"), CONTEXT, new SchemaTableName("foodmart", "test"));

        // 'analyst' group have SELECT, UPDATE {group - analyst, user - joe}
        accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("joe"), CONTEXT, new SchemaTableName("foodmart", "test"), ImmutableSet.of("column1"));
        accessControl.checkCanInsertIntoTable(TRANSACTION_HANDLE, user("joe"), CONTEXT, new SchemaTableName("foodmart", "test"));

        //  Access denied to others {group - readall, user - bob}
        accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("foodmart", "test"), ImmutableSet.of("column1"));
        assertDenied(() -> accessControl.checkCanInsertIntoTable(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("foodmart", "test")));
    }

    @Test
    public void testColumnLevelAccess()
            throws IOException
    {
        ConnectorAccessControl accessControl = createRangerAccessControl("default-table-column-access.json", "user_groups.json", "user_roles.json");
        // 'analyst' group have read acces {group - analyst, user - joe}
        accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("joe"), CONTEXT, new SchemaTableName("foodmart", "salary"), ImmutableSet.of("currency_id", "overtime_paid"));

        //  Access denied to others {group - readall, user - bob}
        assertDenied(() -> accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("foodmart", "salary"), ImmutableSet.of("currency_id", "overtime_paid")));
    }

    @Test
    public void testRoleBasedAccess()
            throws IOException
    {
        ConnectorAccessControl accessControl = createRangerAccessControl("ranger-role-based-access.json", "user_groups.json", "user_roles.json");

        // 'admin_role' role have all access {user - raj, group - admin, role - admin_role}
        accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("raj"), CONTEXT, new SchemaTableName("default", "customer"), ImmutableSet.of("column1"));
        accessControl.checkCanInsertIntoTable(TRANSACTION_HANDLE, user("raj"), CONTEXT, new SchemaTableName("default", "customer"));
        accessControl.checkCanDropTable(TRANSACTION_HANDLE, user("raj"), CONTEXT, new SchemaTableName("default", "customer"));

        // 'etl_role' role have all access {user - maria, group - etldev, role - etl_role}
        accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("maria"), CONTEXT, new SchemaTableName("default", "orders"), ImmutableSet.of("column1"));
        accessControl.checkCanInsertIntoTable(TRANSACTION_HANDLE, user("maria"), CONTEXT, new SchemaTableName("default", "orders"));
        accessControl.checkCanDropTable(TRANSACTION_HANDLE, user("maria"), CONTEXT, new SchemaTableName("default", "orders"));
        //  Access denied to columns other than name & comment {user - maria, group - etldev, role - etl_role}
        assertDenied(() -> accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("maria"), CONTEXT, new SchemaTableName("default", "customer"), ImmutableSet.of("column1")));

        // 'analyst_role' role have all access {user - sam, group - analyst, role - analyst_role}
        accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("sam"), CONTEXT, new SchemaTableName("default", "lineitem"), ImmutableSet.of("column1"));
        accessControl.checkCanInsertIntoTable(TRANSACTION_HANDLE, user("sam"), CONTEXT, new SchemaTableName("default", "lineitem"));
        accessControl.checkCanDropTable(TRANSACTION_HANDLE, user("sam"), CONTEXT, new SchemaTableName("default", "lineitem"));
        //  Access denied {user - sam, group - analyst, role - analyst_role}
        assertDenied(() -> accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("sam"), CONTEXT, new SchemaTableName("default", "customer"), ImmutableSet.of("column1")));
        assertDenied(() -> accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("sam"), CONTEXT, new SchemaTableName("default", "supplier"), ImmutableSet.of("column1")));
    }

    private static ConnectorIdentity user(String name)
    {
        return new ConnectorIdentity(name, Optional.empty(), Optional.empty());
    }

    private ConnectorAccessControl createRangerAccessControl(String policyFile, String usersFile, String rolesFile)
            throws IOException
    {
        String policyFilePath = this.getClass().getClassLoader().getResource("com.facebook.presto.hive.security.ranger/" + policyFile).getPath();
        String usersFilePath = this.getClass().getClassLoader().getResource("com.facebook.presto.hive.security.ranger/" + usersFile).getPath();
        String rolesFilePath = this.getClass().getClassLoader().getResource("com.facebook.presto.hive.security.ranger/" + rolesFile).getPath();

        ServicePolicies servicePolicies = jsonParse(new File(policyFilePath), ServicePolicies.class);
        Users users = jsonParse(new File(usersFilePath), Users.class);
        HashMap<String, Set<String>> userRoles = Arrays.stream(jsonParse(new File(rolesFilePath), HashMap[].class)).findFirst().get();

        Map<String, Set<String>> userRolesSet =
                userRoles.entrySet().stream().collect(Collectors.toMap(
                        entry -> entry.getKey(),
                        entry -> new HashSet(entry.getValue())));

        RangerBasedAccessControl rangerBasedAccessControl = new RangerBasedAccessControl();
        RangerAuthorizer rangerAuthorizer = new RangerAuthorizer(servicePolicies);
        rangerBasedAccessControl.setRangerAuthorizer(rangerAuthorizer);
        rangerBasedAccessControl.setUsers(users);
        rangerBasedAccessControl.setUserRoles(userRolesSet);
        return rangerBasedAccessControl;
    }

    private static <T> T jsonParse(File file, Class<T> clazz)
            throws IOException
    {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper.readValue(bufferedReader, clazz);
    }

    private static void assertDenied(ThrowingRunnable runnable)
    {
        assertThrows(AccessDeniedException.class, runnable);
    }
}
