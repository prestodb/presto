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
package com.facebook.presto.tests.hive;

import com.google.common.collect.ImmutableSet;
import com.teradata.tempto.AfterTestWithContext;
import com.teradata.tempto.BeforeTestWithContext;
import com.teradata.tempto.ProductTest;
import com.teradata.tempto.assertions.QueryAssert;
import com.teradata.tempto.query.QueryExecutor;
import com.teradata.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.tests.TestGroups.AUTHORIZATION;
import static com.facebook.presto.tests.TestGroups.HIVE_CONNECTOR;
import static com.facebook.presto.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static com.facebook.presto.tests.TestGroups.ROLES;
import static com.facebook.presto.tests.utils.QueryExecutors.connectToPresto;
import static com.facebook.presto.tests.utils.QueryExecutors.onHive;
import static com.facebook.presto.tests.utils.QueryExecutors.onPresto;
import static com.teradata.tempto.assertions.QueryAssert.Row.row;
import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRoles
        extends ProductTest
{
    private static final String ROLE1 = "role1";
    private static final String ROLE2 = "role2";
    private static final String ROLE3 = "role3";
    private static final Set<String> TEST_ROLES = ImmutableSet.of(ROLE1, ROLE2, ROLE3);

    @BeforeTestWithContext
    public void setUp()
            throws Exception
    {
        onHive().executeQuery("SET ROLE admin");
        cleanup();
    }

    @AfterTestWithContext
    public void tearDown()
            throws Exception
    {
        cleanup();
    }

    public void cleanup()
    {
        Set<String> existentRoles = listRoles();
        for (String role : TEST_ROLES) {
            if (existentRoles.contains(role)) {
                onHive().executeQuery(format("DROP ROLE %s", role));
            }
        }
    }

    private Set<String> listRoles()
    {
        return ImmutableSet.copyOf(
                onHive().executeQuery("SHOW ROLES")
                        .rows()
                        .stream()
                        .map(row -> row.get(0).toString())
                        .collect(toSet()));
    }

    @Test(groups = {HIVE_CONNECTOR, ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testCreateRole()
    {
        onPresto().executeQuery(format("CREATE ROLE %s", ROLE1));
        onPresto().executeQuery(format("CREATE ROLE %s IN hive", ROLE2));
        assertThat(listRoles()).contains(ROLE1, ROLE2);
    }

    @Test(groups = {HIVE_CONNECTOR, ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testDropRole()
    {
        onHive().executeQuery(format("CREATE ROLE %s", ROLE1));
        assertThat(listRoles()).contains(ROLE1);
        onPresto().executeQuery(format("DROP ROLE %s", ROLE1));
        assertThat(listRoles()).doesNotContain(ROLE1);
    }

    @Test(groups = {HIVE_CONNECTOR, ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testListRoles()
    {
        onPresto().executeQuery(format("CREATE ROLE %s", ROLE1));
        onPresto().executeQuery(format("CREATE ROLE %s IN hive", ROLE2));
        QueryResult expected = onHive().executeQuery("SHOW ROLES");
        QueryResult actual = onPresto().executeQuery("SELECT * FROM hive.information_schema.roles");
        assertThat(actual.rows()).containsOnly(expected.rows().toArray(new List[] {}));
    }

    @Test(groups = {HIVE_CONNECTOR, ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testCreateDuplicateRole()
            throws Exception
    {
        onPresto().executeQuery(format("CREATE ROLE %s", ROLE1));
        QueryAssert.assertThat(() -> onPresto().executeQuery(format("CREATE ROLE %s", ROLE1)))
                .failsWithMessage(format("Role '%s' already exists", ROLE1));
    }

    @Test(groups = {HIVE_CONNECTOR, ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testDropNonExistentRole()
            throws Exception
    {
        QueryAssert.assertThat(() -> onPresto().executeQuery(format("DROP ROLE %s", ROLE3)))
                .failsWithMessage(format("Role '%s' does not exist", ROLE3));
    }

    @Test(groups = {HIVE_CONNECTOR, ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testCreateDropRoleAccessControl()
            throws Exception
    {
        // Only users that are granted with "admin" role can create, drop and list roles
        // Alice is not granted with "admin" role
        QueryAssert.assertThat(() -> onPrestoAlice().executeQuery(format("CREATE ROLE %s", ROLE3)))
                .failsWithMessage(format("Cannot create role %s", ROLE3));
        QueryAssert.assertThat(() -> onPrestoAlice().executeQuery(format("DROP ROLE %s", ROLE3)))
                .failsWithMessage(format("Cannot drop role %s", ROLE3));
        QueryAssert.assertThat(() -> onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.roles"))
                .failsWithMessage("Cannot select from table information_schema.roles");
    }

    @Test(groups = {HIVE_CONNECTOR, ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testPublicRoleIsGrantedToEveryone()
            throws Exception
    {
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .contains(row("alice", "USER", "public", "NO"));
        QueryAssert.assertThat(onPrestoBob().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .contains(row("bob", "USER", "public", "NO"));
    }

    @Test(groups = {HIVE_CONNECTOR, ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAdminRoleIsGrantedToHdfs()
            throws Exception
    {
        QueryAssert.assertThat(onPresto().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .contains(row("hdfs", "USER", "admin", "YES"));
    }

    @Test(groups = {HIVE_CONNECTOR, ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRoleToUser()
            throws Exception
    {
        onPresto().executeQuery("CREATE ROLE role1");
        onPresto().executeQuery("GRANT role1 TO USER alice");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"));
    }

    @Test(groups = {HIVE_CONNECTOR, ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRoleToRole()
            throws Exception
    {
        onPresto().executeQuery("CREATE ROLE role1");
        onPresto().executeQuery("CREATE ROLE role2");
        onPresto().executeQuery("GRANT role1 TO USER alice");
        onPresto().executeQuery("GRANT role2 TO ROLE role1");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"));
    }

    @Test(groups = {HIVE_CONNECTOR, ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRoleWithAdminOption()
            throws Exception
    {
        onPresto().executeQuery("CREATE ROLE role1");
        onPresto().executeQuery("CREATE ROLE role2");
        onPresto().executeQuery("GRANT role1 TO USER alice WITH ADMIN OPTION");
        onPresto().executeQuery("GRANT role2 TO ROLE role1 WITH ADMIN OPTION");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "YES"),
                        row("role1", "ROLE", "role2", "YES"));
    }

    @Test(groups = {HIVE_CONNECTOR, ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRoleMultipleTimes()
            throws Exception
    {
        onPresto().executeQuery("CREATE ROLE role1");
        onPresto().executeQuery("CREATE ROLE role2");
        onPresto().executeQuery("GRANT role1 TO USER alice");
        onPresto().executeQuery("GRANT role2 TO ROLE role1");
        onPresto().executeQuery("GRANT role1 TO USER alice");
        onPresto().executeQuery("GRANT role2 TO ROLE role1");
        onPresto().executeQuery("GRANT role1 TO USER alice WITH ADMIN OPTION");
        onPresto().executeQuery("GRANT role2 TO ROLE role1 WITH ADMIN OPTION");
        onPresto().executeQuery("GRANT role1 TO USER alice WITH ADMIN OPTION");
        onPresto().executeQuery("GRANT role2 TO ROLE role1 WITH ADMIN OPTION");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "YES"),
                        row("role1", "ROLE", "role2", "YES"));
    }

    @Test(groups = {HIVE_CONNECTOR, ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeRoleFromUser()
            throws Exception
    {
        onPresto().executeQuery("CREATE ROLE role1");
        onPresto().executeQuery("GRANT role1 TO USER alice");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"));

        onPresto().executeQuery("REVOKE role1 FROM USER alice");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"));
    }

    @Test(groups = {HIVE_CONNECTOR, ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeRoleFromRole()
            throws Exception
    {
        onPresto().executeQuery("CREATE ROLE role1");
        onPresto().executeQuery("CREATE ROLE role2");
        onPresto().executeQuery("GRANT role1 TO USER alice");
        onPresto().executeQuery("GRANT role2 TO ROLE role1");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"));

        onPresto().executeQuery("REVOKE role2 FROM ROLE role1");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"));
    }

    @Test(groups = {HIVE_CONNECTOR, ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testDropGrantedRole()
            throws Exception
    {
        onPresto().executeQuery("CREATE ROLE role1");
        onPresto().executeQuery("GRANT role1 TO USER alice");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"));

        onPresto().executeQuery("DROP ROLE role1");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"));
    }

    @Test(groups = {HIVE_CONNECTOR, ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeTransitiveRoleFromUser()
            throws Exception
    {
        onPresto().executeQuery("CREATE ROLE role1");
        onPresto().executeQuery("CREATE ROLE role2");
        onPresto().executeQuery("GRANT role1 TO USER alice");
        onPresto().executeQuery("GRANT role2 TO ROLE role1");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"));

        onPresto().executeQuery("REVOKE role1 FROM USER alice");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"));
    }

    @Test(groups = {HIVE_CONNECTOR, ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeTransitiveRoleFromRole()
            throws Exception
    {
        onPresto().executeQuery("CREATE ROLE role1");
        onPresto().executeQuery("CREATE ROLE role2");
        onPresto().executeQuery("CREATE ROLE role3");
        onPresto().executeQuery("GRANT role1 TO USER alice");
        onPresto().executeQuery("GRANT role2 TO ROLE role1");
        onPresto().executeQuery("GRANT role3 TO ROLE role2");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"),
                        row("role2", "ROLE", "role3", "NO"));

        onPresto().executeQuery("REVOKE role2 FROM ROLE role1");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"));
    }

    @Test(groups = {HIVE_CONNECTOR, ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testDropTransitiveRole()
            throws Exception
    {
        onPresto().executeQuery("CREATE ROLE role1");
        onPresto().executeQuery("CREATE ROLE role2");
        onPresto().executeQuery("CREATE ROLE role3");
        onPresto().executeQuery("GRANT role1 TO USER alice");
        onPresto().executeQuery("GRANT role2 TO ROLE role1");
        onPresto().executeQuery("GRANT role3 TO ROLE role2");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"),
                        row("role2", "ROLE", "role3", "NO"));

        onPresto().executeQuery("DROP ROLE role2");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"));
    }

    @Test(groups = {HIVE_CONNECTOR, ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeAdminOption()
            throws Exception
    {
        onPresto().executeQuery("CREATE ROLE role1");
        onPresto().executeQuery("CREATE ROLE role2");
        onPresto().executeQuery("GRANT role1 TO USER alice WITH ADMIN OPTION");
        onPresto().executeQuery("GRANT role2 TO ROLE role1 WITH ADMIN OPTION");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "YES"),
                        row("role1", "ROLE", "role2", "YES"));

        onPresto().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER alice");
        onPresto().executeQuery("REVOKE ADMIN OPTION FOR role2 FROM ROLE role1");

        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"));
    }

    @Test(groups = {HIVE_CONNECTOR, ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeMultipleTimes()
            throws Exception
    {
        onPresto().executeQuery("CREATE ROLE role1");
        onPresto().executeQuery("CREATE ROLE role2");
        onPresto().executeQuery("GRANT role1 TO USER alice WITH ADMIN OPTION");
        onPresto().executeQuery("GRANT role2 TO ROLE role1 WITH ADMIN OPTION");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "YES"),
                        row("role1", "ROLE", "role2", "YES"));

        onPresto().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER alice");
        onPresto().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER alice");
        onPresto().executeQuery("REVOKE ADMIN OPTION FOR role2 FROM ROLE role1");
        onPresto().executeQuery("REVOKE ADMIN OPTION FOR role2 FROM ROLE role1");

        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"));

        onPresto().executeQuery("REVOKE role1 FROM USER alice");
        onPresto().executeQuery("REVOKE role1 FROM USER alice");
        onPresto().executeQuery("REVOKE role2 FROM ROLE role1");
        onPresto().executeQuery("REVOKE role2 FROM ROLE role1");

        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"));
    }

    @Test(groups = {HIVE_CONNECTOR, ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRevokeRoleAccessControl()
            throws Exception
    {
        onPresto().executeQuery("CREATE ROLE role1");
        onPresto().executeQuery("CREATE ROLE role2");

        QueryAssert.assertThat(() -> onPrestoAlice().executeQuery("GRANT role1 TO USER bob"))
                .failsWithMessage("Cannot grant roles [role1] to [USER bob]");
        QueryAssert.assertThat(() -> onPrestoAlice().executeQuery("GRANT role1 TO USER bob WITH ADMIN OPTION"))
                .failsWithMessage("Cannot grant roles [role1] to [USER bob]");
        QueryAssert.assertThat(() -> onPrestoAlice().executeQuery("REVOKE role1 FROM USER bob"))
                .failsWithMessage("Cannot revoke roles [role1] from [USER bob]");
        QueryAssert.assertThat(() -> onPrestoAlice().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER bob"))
                .failsWithMessage("Cannot revoke roles [role1] from [USER bob]");

        onPresto().executeQuery("GRANT role1 TO USER alice WITH ADMIN OPTION");

        onPrestoAlice().executeQuery("GRANT role1 TO USER bob");
        onPrestoAlice().executeQuery("GRANT role1 TO USER bob WITH ADMIN OPTION");
        onPrestoAlice().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER bob");
        onPrestoAlice().executeQuery("REVOKE role1 FROM USER bob");

        onPresto().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER alice");

        QueryAssert.assertThat(() -> onPrestoAlice().executeQuery("GRANT role1 TO USER bob"))
                .failsWithMessage("Cannot grant roles [role1] to [USER bob]");
        QueryAssert.assertThat(() -> onPrestoAlice().executeQuery("GRANT role1 TO USER bob WITH ADMIN OPTION"))
                .failsWithMessage("Cannot grant roles [role1] to [USER bob]");
        QueryAssert.assertThat(() -> onPrestoAlice().executeQuery("REVOKE role1 FROM USER bob"))
                .failsWithMessage("Cannot revoke roles [role1] from [USER bob]");
        QueryAssert.assertThat(() -> onPrestoAlice().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER bob"))
                .failsWithMessage("Cannot revoke roles [role1] from [USER bob]");

        onPresto().executeQuery("GRANT role2 TO USER alice");
        onPresto().executeQuery("GRANT role1 TO ROLE role2 WITH ADMIN OPTION");

        onPrestoAlice().executeQuery("GRANT role1 TO USER bob");
        onPrestoAlice().executeQuery("GRANT role1 TO USER bob WITH ADMIN OPTION");
        onPrestoAlice().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER bob");
        onPrestoAlice().executeQuery("REVOKE role1 FROM USER bob");

        onPrestoAlice().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM ROLE role2");

        QueryAssert.assertThat(() -> onPrestoAlice().executeQuery("GRANT role1 TO USER bob"))
                .failsWithMessage("Cannot grant roles [role1] to [USER bob]");
        QueryAssert.assertThat(() -> onPrestoAlice().executeQuery("GRANT role1 TO USER bob WITH ADMIN OPTION"))
                .failsWithMessage("Cannot grant roles [role1] to [USER bob]");
        QueryAssert.assertThat(() -> onPrestoAlice().executeQuery("REVOKE role1 FROM USER bob"))
                .failsWithMessage("Cannot revoke roles [role1] from [USER bob]");
        QueryAssert.assertThat(() -> onPrestoAlice().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER bob"))
                .failsWithMessage("Cannot revoke roles [role1] from [USER bob]");
    }

    @Test(groups = {HIVE_CONNECTOR, ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testSetRole()
            throws Exception
    {
        onPresto().executeQuery("CREATE ROLE role1");
        onPresto().executeQuery("CREATE ROLE role2");
        onPresto().executeQuery("CREATE ROLE role3");
        onPresto().executeQuery("GRANT role1 TO USER alice");
        onPresto().executeQuery("GRANT role2 TO ROLE role1");
        onPresto().executeQuery("GRANT role3 TO ROLE role2");

        onPrestoAlice().executeQuery("SET ROLE ALL");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                .containsOnly(
                        row("public"),
                        row("role1"),
                        row("role2"),
                        row("role3"));

        onPrestoAlice().executeQuery("SET ROLE NONE");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                .containsOnly(
                        row("public"));

        onPrestoAlice().executeQuery("SET ROLE role1");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                .containsOnly(
                        row("public"),
                        row("role1"),
                        row("role2"),
                        row("role3"));

        onPrestoAlice().executeQuery("SET ROLE role2");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                .containsOnly(
                        row("public"),
                        row("role2"),
                        row("role3"));

        onPrestoAlice().executeQuery("SET ROLE role3");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                .containsOnly(
                        row("public"),
                        row("role3"));
    }

    @Test(groups = {HIVE_CONNECTOR, ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testSetAdminRole()
            throws Exception
    {
        QueryAssert.assertThat(onPresto().executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                .containsOnly(
                        row("public"));
        onPresto().executeQuery("SET ROLE admin");
        QueryAssert.assertThat(onPresto().executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                .containsOnly(
                        row("public"),
                        row("admin"));
    }

    @Test(groups = {HIVE_CONNECTOR, ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testShowRoles()
            throws Exception
    {
        QueryAssert.assertThat(onPresto().executeQuery("SHOW ROLES"))
                .containsOnly(
                        row("public"),
                        row("admin"));
        onPresto().executeQuery("CREATE ROLE role1");
        QueryAssert.assertThat(onPresto().executeQuery("SHOW ROLES"))
                .containsOnly(
                        row("public"),
                        row("admin"),
                        row("role1"));
        QueryAssert.assertThat(() -> onPrestoAlice().executeQuery("SHOW ROLES"))
                .failsWithMessage("Cannot show roles from catalog hive");
        onPresto().executeQuery("GRANT admin TO alice");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW ROLES"))
                .containsOnly(
                        row("public"),
                        row("admin"),
                        row("role1"));
    }

    @Test(groups = {HIVE_CONNECTOR, ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testShowCurrentRoles()
            throws Exception
    {
        QueryAssert.assertThat(onPresto().executeQuery("SHOW CURRENT ROLES"))
                .containsOnly(
                        row("public"));
        onPresto().executeQuery("CREATE ROLE role1");
        onPresto().executeQuery("CREATE ROLE role2");
        onPresto().executeQuery("GRANT role1 TO alice");
        onPresto().executeQuery("GRANT role2 TO alice");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW CURRENT ROLES"))
                .containsOnly(
                        row("public"),
                        row("role1"),
                        row("role2"));
        onPrestoAlice().executeQuery("SET ROLE role2");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW CURRENT ROLES"))
                .containsOnly(
                        row("public"),
                        row("role2"));
    }

    private static QueryExecutor onPrestoAlice()
    {
        return connectToPresto("alice@presto");
    }

    private static QueryExecutor onPrestoBob()
    {
        return connectToPresto("bob@presto");
    }
}
