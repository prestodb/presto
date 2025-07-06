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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.prestodb.tempto.AfterTestWithContext;
import io.prestodb.tempto.BeforeTestWithContext;
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.assertions.QueryAssert;
import io.prestodb.tempto.query.QueryExecutor;
import io.prestodb.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.tests.TestGroups.AUTHORIZATION;
import static com.facebook.presto.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static com.facebook.presto.tests.TestGroups.ROLES;
import static com.facebook.presto.tests.utils.QueryExecutors.connectToPresto;
import static com.facebook.presto.tests.utils.QueryExecutors.onHive;
import static com.facebook.presto.tests.utils.QueryExecutors.onPresto;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static java.lang.String.format;
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
    {
        onPresto().executeQuery("SET ROLE admin");
        onHive().executeQuery("SET ROLE admin");
        cleanup();
    }

    @AfterTestWithContext
    public void tearDown()
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
        return onHive().executeQuery("SHOW ROLES").rows().stream()
                .map(Iterables::getOnlyElement)
                .map(String.class::cast)
                .collect(toImmutableSet());
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testCreateRole()
    {
        onPresto().executeQuery(format("CREATE ROLE %s", ROLE1));
        assertThat(listRoles()).contains(ROLE1);
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testDropRole()
    {
        onHive().executeQuery(format("CREATE ROLE %s", ROLE1));
        assertThat(listRoles()).contains(ROLE1);
        onPresto().executeQuery(format("DROP ROLE %s", ROLE1));
        assertThat(listRoles()).doesNotContain(ROLE1);
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testListRoles()
    {
        onPresto().executeQuery(format("CREATE ROLE %s", ROLE1));
        QueryResult expected = onHive().executeQuery("SHOW ROLES");
        QueryResult actual = onPresto().executeQuery("SELECT * FROM hive.information_schema.roles");
        assertThat(actual.rows()).containsOnly(expected.rows().toArray(new List[] {}));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testCreateDuplicateRole()
    {
        onPresto().executeQuery(format("CREATE ROLE %s", ROLE1));
        QueryAssert.assertThat(() -> onPresto().executeQuery(format("CREATE ROLE %s", ROLE1)))
                .failsWithMessage(format("Role '%s' already exists", ROLE1));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testDropNonExistentRole()
    {
        QueryAssert.assertThat(() -> onPresto().executeQuery(format("DROP ROLE %s", ROLE3)))
                .failsWithMessage(format("Role '%s' does not exist", ROLE3));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControl()
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

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testPublicRoleIsGrantedToEveryone()
    {
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .contains(row("alice", "USER", "public", "NO"));
        QueryAssert.assertThat(onPrestoBob().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .contains(row("bob", "USER", "public", "NO"));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAdminRoleIsGrantedToHdfs()
    {
        QueryAssert.assertThat(onPresto().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .contains(row("hdfs", "USER", "admin", "YES"));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRoleToUser()
    {
        onPresto().executeQuery("CREATE ROLE role1");
        onPresto().executeQuery("GRANT role1 TO USER alice");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRoleToRole()
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

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRoleWithAdminOption()
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

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRoleMultipleTimes()
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

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeRoleFromUser()
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

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeRoleFromRole()
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

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeRoleFromOwner()
    {
        try {
            onPrestoAlice().executeQuery("CREATE TABLE hive.default.test_table (foo BIGINT)");
            QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW GRANTS ON hive.default.test_table"))
                    .containsOnly(ImmutableList.of(
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "SELECT", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "DELETE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "UPDATE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "INSERT", "YES", null)));

            onPresto().executeQuery("REVOKE SELECT ON hive.default.test_table FROM USER alice");

            // now there should be no SELECT privileges shown even though alice has OWNERSHIP
            QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW GRANTS ON hive.default.test_table"))
                    .containsOnly(ImmutableList.of(
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "DELETE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "UPDATE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "INSERT", "YES", null)));
        }
        finally {
            onPrestoAlice().executeQuery("DROP TABLE hive.default.test_table");
        }
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testDropGrantedRole()
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

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeTransitiveRoleFromUser()
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

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeTransitiveRoleFromRole()
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

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testDropTransitiveRole()
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

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeAdminOption()
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

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeMultipleTimes()
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

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRevokeRoleAccessControl()
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

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testSetRole()
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

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testSetAdminRole()
    {
        onPresto().executeQuery("SET ROLE NONE");
        QueryAssert.assertThat(onPresto().executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                .containsOnly(
                        row("public"));
        onPresto().executeQuery("SET ROLE admin");
        QueryAssert.assertThat(onPresto().executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                .containsOnly(
                        row("public"),
                        row("admin"));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testShowRoles()
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
        onPrestoAlice().executeQuery("SET ROLE admin");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW ROLES"))
                .containsOnly(
                        row("public"),
                        row("admin"),
                        row("role1"));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testShowCurrentRoles()
    {
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW CURRENT ROLES"))
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

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testShowRoleGrants()
    {
        QueryAssert.assertThat(onPresto().executeQuery("SHOW ROLE GRANTS"))
                .containsOnly(
                        row("public"),
                        row("admin"));
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW ROLE GRANTS"))
                .containsOnly(
                        row("public"));
        onPresto().executeQuery("CREATE ROLE role1");
        onPresto().executeQuery("CREATE ROLE role2");
        onPresto().executeQuery("GRANT role1 TO alice");
        onPresto().executeQuery("GRANT role2 TO role1");
        QueryAssert.assertThat(onPresto().executeQuery("SHOW ROLE GRANTS"))
                .containsOnly(
                        row("public"),
                        row("admin"));
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW ROLE GRANTS"))
                .containsOnly(
                        row("public"),
                        row("role1"));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testSetRoleCreateDropSchema()
    {
        assertAdminExecute("CREATE SCHEMA hive.test_admin_schema");
        onPresto().executeQuery("DROP SCHEMA hive.test_admin_schema");
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAdminCanDropAnyTable()
    {
        onPrestoAlice().executeQuery("CREATE TABLE hive.default.test_table (foo BIGINT)");
        assertAdminExecute("DROP TABLE hive.default.test_table");
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAdminCanRenameAnyTable()
    {
        onPrestoAlice().executeQuery("CREATE TABLE hive.default.test_table (foo BIGINT)");
        assertAdminExecute("ALTER TABLE hive.default.test_table RENAME TO hive.default.test_table_1");
        onPrestoAlice().executeQuery("DROP TABLE hive.default.test_table_1");
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAdminCanAddColumnToAnyTable()
    {
        onPrestoAlice().executeQuery("CREATE TABLE hive.default.test_table (foo BIGINT)");
        assertAdminExecute("ALTER TABLE hive.default.test_table ADD COLUMN bar DATE");
        onPrestoAlice().executeQuery("DROP TABLE hive.default.test_table");
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAdminCanRenameColumnInAnyTable()
    {
        onPrestoAlice().executeQuery("CREATE TABLE hive.default.test_table (foo BIGINT)");
        assertAdminExecute("ALTER TABLE hive.default.test_table RENAME COLUMN foo TO bar");
        onPrestoAlice().executeQuery("DROP TABLE hive.default.test_table");
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAdminCanShowAllGrants()
    {
        try {
            onPrestoBob().executeQuery("CREATE TABLE hive.default.test_table_bob (foo BIGINT)");
            onPrestoAlice().executeQuery("CREATE TABLE hive.default.test_table_alice (foo BIGINT)");
            onPresto().executeQuery("GRANT admin TO alice");
            onPrestoAlice().executeQuery("SET ROLE ADMIN");

            QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW GRANTS ON hive.default.test_table_alice"))
                    .containsOnly(ImmutableList.of(
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "SELECT", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "DELETE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "UPDATE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "INSERT", "YES", null)));

            QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW GRANTS ON hive.default.test_table_bob"))
                    .containsOnly(ImmutableList.of(
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "SELECT", "YES", null),
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "DELETE", "YES", null),
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "UPDATE", "YES", null),
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "INSERT", "YES", null)));

            onPrestoAlice().executeQuery("GRANT SELECT ON hive.default.test_table_alice  TO bob WITH GRANT OPTION");
            onPrestoAlice().executeQuery("GRANT INSERT ON hive.default.test_table_alice  TO bob");

            QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW GRANTS ON hive.default.test_table_alice"))
                    .containsOnly(ImmutableList.of(
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "SELECT", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "DELETE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "UPDATE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "INSERT", "YES", null),
                            row("alice", "USER", "bob", "USER", "hive", "default", "test_table_alice", "SELECT", "YES", null),
                            row("alice", "USER", "bob", "USER", "hive", "default", "test_table_alice", "INSERT", "NO", null)));
        }
        finally {
            onPrestoAlice().executeQuery("DROP TABLE hive.default.test_table_alice");
            onPrestoBob().executeQuery("DROP TABLE hive.default.test_table_bob");
            onPresto().executeQuery("REVOKE admin FROM alice");
        }
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAdminCanShowGrantsOnlyFromCurrentSchema()
    {
        try {
            onPrestoBob().executeQuery("CREATE TABLE hive.default.test_table_bob (foo BIGINT)");
            onPresto().executeQuery("CREATE SCHEMA hive.test");
            onPresto().executeQuery("GRANT admin TO alice");
            onPrestoAlice().executeQuery("SET ROLE ADMIN");
            onPrestoAlice().executeQuery("CREATE TABLE hive.test.test_table_bob (foo BIGINT) with (external_location='/tmp')");

            QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW GRANTS ON hive.default.test_table_bob"))
                    .containsOnly(ImmutableList.of(
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "SELECT", "YES", null),
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "DELETE", "YES", null),
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "UPDATE", "YES", null),
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "INSERT", "YES", null)));

            QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW GRANTS ON hive.test.test_table_bob"))
                    .containsOnly(ImmutableList.of(
                            row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "SELECT", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "DELETE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "UPDATE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "INSERT", "YES", null)));
            QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.table_privileges where table_name = 'test_table_bob'"))
                    .containsOnly(ImmutableList.of(
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "SELECT", "YES", null),
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "DELETE", "YES", null),
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "UPDATE", "YES", null),
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "INSERT", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "SELECT", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "DELETE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "UPDATE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "INSERT", "YES", null)));
        }
        finally {
            onPrestoBob().executeQuery("DROP TABLE hive.default.test_table_bob");
            onPrestoAlice().executeQuery("DROP TABLE hive.test.test_table_bob");
            onPresto().executeQuery("DROP SCHEMA hive.test");
            onPresto().executeQuery("REVOKE admin FROM alice");
        }
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testSetRoleTablePermissions()
    {
        onPresto().executeQuery("CREATE ROLE role1");
        onPresto().executeQuery("CREATE ROLE role2");

        onPresto().executeQuery("GRANT role1 TO USER bob");
        onPresto().executeQuery("GRANT role2 TO USER bob");

        onPrestoAlice().executeQuery("CREATE TABLE hive.default.test_table (foo BIGINT)");
        onPrestoAlice().executeQuery("GRANT SELECT ON hive.default.test_table TO ROLE role1");
        onPrestoAlice().executeQuery("GRANT INSERT ON hive.default.test_table TO ROLE role2");

        String select = "SELECT * FROM hive.default.test_table";
        String insert = "INSERT INTO hive.default.test_table (foo) VALUES (1)";

        assertAdminExecute(select);
        assertAdminExecute(insert);

        onPrestoBob().executeQuery(select);
        onPrestoBob().executeQuery(insert);
        QueryAssert.assertThat(onPrestoBob().executeQuery("SHOW GRANTS ON hive.default.test_table"))
                .containsOnly(ImmutableList.of(
                        row("alice", "USER", "role1", "ROLE", "hive", "default", "test_table", "SELECT", "NO", null),
                        row("alice", "USER", "role2", "ROLE", "hive", "default", "test_table", "INSERT", "NO", null)));

        onPrestoBob().executeQuery("SET ROLE ALL");
        onPrestoBob().executeQuery(select);
        onPrestoBob().executeQuery(insert);
        QueryAssert.assertThat(onPrestoBob().executeQuery("SHOW GRANTS ON hive.default.test_table"))
                .containsOnly(ImmutableList.of(
                        row("alice", "USER", "role1", "ROLE", "hive", "default", "test_table", "SELECT", "NO", null),
                        row("alice", "USER", "role2", "ROLE", "hive", "default", "test_table", "INSERT", "NO", null)));

        onPrestoBob().executeQuery("SET ROLE NONE");
        QueryAssert.assertThat(() -> onPrestoBob().executeQuery(select))
                .failsWithMessage("Access Denied");
        QueryAssert.assertThat(() -> onPrestoBob().executeQuery(insert))
                .failsWithMessage("Access Denied");
        QueryAssert.assertThat(onPrestoBob().executeQuery("SHOW GRANTS ON hive.default.test_table"))
                .containsOnly(ImmutableList.of());

        onPrestoBob().executeQuery("SET ROLE role1");
        onPrestoBob().executeQuery(select);
        QueryAssert.assertThat(() -> onPrestoBob().executeQuery(insert))
                .failsWithMessage("Access Denied");
        QueryAssert.assertThat(onPrestoBob().executeQuery("SHOW GRANTS ON hive.default.test_table"))
                .containsOnly(ImmutableList.of(
                        row("alice", "USER", "role1", "ROLE", "hive", "default", "test_table", "SELECT", "NO", null)));

        onPrestoBob().executeQuery("SET ROLE role2");
        QueryAssert.assertThat(() -> onPrestoBob().executeQuery(select))
                .failsWithMessage("Access Denied");
        onPrestoBob().executeQuery(insert);
        QueryAssert.assertThat(onPrestoBob().executeQuery("SHOW GRANTS ON hive.default.test_table"))
                .containsOnly(ImmutableList.of(
                        row("alice", "USER", "role2", "ROLE", "hive", "default", "test_table", "INSERT", "NO", null)));

        onPrestoAlice().executeQuery("DROP TABLE hive.default.test_table");
    }

    private static void assertAdminExecute(String query)
    {
        onPresto().executeQuery("SET ROLE NONE");
        QueryAssert.assertThat(() -> onPresto().executeQuery(query))
                .failsWithMessage("Access Denied");

        onPresto().executeQuery("SET ROLE ALL");
        QueryAssert.assertThat(() -> onPresto().executeQuery(query))
                .failsWithMessage("Access Denied");

        onPresto().executeQuery("SET ROLE admin");
        onPresto().executeQuery(query);
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
