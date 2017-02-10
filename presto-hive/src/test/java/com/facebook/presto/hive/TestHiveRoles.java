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
package com.facebook.presto.hive;

import com.facebook.presto.Session;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestHiveRoles
        extends AbstractTestQueryFramework
{
    protected TestHiveRoles()
    {
        super(HiveQueryRunner::createQueryRunner);
    }

    @AfterMethod
    public void afterMethod()
    {
        for (String role : listRoles()) {
            executeFromAdmin("DROP ROLE " + role);
        }
    }

    @Test
    public void testCreateRole()
            throws Exception
    {
        executeFromAdmin("CREATE ROLE role1");
        assertEquals(listRoles(), ImmutableSet.of("role1"));
        executeFromAdmin("CREATE ROLE role2 IN hive");
        assertEquals(listRoles(), ImmutableSet.of("role1", "role2"));
    }

    @Test
    public void testCreateDuplicateRole()
            throws Exception
    {
        executeFromAdmin("CREATE ROLE duplicate_role");
        assertQueryFails(createAdminSession(), "CREATE ROLE duplicate_role", ".*?Role 'duplicate_role' already exists");
    }

    @Test
    public void testCreateRoleWithAdminOption()
            throws Exception
    {
        assertQueryFails(createAdminSession(), "CREATE ROLE role1 WITH ADMIN admin", ".*?Hive Connector does not support WITH ADMIN statement");
    }

    @Test
    public void testCreateReservedRole()
            throws Exception
    {
        assertQueryFails(createAdminSession(), "CREATE ROLE all", "Role name cannot be one of the reserved roles: \\[all, default, none\\]");
        assertQueryFails(createAdminSession(), "CREATE ROLE default", "Role name cannot be one of the reserved roles: \\[all, default, none\\]");
        assertQueryFails(createAdminSession(), "CREATE ROLE none", "Role name cannot be one of the reserved roles: \\[all, default, none\\]");
    }

    @Test
    public void testCreateRoleByNonAdminUser()
            throws Exception
    {
        assertQueryFails(createUserSession("non_admin_user"), "CREATE ROLE role1", "Access Denied: Cannot create role role1");
    }

    @Test
    public void testDropRole()
            throws Exception
    {
        executeFromAdmin("CREATE ROLE role1");
        executeFromAdmin("CREATE ROLE role2");
        assertEquals(listRoles(), ImmutableSet.of("role1", "role2"));
        executeFromAdmin("DROP ROLE role2");
        assertEquals(listRoles(), ImmutableSet.of("role1"));
        executeFromAdmin("DROP ROLE role1 IN hive");
        assertEquals(listRoles(), ImmutableSet.of());
    }

    @Test
    public void testDropNonExistentRole()
            throws Exception
    {
        assertQueryFails(createAdminSession(), "DROP ROLE non_existent_role", ".*?Role 'non_existent_role' does not exist");
    }

    @Test
    public void testDropRoleByNonAdminUser()
            throws Exception
    {
        assertQueryFails(createUserSession("non_admin_user"), "DROP ROLE role1", "Access Denied: Cannot drop role role1");
    }

    @Test
    public void testListRolesByNonAdminUser()
            throws Exception
    {
        assertQueryFails(createUserSession("non_admin_user"), "SELECT * FROM hive.information_schema.roles", "Access Denied: Cannot select from table information_schema.roles");
    }

    private Set<String> listRoles()
    {
        return executeFromAdmin("SELECT * FROM hive.information_schema.roles")
                .getMaterializedRows()
                .stream()
                .map(row -> row.getField(0).toString())
                .collect(Collectors.toSet());
    }

    private MaterializedResult executeFromAdmin(String sql)
    {
        return getQueryRunner().execute(createAdminSession(), sql);
    }

    private MaterializedResult executeFromUser(String user, String sql)
    {
        return getQueryRunner().execute(createUserSession(user), sql);
    }

    private Session createAdminSession()
    {
        return createUserSession("admin");
    }

    private Session createUserSession(String user)
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setIdentity(new Identity(user, Optional.empty()))
                .build();
    }
}
