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
package com.facebook.presto.plugin.memory;

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.ViewExpression;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.DELETE_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_VIEW;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.privilege;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMaterializedViewAccessControl
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("memory")
                .setSchema("default")
                .setSystemProperty("legacy_materialized_views", "false")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(4)
                .setExtraProperties(ImmutableMap.of("experimental.allow-legacy-materialized-views-toggle", "true"))
                .build();

        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog("memory", "memory", ImmutableMap.of());

        return queryRunner;
    }

    @Test
    public void testCreateMaterializedViewRequiresBothCreateTableAndCreateView()
    {
        // Setup: Create a base table
        assertUpdate("CREATE TABLE test_base (id BIGINT, value VARCHAR)");
        assertUpdate("INSERT INTO test_base VALUES (1, 'test')", 1);

        try {
            // Deny only CREATE_VIEW - should fail
            getQueryRunner().getAccessControl().deny(privilege("test_mv_create_table_only", CREATE_VIEW));
            assertQueryFails(
                    "CREATE MATERIALIZED VIEW test_mv_create_table_only AS SELECT * FROM test_base",
                    ".*Cannot create view.*");
            getQueryRunner().getAccessControl().reset();

            // Deny only CREATE_TABLE - should fail
            getQueryRunner().getAccessControl().deny(privilege("test_mv_create_view_only", CREATE_TABLE));
            assertQueryFails(
                    "CREATE MATERIALIZED VIEW test_mv_create_view_only AS SELECT * FROM test_base",
                    ".*Cannot create table.*");
            getQueryRunner().getAccessControl().reset();

            // Allow both - should succeed
            assertUpdate("CREATE MATERIALIZED VIEW test_mv_both_perms AS SELECT * FROM test_base");
            assertUpdate("DROP MATERIALIZED VIEW test_mv_both_perms");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_base");
            getQueryRunner().getAccessControl().reset();
        }
    }

    @Test
    public void testRefreshMaterializedViewRequiresBothDeleteAndInsert()
    {
        assertUpdate("CREATE TABLE refresh_base (id BIGINT, value VARCHAR)");
        assertUpdate("INSERT INTO refresh_base VALUES (1, 'test')", 1);
        assertUpdate("CREATE MATERIALIZED VIEW test_mv_refresh AS SELECT * FROM refresh_base");

        try {
            // Deny only INSERT_TABLE - should fail
            getQueryRunner().getAccessControl().deny(privilege("test_mv_refresh", INSERT_TABLE));
            assertQueryFails(
                    "REFRESH MATERIALIZED VIEW test_mv_refresh",
                    ".*Cannot insert into table.*");
            getQueryRunner().getAccessControl().reset();

            // Deny only DELETE_TABLE - should fail
            getQueryRunner().getAccessControl().deny(privilege("test_mv_refresh", DELETE_TABLE));
            assertQueryFails(
                    "REFRESH MATERIALIZED VIEW test_mv_refresh",
                    ".*Cannot delete from table.*");
            getQueryRunner().getAccessControl().reset();

            // Allow both - should succeed
            assertUpdate("REFRESH MATERIALIZED VIEW test_mv_refresh", 1);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS test_mv_refresh");
            assertUpdate("DROP TABLE IF EXISTS refresh_base");
            getQueryRunner().getAccessControl().reset();
        }
    }

    @Test
    public void testDropMaterializedViewRequiresBothDropTableAndDropView()
    {
        try {
            // Deny only DROP_VIEW - should fail
            assertUpdate("CREATE TABLE drop_base1 (id BIGINT)");
            assertUpdate("CREATE MATERIALIZED VIEW test_mv_drop1 AS SELECT * FROM drop_base1");

            getQueryRunner().getAccessControl().deny(privilege("test_mv_drop1", DROP_VIEW));
            assertQueryFails(
                    "DROP MATERIALIZED VIEW test_mv_drop1",
                    ".*Cannot drop view.*");
            getQueryRunner().getAccessControl().reset();

            assertUpdate("DROP MATERIALIZED VIEW test_mv_drop1");
            assertUpdate("DROP TABLE drop_base1");

            //  Deny only DROP_TABLE - should fail
            assertUpdate("CREATE TABLE drop_base2 (id BIGINT)");
            assertUpdate("CREATE MATERIALIZED VIEW test_mv_drop2 AS SELECT * FROM drop_base2");

            getQueryRunner().getAccessControl().deny(privilege("test_mv_drop2", DROP_TABLE));
            assertQueryFails(
                    "DROP MATERIALIZED VIEW test_mv_drop2",
                    ".*Cannot drop table.*");
            getQueryRunner().getAccessControl().reset();

            assertUpdate("DROP MATERIALIZED VIEW test_mv_drop2");
            assertUpdate("DROP TABLE drop_base2");

            // Allow both - should succeed
            assertUpdate("CREATE TABLE drop_base3 (id BIGINT)");
            assertUpdate("CREATE MATERIALIZED VIEW test_mv_drop3 AS SELECT * FROM drop_base3");
            assertUpdate("DROP MATERIALIZED VIEW test_mv_drop3");
            assertUpdate("DROP TABLE drop_base3");
        }
        finally {
            getQueryRunner().getAccessControl().reset();
        }
    }

    @Test
    public void testSecurityDefinerAllowsUnprivilegedUserToQuery()
    {
        Session adminSession = createSessionForUser("admin");
        Session restrictedSession = createSessionForUser("restricted_user");

        assertUpdate(adminSession, "CREATE TABLE secure_base (id BIGINT, secret VARCHAR, value BIGINT)");
        assertUpdate(adminSession, "INSERT INTO secure_base VALUES (1, 'confidential', 100), (2, 'classified', 200)", 2);

        try {
            // Deny restricted_user access to secure_base
            getQueryRunner().getAccessControl().deny(privilege("restricted_user", "secure_base", SELECT_COLUMN));

            assertUpdate(adminSession,
                    "CREATE MATERIALIZED VIEW mv_definer " +
                    "SECURITY DEFINER AS " +
                    "SELECT id, secret, value FROM secure_base");

            assertQuery(adminSession, "SELECT COUNT(*) FROM mv_definer", "SELECT 2");
            assertQuery(adminSession, "SELECT id, value FROM mv_definer ORDER BY id",
                    "VALUES (1, 100), (2, 200)");

            assertQueryFails(restrictedSession, "SELECT COUNT(*) FROM secure_base", ".*Access Denied.*");

            assertQuery(restrictedSession, "SELECT COUNT(*) FROM mv_definer", "SELECT 2");
            assertQuery(restrictedSession, "SELECT id, value FROM mv_definer ORDER BY id",
                    "VALUES (1, 100), (2, 200)");

            assertUpdate(adminSession, "DROP MATERIALIZED VIEW mv_definer");
            assertUpdate(adminSession, "DROP TABLE secure_base");
        }
        finally {
            getQueryRunner().getAccessControl().reset();
        }
    }

    @Test
    public void testSecurityInvokerUsesCurrentUserPermissions()
    {
        Session adminSession = createSessionForUser("admin");
        Session restrictedSession = createSessionForUser("restricted_user");

        assertUpdate(adminSession, "CREATE TABLE invoker_base (id BIGINT, data VARCHAR, value BIGINT)");
        assertUpdate(adminSession, "INSERT INTO invoker_base VALUES (1, 'data1', 100), (2, 'data2', 200)", 2);

        try {
            getQueryRunner().getAccessControl().deny(privilege("restricted_user", "invoker_base", SELECT_COLUMN));

            assertUpdate(adminSession,
                    "CREATE MATERIALIZED VIEW mv_invoker " +
                    "SECURITY INVOKER AS " +
                    "SELECT id, data, value FROM invoker_base");

            assertQuery(adminSession, "SELECT COUNT(*) FROM mv_invoker", "SELECT 2");
            assertQuery(adminSession, "SELECT id, value FROM mv_invoker ORDER BY id",
                    "VALUES (1, 100), (2, 200)");

            assertQueryFails(restrictedSession, "SELECT COUNT(*) FROM mv_invoker",
                    ".*Access Denied.*");

            assertUpdate(adminSession, "DROP MATERIALIZED VIEW mv_invoker");
            assertUpdate(adminSession, "DROP TABLE invoker_base");
        }
        finally {
            getQueryRunner().getAccessControl().reset();
        }
    }

    @Test
    public void testRefreshWithSecurityDefiner()
    {
        Session adminSession = createSessionForUser("admin");
        Session restrictedSession = createSessionForUser("restricted_user");

        assertUpdate(adminSession, "CREATE TABLE refresh_definer_base (id BIGINT, value BIGINT)");
        assertUpdate(adminSession, "INSERT INTO refresh_definer_base VALUES (1, 100), (2, 200)", 2);

        try {
            getQueryRunner().getAccessControl().deny(privilege("restricted_user", "refresh_definer_base", SELECT_COLUMN));

            assertUpdate(adminSession,
                    "CREATE MATERIALIZED VIEW mv_refresh_definer " +
                    "SECURITY DEFINER AS " +
                    "SELECT id, value FROM refresh_definer_base");

            assertUpdate(adminSession, "REFRESH MATERIALIZED VIEW mv_refresh_definer", 2);
            assertQuery(adminSession, "SELECT COUNT(*) FROM mv_refresh_definer", "SELECT 2");

            assertUpdate(adminSession, "INSERT INTO refresh_definer_base VALUES (3, 300)", 1);

            assertUpdate(restrictedSession, "REFRESH MATERIALIZED VIEW mv_refresh_definer", 3);
            assertQuery(restrictedSession, "SELECT COUNT(*) FROM mv_refresh_definer", "SELECT 3");

            assertUpdate(adminSession, "DROP MATERIALIZED VIEW mv_refresh_definer");
            assertUpdate(adminSession, "DROP TABLE refresh_definer_base");
        }
        finally {
            getQueryRunner().getAccessControl().reset();
        }
    }

    @Test
    public void testRefreshWithSecurityInvoker()
    {
        Session adminSession = createSessionForUser("admin");
        Session restrictedSession = createSessionForUser("restricted_user");

        assertUpdate(adminSession, "CREATE TABLE refresh_invoker_base (id BIGINT, value BIGINT)");
        assertUpdate(adminSession, "INSERT INTO refresh_invoker_base VALUES (1, 100), (2, 200)", 2);

        assertUpdate(adminSession,
                "CREATE MATERIALIZED VIEW mv_refresh_invoker " +
                "SECURITY INVOKER AS " +
                "SELECT id, value FROM refresh_invoker_base");

        assertUpdate(adminSession, "REFRESH MATERIALIZED VIEW mv_refresh_invoker", 2);
        assertQuery(adminSession, "SELECT COUNT(*) FROM mv_refresh_invoker", "SELECT 2");

        assertUpdate(adminSession, "INSERT INTO refresh_invoker_base VALUES (3, 300)", 1);

        assertUpdate(restrictedSession, "REFRESH MATERIALIZED VIEW mv_refresh_invoker", 3);
        assertQuery(restrictedSession, "SELECT COUNT(*) FROM mv_refresh_invoker", "SELECT 3");

        assertUpdate(adminSession, "DROP MATERIALIZED VIEW mv_refresh_invoker");
        assertUpdate(adminSession, "DROP TABLE refresh_invoker_base");
    }

    @Test
    public void testDefaultViewSecurityModeDefiner()
    {
        Session adminSession = Session.builder(createSessionForUser("admin"))
                .setSystemProperty("default_view_security_mode", "DEFINER")
                .build();
        Session restrictedSession = createSessionForUser("restricted_user");

        assertUpdate(adminSession, "CREATE TABLE default_definer_base (id BIGINT, secret VARCHAR)");
        assertUpdate(adminSession, "INSERT INTO default_definer_base VALUES (1, 'secret1'), (2, 'secret2')", 2);

        try {
            getQueryRunner().getAccessControl().deny(privilege("restricted_user", "default_definer_base", SELECT_COLUMN));

            assertUpdate(adminSession,
                    "CREATE MATERIALIZED VIEW mv_default_definer AS " +
                    "SELECT id, secret FROM default_definer_base");

            assertQuery(adminSession, "SELECT COUNT(*) FROM mv_default_definer", "SELECT 2");

            assertQueryFails(restrictedSession, "SELECT COUNT(*) FROM default_definer_base", ".*Access Denied.*");

            assertQuery(restrictedSession, "SELECT COUNT(*) FROM mv_default_definer", "SELECT 2");

            String showCreate = (String) computeScalar(adminSession, "SHOW CREATE MATERIALIZED VIEW mv_default_definer");
            assertTrue(showCreate.contains("SECURITY DEFINER"),
                    "SHOW CREATE should include SECURITY DEFINER for MV created with default_view_security_mode=DEFINER");

            assertUpdate(adminSession, "DROP MATERIALIZED VIEW mv_default_definer");
            assertUpdate(adminSession, "DROP TABLE default_definer_base");
        }
        finally {
            getQueryRunner().getAccessControl().reset();
        }
    }

    @Test
    public void testDefaultViewSecurityModeInvoker()
    {
        Session adminSession = Session.builder(createSessionForUser("admin"))
                .setSystemProperty("default_view_security_mode", "INVOKER")
                .build();
        Session restrictedSession = createSessionForUser("restricted_user");

        assertUpdate(adminSession, "CREATE TABLE default_invoker_base (id BIGINT, data VARCHAR)");
        assertUpdate(adminSession, "INSERT INTO default_invoker_base VALUES (1, 'data1'), (2, 'data2')", 2);

        try {
            getQueryRunner().getAccessControl().deny(privilege("restricted_user", "default_invoker_base", SELECT_COLUMN));

            assertUpdate(adminSession,
                    "CREATE MATERIALIZED VIEW mv_default_invoker AS " +
                    "SELECT id, data FROM default_invoker_base");

            assertQuery(adminSession, "SELECT COUNT(*) FROM mv_default_invoker", "SELECT 2");

            assertQueryFails(restrictedSession, "SELECT COUNT(*) FROM mv_default_invoker",
                    ".*Access Denied.*");

            String showCreate = (String) computeScalar(adminSession, "SHOW CREATE MATERIALIZED VIEW mv_default_invoker");
            assertTrue(showCreate.contains("SECURITY INVOKER"),
                    "SHOW CREATE should include SECURITY INVOKER for MV created with default_view_security_mode=INVOKER");

            assertUpdate(adminSession, "DROP MATERIALIZED VIEW mv_default_invoker");
            assertUpdate(adminSession, "DROP TABLE default_invoker_base");
        }
        finally {
            getQueryRunner().getAccessControl().reset();
        }
    }

    @Test
    public void testAccessControlOnMaterializedViewObject()
    {
        Session adminSession = createSessionForUser("admin");
        Session restrictedSession = createSessionForUser("restricted_user");

        assertUpdate(adminSession, "CREATE TABLE accessible_base (id BIGINT, value BIGINT)");
        assertUpdate(adminSession, "INSERT INTO accessible_base VALUES (1, 100), (2, 200)", 2);

        assertUpdate(adminSession,
                "CREATE MATERIALIZED VIEW mv_no_access " +
                "SECURITY DEFINER AS " +
                "SELECT id, value FROM accessible_base");

        try {
            getQueryRunner().getAccessControl().deny(privilege("restricted_user", "mv_no_access", SELECT_COLUMN));

            assertQuery(adminSession, "SELECT COUNT(*) FROM mv_no_access", "SELECT 2");

            assertQueryFails(restrictedSession, "SELECT COUNT(*) FROM mv_no_access",
                    ".*Access Denied.*");

            assertUpdate(adminSession, "DROP MATERIALIZED VIEW mv_no_access");
            assertUpdate(adminSession, "DROP TABLE accessible_base");
        }
        finally {
            getQueryRunner().getAccessControl().reset();
        }
    }

    @Test
    public void testSecurityInvokerWithRowFiltersAlwaysTreatedAsStale()
    {
        Session adminSession = createSessionForUser("admin");
        Session restrictedSession = createSessionForUser("restricted_user");

        assertUpdate(adminSession, "CREATE TABLE row_filter_base (id BIGINT, user_id BIGINT, value VARCHAR)");
        assertUpdate(adminSession, "INSERT INTO row_filter_base VALUES (1, 1, 'user1_data'), (2, 2, 'user2_data'), (3, 1, 'more_user1')", 3);

        try {
            assertUpdate(adminSession,
                    "CREATE MATERIALIZED VIEW mv_with_row_filters " +
                    "SECURITY INVOKER AS " +
                    "SELECT id, user_id, value FROM row_filter_base");

            assertUpdate(adminSession, "REFRESH MATERIALIZED VIEW mv_with_row_filters", 3);

            // Add row filter on the base table for restricted_user to trigger staleness
            getQueryRunner().getAccessControl().rowFilter(
                    QualifiedObjectName.valueOf("memory.default.row_filter_base"),
                    "restricted_user",
                    new ViewExpression("restricted_user", Optional.empty(), Optional.empty(), "user_id = 999"));

            assertQuery(adminSession, "SELECT COUNT(*) FROM mv_with_row_filters", "SELECT 3");

            // Since the row filter is "user_id = 999" and no data matches, should return 0
            assertQuery(restrictedSession, "SELECT COUNT(*) FROM mv_with_row_filters", "SELECT 0");

            assertUpdate(adminSession, "DROP MATERIALIZED VIEW mv_with_row_filters");
            assertUpdate(adminSession, "DROP TABLE row_filter_base");
        }
        finally {
            getQueryRunner().getAccessControl().reset();
        }
    }

    @Test
    public void testSecurityInvokerWithColumnMasksAlwaysTreatedAsStale()
    {
        Session adminSession = createSessionForUser("admin");
        Session restrictedSession = createSessionForUser("restricted_user");

        assertUpdate(adminSession, "CREATE TABLE column_mask_base (id BIGINT, sensitive_data VARCHAR)");
        assertUpdate(adminSession, "INSERT INTO column_mask_base VALUES (1, 'secret1'), (2, 'secret2'), (3, 'secret3')", 3);

        try {
            assertUpdate(adminSession,
                    "CREATE MATERIALIZED VIEW mv_with_column_masks " +
                    "SECURITY INVOKER AS " +
                    "SELECT id, sensitive_data FROM column_mask_base");

            assertUpdate(adminSession, "REFRESH MATERIALIZED VIEW mv_with_column_masks", 3);

            getQueryRunner().getAccessControl().columnMask(
                    QualifiedObjectName.valueOf("memory.default.column_mask_base"),
                    "sensitive_data",
                    "restricted_user",
                    new ViewExpression("restricted_user", Optional.empty(), Optional.empty(), "'MASKED'"));

            assertQuery(adminSession, "SELECT sensitive_data FROM mv_with_column_masks WHERE id = 1", "SELECT 'secret1'");

            // Uses the view query plan that queries the base table as restricted_user, applying the column mask
            assertQuery(restrictedSession, "SELECT sensitive_data FROM mv_with_column_masks WHERE id = 1", "SELECT 'MASKED'");

            assertUpdate(adminSession, "DROP MATERIALIZED VIEW mv_with_column_masks");
            assertUpdate(adminSession, "DROP TABLE column_mask_base");
        }
        finally {
            getQueryRunner().getAccessControl().reset();
        }
    }

    @Test
    public void testNestedViewsWithDifferentSecurityModes()
    {
        Session adminSession = createSessionForUser("admin");
        Session restrictedSession = createSessionForUser("restricted_user");

        assertUpdate(adminSession, "CREATE TABLE nested_base (id BIGINT, data VARCHAR)");
        assertUpdate(adminSession, "INSERT INTO nested_base VALUES (1, 'data1'), (2, 'data2')", 2);

        try {
            assertUpdate(adminSession, "CREATE VIEW v_inner SECURITY DEFINER AS SELECT * FROM nested_base");

            assertUpdate(adminSession, "CREATE MATERIALIZED VIEW mv_outer SECURITY INVOKER AS SELECT * FROM v_inner");
            assertUpdate(adminSession, "REFRESH MATERIALIZED VIEW mv_outer", 2);

            getQueryRunner().getAccessControl().deny(privilege("restricted_user", "nested_base", SELECT_COLUMN));

            assertQuery(adminSession, "SELECT COUNT(*) FROM mv_outer", "SELECT 2");

            assertQuery(restrictedSession, "SELECT COUNT(*) FROM mv_outer", "SELECT 2");

            assertUpdate(adminSession, "DROP MATERIALIZED VIEW mv_outer");
            assertUpdate(adminSession, "DROP VIEW v_inner");
            assertUpdate(adminSession, "DROP TABLE nested_base");
        }
        finally {
            getQueryRunner().getAccessControl().reset();
        }
    }

    @Test
    public void testConcurrentAccessWithDifferentSecurityContexts()
    {
        Session adminSession = createSessionForUser("admin");
        Session user1Session = createSessionForUser("user1");
        Session user2Session = createSessionForUser("user2");

        assertUpdate(adminSession, "CREATE TABLE concurrent_base (id BIGINT, value VARCHAR)");
        assertUpdate(adminSession, "INSERT INTO concurrent_base VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);

        try {
            assertUpdate(adminSession, "CREATE MATERIALIZED VIEW mv_concurrent SECURITY INVOKER AS SELECT * FROM concurrent_base");
            assertUpdate(adminSession, "REFRESH MATERIALIZED VIEW mv_concurrent", 3);

            getQueryRunner().getAccessControl().deny(privilege("user1", "concurrent_base", SELECT_COLUMN));

            assertQuery(adminSession, "SELECT COUNT(*) FROM mv_concurrent", "SELECT 3");

            assertQueryFails(user1Session, "SELECT COUNT(*) FROM mv_concurrent",
                    ".*Access Denied.*concurrent_base.*");

            assertQuery(user2Session, "SELECT COUNT(*) FROM mv_concurrent", "SELECT 3");

            assertUpdate(adminSession, "DROP MATERIALIZED VIEW mv_concurrent");
            assertUpdate(adminSession, "DROP TABLE concurrent_base");
        }
        finally {
            getQueryRunner().getAccessControl().reset();
        }
    }

    @Test
    public void testDefinerModeWithRowFilters()
    {
        Session adminSession = createSessionForUser("admin");
        Session restrictedSession = createSessionForUser("restricted_user");

        assertUpdate(adminSession, "CREATE TABLE row_filter_base (id BIGINT, owner VARCHAR)");
        assertUpdate(adminSession, "INSERT INTO row_filter_base VALUES (1, 'admin'), (2, 'other'), (3, 'admin')", 3);

        try {
            assertUpdate(adminSession, "CREATE MATERIALIZED VIEW mv_definer_filter SECURITY DEFINER AS SELECT * FROM row_filter_base");
            assertUpdate(adminSession, "REFRESH MATERIALIZED VIEW mv_definer_filter", 3);

            getQueryRunner().getAccessControl().rowFilter(
                    QualifiedObjectName.valueOf("memory.default.row_filter_base"),
                    "restricted_user",
                    new ViewExpression("restricted_user", Optional.empty(), Optional.empty(), "owner = 'restricted_user'"));

            assertQuery(adminSession, "SELECT COUNT(*) FROM mv_definer_filter", "SELECT 3");

            assertQuery(restrictedSession, "SELECT COUNT(*) FROM mv_definer_filter", "SELECT 3");

            assertUpdate(adminSession, "DROP MATERIALIZED VIEW mv_definer_filter");
            assertUpdate(adminSession, "DROP TABLE row_filter_base");
        }
        finally {
            getQueryRunner().getAccessControl().reset();
        }
    }

    @Test
    public void testInvokerModeWithRowFilters()
    {
        Session adminSession = createSessionForUser("admin");
        Session restrictedSession = createSessionForUser("restricted_user");

        assertUpdate(adminSession, "CREATE TABLE row_filter_invoker_base (id BIGINT, data VARCHAR)");
        assertUpdate(adminSession, "INSERT INTO row_filter_invoker_base VALUES (1, 'visible'), (2, 'hidden'), (3, 'visible')", 3);

        try {
            assertUpdate(adminSession, "CREATE MATERIALIZED VIEW mv_invoker_filter SECURITY INVOKER AS SELECT * FROM row_filter_invoker_base");
            assertUpdate(adminSession, "REFRESH MATERIALIZED VIEW mv_invoker_filter", 3);

            getQueryRunner().getAccessControl().rowFilter(
                    QualifiedObjectName.valueOf("memory.default.row_filter_invoker_base"),
                    "restricted_user",
                    new ViewExpression("restricted_user", Optional.empty(), Optional.empty(), "data = 'visible'"));

            assertQuery(adminSession, "SELECT COUNT(*) FROM mv_invoker_filter", "SELECT 3");

            assertQuery(restrictedSession, "SELECT COUNT(*) FROM mv_invoker_filter", "SELECT 2");

            assertUpdate(adminSession, "DROP MATERIALIZED VIEW mv_invoker_filter");
            assertUpdate(adminSession, "DROP TABLE row_filter_invoker_base");
        }
        finally {
            getQueryRunner().getAccessControl().reset();
        }
    }

    private Session createSessionForUser(String user)
    {
        return Session.builder(getSession())
                .setIdentity(new Identity(user, Optional.empty()))
                .build();
    }
}
