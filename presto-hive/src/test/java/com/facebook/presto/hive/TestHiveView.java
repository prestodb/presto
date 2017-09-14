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

import com.facebook.presto.hive.metastore.BridgingHiveMetastore;
import com.facebook.presto.hive.metastore.InMemoryHiveMetastore;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.tpch.TpchTable;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveQueryRunner.TPCH_SCHEMA;
import static com.facebook.presto.hive.HiveQueryRunner.createSession;
import static com.facebook.presto.hive.security.SqlStandardAccessControl.ADMIN_ROLE_NAME;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.airlift.tpch.TpchTable.ORDERS;
import static org.testng.Assert.assertEquals;

public class TestHiveView
        extends AbstractTestQueryFramework
{
    private static final TestQueryRunnerUtil util = new TestQueryRunnerUtil(ORDERS);

    @SuppressWarnings("unused")
    public TestHiveView()
            throws Exception
    {
        super(() -> util.getQueryRunner());
    }

    @Test
    public void testSelectOnView()
    {
        util.addView("test_hive_view", buildInitialPrivilegeSet("user"));
        assertQuery("SELECT * from test_hive_view", "SELECT * FROM orders");
        assertUpdate("DROP TABLE test_hive_view");
    }

    @Test
    public void testSelectOnViewWithoutPrivilege() throws Exception
    {
        util.addView("test_hiveview_accessdeny", buildInitialPrivilegeSet("user1"));
        assertQueryFails("SELECT * from test_hiveview_accessdeny",
                "Access Denied: Cannot select from view tpch.test_hiveview_accessdeny");
        assertUpdate("DROP TABLE IF EXISTS test_hiveview_accessdeny");
    }

    @Test
    public void testCatalogQuery() throws Exception
    {
        util.addView("test_hiveview_cat1", buildInitialPrivilegeSet("user"));
        util.addIncompatibleView("test_incompat_hiveview1", buildInitialPrivilegeSet("user"));
        computeActual("SELECT c.table_name,\n" +
                "       c.table_schema,\n" +
                "       c.column_name,\n" +
                "       c.data_type,\n" +
                "       'hive' AS catalog,\n" +
                "       CASE WHEN c.is_nullable = 'NO'\n" +
                "            THEN FALSE\n" +
                "            ELSE TRUE\n" +
                "             END AS is_nullable,\n" +
                "       FALSE AS primary_key\n" +
                "  FROM hive.information_schema.columns c\n" +
                " WHERE c.table_schema != 'information_schema'\n" +
                " ORDER BY c.table_name, c.table_schema, c.column_name");
        assertUpdate("DROP TABLE test_hiveview_cat1");
        assertUpdate("DROP TABLE test_incompat_hiveview1");
    }

    @Test
    public void testShowTables() throws Exception
    {
        MaterializedResult results = computeActual("show tables");
        int initTables = results.getRowCount();
        util.addView("test_compat_hiveview", buildInitialPrivilegeSet("user"));
        util.addIncompatibleView("test_incompat_hiveview", buildInitialPrivilegeSet("user"));
        results = computeActual("show tables");
        assertEquals(results.getRowCount(), initTables + 2);
        assertUpdate("DROP TABLE test_compat_hiveview");
        assertUpdate("DROP TABLE test_incompat_hiveview");
    }

    @Test
    public void testShowColumnsOnIncompatibleView() throws Exception
    {
        util.addIncompatibleView("test_incompat_hiveview1", buildInitialPrivilegeSet("user"));
        MaterializedResult results = computeActual("show columns from test_incompat_hiveview1");
        assertEquals(results.getRowCount(), 0);
        assertUpdate("DROP TABLE test_incompat_hiveview1");
    }

    @Test
    public void testSelectOnIncompatibleView() throws Exception
    {
        util.addIncompatibleView("test_incompat_hiveview3", buildInitialPrivilegeSet("user"));
        assertQueryFails("select * from test_incompat_hiveview3", "Table 'tpch.test_incompat_hiveview3' not found");
        assertUpdate("DROP TABLE test_incompat_hiveview3");
    }

    private static class TestQueryRunnerUtil
    {
        DistributedQueryRunner queryRunner;
        InMemoryHiveMetastore metastore;
        private static final DateTimeZone TIME_ZONE = DateTimeZone.forID("Asia/Kathmandu");

        public TestQueryRunnerUtil(TpchTable<?>... tables)
        {
            init(ImmutableList.copyOf(tables));
        }

        public InMemoryHiveMetastore getMetastore()
        {
            return metastore;
        }
        public DistributedQueryRunner getQueryRunner()
        {
            return queryRunner;
        }

        private void init(Iterable<TpchTable<?>> tables)
        {
            try {
                queryRunner = new DistributedQueryRunner(createSession(), 4, ImmutableMap.of());
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data").toFile();
                metastore = new InMemoryHiveMetastore(baseDir);
                metastore.setUserRoles(createSession().getUser(), ImmutableSet.of(ADMIN_ROLE_NAME));
                metastore.createDatabase(createDatabaseMetastoreObject(baseDir, TPCH_SCHEMA));
                queryRunner.installPlugin(new HivePlugin(HIVE_CATALOG, new BridgingHiveMetastore(metastore)));

                metastore.setUserRoles(createSession().getUser(), ImmutableSet.of("admin"));

                Map<String, String> hiveProperties = ImmutableMap.<String, String>builder()
                        .putAll(ImmutableMap.of())
                        .put("hive.metastore.uri", "thrift://localhost:8080")
                        .put("hive.time-zone", TIME_ZONE.getID())
                        .put("hive.security", "sql-standard")
                        .build();
                queryRunner.createCatalog(HIVE_CATALOG, HIVE_CATALOG, hiveProperties);

                copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);
            }
            catch (Exception e) {
                queryRunner.close();
            }
        }
        protected static Database createDatabaseMetastoreObject(File baseDir, String name)
        {
            Database database = new Database(name, null, new File(baseDir, name).toURI().toString(), null);
            database.setOwnerName("public");
            database.setOwnerType(PrincipalType.ROLE);
            return database;
        }
        public void addView(String name, PrincipalPrivilegeSet privSet)
        {
            metastore.createTable(toMetastoreApiView("tpch", name, "user",
                    "select * from orders", privSet));
        }
        public void addIncompatibleView(String name, PrincipalPrivilegeSet privSet)
        {
            metastore.createTable(toMetastoreApiView("tpch", name, "user",
                    "select greatest(shippriority, orderpriority) from orders", privSet));
        }
        private static org.apache.hadoop.hive.metastore.api.Table toMetastoreApiView(String databaseName, String viewName, String owner, String sql, PrincipalPrivilegeSet privileges)
        {
            org.apache.hadoop.hive.metastore.api.Table result =
                    new org.apache.hadoop.hive.ql.metadata.Table(databaseName, viewName).getTTable();
            result.setOwner(owner);
            result.setTableType(TableType.VIRTUAL_VIEW.name());
            result.setParameters(ImmutableMap.of());
            result.setPrivileges(privileges);
            result.setViewOriginalText(sql);
            result.setViewExpandedText(sql);
            return result;
        }
    }
    private static PrincipalPrivilegeSet buildInitialPrivilegeSet(String tableOwner)
    {
        return new PrincipalPrivilegeSet(ImmutableMap.of(tableOwner, ImmutableList.of(
            new PrivilegeGrantInfo("SELECT", 0, tableOwner, PrincipalType.USER, true),
            new PrivilegeGrantInfo("INSERT", 0, tableOwner, PrincipalType.USER, true),
            new PrivilegeGrantInfo("UPDATE", 0, tableOwner, PrincipalType.USER, true),
            new PrivilegeGrantInfo("DELETE", 0, tableOwner, PrincipalType.USER, true))),
            ImmutableMap.of(), ImmutableMap.of());
    }
}
