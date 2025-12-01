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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.testing.Closeables.closeAllSuppress;
import static com.facebook.presto.SystemSessionProperties.INEQUALITY_JOIN_PUSHDOWN_ENABLED;
import static com.facebook.presto.SystemSessionProperties.INNER_JOIN_PUSHDOWN_ENABLED;
import static com.facebook.presto.plugin.jdbc.JdbcQueryRunner.createSchema;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;

public class TestJoinQueriesWithPushDown
        extends AbstractTestQueryFramework
{
    private static final String TPCH_SCHEMA = "tpch";
    private static final String JDBC = "jdbc";
    private static Session pushdownDisabledSession;

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        pushdownDisabledSession = testSessionBuilder()
                .setCatalog(JDBC)
                .setSchema(TPCH_SCHEMA)
                .build();

        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = new DistributedQueryRunner(pushdownDisabledSession, 3);

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            Map<String, String> properties = TestingH2JdbcModule.createProperties();
            createSchema(properties, "tpch");

            queryRunner.installPlugin(new JdbcPlugin("base-jdbc", new TestingH2JdbcModule()));
            queryRunner.createCatalog(JDBC, "base-jdbc", properties);

            copyTpchTables(queryRunner, "tpch", "\"sf0.001\"", pushdownDisabledSession, ImmutableList.copyOf(TpchTable.getTables()));

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Test
    public void testSimpleInnerEquiJoin()
    {
        assertQueryWithSameQueryRunner(getSession(), "SELECT o.orderkey, o.totalprice, c.name " +
                "FROM orders o " +
                "JOIN customer c ON o.custkey = c.custkey " +
                "WHERE o.totalprice > 50000 " +
                "ORDER BY o.totalprice DESC", pushdownDisabledSession);
    }

    @Test
    public void testInnerEquiJoinWithFilter()
    {
        assertQueryWithSameQueryRunner(getSession(), "SELECT o.orderkey, o.totalprice, c.name " +
                "FROM orders o " +
                "JOIN customer c ON o.custkey = c.custkey " +
                "WHERE o.totalprice > 50000 " +
                "ORDER BY o.totalprice DESC", pushdownDisabledSession);
    }

    @Test
    public void testSimpleInnerNonEquiJoin()
    {
        assertQueryWithSameQueryRunner(getSession(), "SELECT o.orderkey, c.name, o.totalprice, c.custkey " +
                "FROM orders o " +
                "JOIN customer c ON o.custkey = c.custkey ", pushdownDisabledSession);
    }

    @Test
    public void testInnerNonEquiJoinWithFilter()
    {
        assertQueryWithSameQueryRunner(getSession(), "SELECT o.orderkey, c.name, o.totalprice, c.custkey " +
                "FROM orders o " +
                "JOIN customer c ON o.custkey = c.custkey " +
                "WHERE o.totalprice < c.acctbal / 2", pushdownDisabledSession);
    }

    @Override
    protected Session getSession()
    {
        return Session.builder(super.getSession())
                .setSystemProperty(INNER_JOIN_PUSHDOWN_ENABLED, "true")
                .setSystemProperty(INEQUALITY_JOIN_PUSHDOWN_ENABLED, "true")
                .build();
    }
}
