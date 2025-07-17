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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.scalar.sql.SqlInvokedFunctionsPlugin;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.testing.TestingSession.TESTING_CATALOG;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestQueryPlanDeterminism
        extends AbstractTestQueries
{
    private PlanDeterminismChecker determinismChecker;

    @BeforeClass
    public void setUp()
    {
        determinismChecker = new PlanDeterminismChecker((LocalQueryRunner) getQueryRunner());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        determinismChecker = null;
    }

    @Override
    protected QueryRunner createQueryRunner()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        LocalQueryRunner localQueryRunner = new LocalQueryRunner(defaultSession);

        // add the tpch catalog
        // local queries run directly against the generator
        localQueryRunner.createCatalog(
                defaultSession.getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());

        localQueryRunner.getMetadata().registerBuiltInFunctions(CUSTOM_FUNCTIONS);

        SessionPropertyManager sessionPropertyManager = localQueryRunner.getMetadata().getSessionPropertyManager();
        sessionPropertyManager.addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
        sessionPropertyManager.addConnectorSessionProperties(new ConnectorId(TESTING_CATALOG), TEST_CATALOG_PROPERTIES);

        localQueryRunner.installPlugin(new SqlInvokedFunctionsPlugin());
        return localQueryRunner;
    }

    @Override
    protected MaterializedResult computeActual(@Language("SQL") String sql)
    {
        determinismChecker.checkPlanIsDeterministic(sql);
        return super.computeActual(sql);
    }

    @Override
    protected MaterializedResult computeActual(Session session, @Language("SQL") String sql)
    {
        determinismChecker.checkPlanIsDeterministic(session, sql);
        return super.computeActual(session, sql);
    }

    @Override
    protected void assertQuery(@Language("SQL") String sql)
    {
        determinismChecker.checkPlanIsDeterministic(sql);
    }

    @Override
    protected void assertQuery(Session session, @Language("SQL") String sql)
    {
        determinismChecker.checkPlanIsDeterministic(session, sql);
    }

    @Override
    public void assertQueryOrdered(@Language("SQL") String sql)
    {
        determinismChecker.checkPlanIsDeterministic(sql);
    }

    @Override
    protected void assertQuery(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        determinismChecker.checkPlanIsDeterministic(actual);
    }

    @Override
    protected void assertQuery(Session session, @Language("SQL") String actual, @Language("SQL") String expected)
    {
        determinismChecker.checkPlanIsDeterministic(session, actual);
    }

    @Override
    protected void assertQueryOrdered(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        determinismChecker.checkPlanIsDeterministic(actual);
    }

    @Override
    protected void assertQueryOrdered(Session session, @Language("SQL") String actual, @Language("SQL") String expected)
    {
        determinismChecker.checkPlanIsDeterministic(session, actual);
    }

    @Override
    protected void assertUpdate(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        determinismChecker.checkPlanIsDeterministic(actual);
    }

    @Override
    protected void assertUpdate(Session session, @Language("SQL") String actual, @Language("SQL") String expected)
    {
        determinismChecker.checkPlanIsDeterministic(session, actual);
    }

    @Override
    protected void assertUpdate(@Language("SQL") String sql)
    {
        determinismChecker.checkPlanIsDeterministic(sql);
    }

    @Override
    protected void assertUpdate(Session session, @Language("SQL") String sql)
    {
        determinismChecker.checkPlanIsDeterministic(session, sql);
    }

    @Override
    protected void assertUpdate(@Language("SQL") String sql, long count)
    {
        determinismChecker.checkPlanIsDeterministic(sql);
    }

    @Override
    protected void assertUpdate(Session session, @Language("SQL") String sql, long count)
    {
        determinismChecker.checkPlanIsDeterministic(session, sql);
    }

    @Override
    protected void assertQueryFails(@Language("SQL") String sql, @Language("RegExp") String expectedMessageRegExp)
    {
        super.assertQueryFails(sql, expectedMessageRegExp);
    }

    @Override
    protected void assertQueryFails(Session session, @Language("SQL") String sql, @Language("RegExp") String expectedMessageRegExp)
    {
        super.assertQueryFails(session, sql, expectedMessageRegExp);
    }

    @Override
    public void testExplainValidate()
    {
    }

    @Override
    public void testSetSessionNativeWorkerSessionProperty()
    {
    }

    @Override
    protected void assertAccessAllowed(@Language("SQL") String sql, TestingAccessControlManager.TestingPrivilege... deniedPrivileges)
    {
    }

    @Override
    protected void assertAccessAllowed(Session session, @Language("SQL") String sql, TestingAccessControlManager.TestingPrivilege... deniedPrivileges)
    {
    }

    @Override
    protected void assertAccessDenied(@Language("SQL") String sql, @Language("RegExp") String exceptionsMessageRegExp, TestingAccessControlManager.TestingPrivilege... deniedPrivileges)
    {
    }

    @Override
    protected void assertAccessDenied(Session session, @Language("SQL") String sql, @Language("RegExp") String exceptionsMessageRegExp, TestingAccessControlManager.TestingPrivilege... deniedPrivileges)
    {
    }

    @Override
    protected void assertTableColumnNames(String tableName, String... columnNames)
    {
    }

    @Test
    public void testUse()
    {
        // USE statement is not supported
    }

    @Override
    protected MaterializedResult computeExpected(@Language("SQL") String sql, List<? extends Type> resultTypes)
    {
        determinismChecker.checkPlanIsDeterministic(sql);
        return super.computeExpected(sql, resultTypes);
    }

    // Give more time for large queries in plan determinsm check.
    @Override
    @Test(timeOut = 100_000)
    public void testLargeQuery()
    {
        super.testLargeQuery();
    }

    @Test
    public void testTpchQ9deterministic()
    {
        //This uses a modified version of TPC-H Q9, because the tpch connector uses non-standard column names
        determinismChecker.checkPlanIsDeterministic("SELECT\n" +
                "  nation,\n" +
                "  o_year,\n" +
                "  sum(amount) AS sum_profit\n" +
                "FROM (\n" +
                "       SELECT\n" +
                "         n.name                                                          AS nation,\n" +
                "         extract(YEAR FROM o.orderdate)                                  AS o_year,\n" +
                "         l.extendedprice * (1 - l.discount) - ps.supplycost * l.quantity AS amount\n" +
                "       FROM\n" +
                "         part p,\n" +
                "         supplier s,\n" +
                "         lineitem l,\n" +
                "         partsupp ps,\n" +
                "         orders o,\n" +
                "         nation n\n" +
                "       WHERE\n" +
                "         s.suppkey = l.suppkey\n" +
                "         AND ps.suppkey = l.suppkey\n" +
                "         AND ps.partkey = l.partkey\n" +
                "         AND p.partkey = l.partkey\n" +
                "         AND o.orderkey = l.orderkey\n" +
                "         AND s.nationkey = n.nationkey\n" +
                "         AND p.name LIKE '%green%'\n" +
                "     ) AS profit\n" +
                "GROUP BY\n" +
                "  nation,\n" +
                "  o_year\n" +
                "ORDER BY\n" +
                "  nation,\n" +
                "  o_year DESC\n");
    }

    @Test
    public void testTpcdsQ6deterministic()
    {
        //This is a query inspired on TPC-DS Q6 that reproduces its plan nondeterminism problems
        determinismChecker.checkPlanIsDeterministic("SELECT orderdate " +
                "FROM orders o,\n" +
                "     lineitem i\n" +
                "WHERE o.orderdate =\n" +
                "    (SELECT DISTINCT (orderdate)\n" +
                "     FROM orders\n" +
                "     WHERE totalprice > 2)\n" +
                "  AND i.quantity > 1.2 *\n" +
                "    (SELECT avg(j.quantity)\n" +
                "     FROM lineitem j\n" +
                "    )\n");
    }
}
