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
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.airlift.testing.Closeables.closeAllRuntimeException;

public class TestQueryPlansDeterministic
{
    private LocalQueryRunner runner;
    private PlanDeterminismChecker determinismChecker;

    @BeforeClass
    public void setUp()
    {
        runner = createLocalQueryRunner();
        determinismChecker = new PlanDeterminismChecker(runner);
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        closeAllRuntimeException(runner);
        runner = null;
        determinismChecker = null;
    }

    private static LocalQueryRunner createLocalQueryRunner()
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

        return localQueryRunner;
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
