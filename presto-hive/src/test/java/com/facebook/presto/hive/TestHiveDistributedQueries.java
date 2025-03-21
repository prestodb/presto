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
import com.facebook.presto.hive.TestHiveEventListenerPlugin.TestingHiveEventListener;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.CTE_MATERIALIZATION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.CTE_PARTITIONING_PROVIDER_CATALOG;
import static com.facebook.presto.SystemSessionProperties.VERBOSE_OPTIMIZER_INFO_ENABLED;
import static com.facebook.presto.sql.tree.ExplainType.Type.LOGICAL;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.tpch.TpchTable.getTables;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestHiveDistributedQueries
        extends AbstractTestDistributedQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(getTables());
    }

    @Override
    protected boolean supportsNotNullColumns()
    {
        return false;
    }

    @Override
    @AfterClass(alwaysRun = true)
    public void close()
            throws Exception
    {
        assertFalse(getQueryRunner().getEventListeners().isEmpty());
        EventListener eventListener = getQueryRunner().getEventListeners().get(0);
        assertTrue(eventListener instanceof TestingHiveEventListener, eventListener.getClass().getName());
        Set<QueryId> runningQueryIds = ((TestingHiveEventListener) eventListener).getRunningQueries();

        if (!runningQueryIds.isEmpty()) {
            // Await query events to propagate and finish
            Thread.sleep(1000);
        }
        assertEquals(
                runningQueryIds,
                ImmutableSet.of(),
                format(
                        "Query completion events not sent for %d queries: %s",
                        runningQueryIds.size(),
                        runningQueryIds.stream().map(QueryId::getId).collect(joining(", "))));
        super.close();
    }

    @Override
    public void testDelete()
    {
        // Hive connector currently does not support row-by-row delete
    }

    @Override
    public void testUpdate()
    {
        // Updates are not supported by the connector
    }

    @Test
    public void testExplainOfCreateTableAs()
    {
        String query = "CREATE TABLE copy_orders AS SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan("EXPLAIN ", query, LOGICAL));
    }

    @Test
    public void testTrackMaterializedCTEs()
    {
        Session materializedSession = Session.builder(getSession())
                .setSystemProperty(VERBOSE_OPTIMIZER_INFO_ENABLED, "true")
                .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "ALL")
                .setSystemProperty(CTE_PARTITIONING_PROVIDER_CATALOG, "hive")
                .build();

        String query = "with tbl as (select * from lineitem), tbl2 as (select * from tbl) select * from tbl, tbl2";
        MaterializedResult materializedResult = computeActual(materializedSession, "explain " + query);
        String explain = (String) getOnlyElement(materializedResult.getOnlyColumnAsSet());

        checkCTEInfo(explain, "tbl", 2, false, true);
        checkCTEInfo(explain, "tbl2", 1, false, true);
    }

    // Hive specific tests should normally go in TestHiveIntegrationSmokeTest
}
