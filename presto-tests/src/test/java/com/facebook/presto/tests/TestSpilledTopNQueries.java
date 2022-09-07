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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.ORDER_BY_SPILL_ENABLED;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_TOTAL_MEMORY_PER_NODE;
import static com.facebook.presto.SystemSessionProperties.TOPN_SPILL_ENABLED;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestSpilledTopNQueries
        extends AbstractTestTopN

{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        HashMap<String, String> spillProperties = new HashMap<>();
        spillProperties.put(TOPN_SPILL_ENABLED, "true");
        return TestDistributedSpilledQueries.createDistributedSpillingQueryRunner(spillProperties);
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        HashMap<String, String> extraSessionProperties = new HashMap<>();
        extraSessionProperties.put(TOPN_SPILL_ENABLED, "false");

        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(SystemSessionProperties.TASK_CONCURRENCY, "1")
                .setSystemProperty(SystemSessionProperties.MAX_DRIVERS_PER_TASK, "1");

        if (extraSessionProperties != null) {
            for (Map.Entry<String, String> e : extraSessionProperties.entrySet()) {
                sessionBuilder.setSystemProperty(e.getKey(), e.getValue());
            }
        }

        ImmutableMap<String, String> extraProperties = ImmutableMap.<String, String>builder()
                .build();

        DistributedQueryRunner queryRunner = new DistributedQueryRunner(sessionBuilder.build(), 1, extraProperties);

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }

    }

    @Test
    public void testGroupedTopN()
    {
        Session actualSession = Session.builder(getSession())
                .setSystemProperty(SystemSessionProperties.QUERY_MAX_MEMORY_PER_NODE, "160B")
                .build();

        Session expectedSession = Session.builder(getSession())
                .setSystemProperty(TOPN_SPILL_ENABLED, "false")
                .build();

        assertQuery(
                actualSession,
                "SELECT * FROM (SELECT " +
                        "custkey, " +
                        "totalprice, " +
                        "ROW_NUMBER() OVER (PARTITION BY custkey order by totalprice) rn " +
                        "from orders) " +
                        "where rn < 3",
                expectedSession,
                "SELECT * FROM (SELECT " +
                        "custkey, " +
                        "totalprice, " +
                        "ROW_NUMBER() OVER (PARTITION BY custkey order by totalprice) rn " +
                        "from orders) " +
                        "where rn < 3");
    }

    @Test
    public void testUngroupedTopN()
    {
        Session actualSession = Session.builder(getSession())
                .setSystemProperty(SystemSessionProperties.QUERY_MAX_MEMORY_PER_NODE, "160B")
                .build();

        Session expectedSession = Session.builder(getSession())
                .setSystemProperty(TOPN_SPILL_ENABLED, "false")
                .build();

        assertQuery(
                actualSession, "SELECT custkey, totalprice from orders ORDER BY totalprice limit 3",
                expectedSession, "SELECT custkey, totalprice from orders ORDER BY totalprice limit 3");
    }



    @Test
    public void testDoesNotSpillTopNWhenDisabled()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(TOPN_SPILL_ENABLED, "false")
                // set this low so that if we ran without spill the query would fail
                .setSystemProperty(QUERY_MAX_TOTAL_MEMORY_PER_NODE, "1B")
                .build();
        assertQueryFails(session,
                "SELECT orderpriority, custkey FROM orders ORDER BY orderpriority  LIMIT 1000", "Query exceeded.*memory limit.*");
    }
}
