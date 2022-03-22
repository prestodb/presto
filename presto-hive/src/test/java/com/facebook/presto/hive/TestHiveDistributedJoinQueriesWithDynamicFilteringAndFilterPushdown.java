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
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestJoinQueries;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.PUSHDOWN_SUBFIELDS_ENABLED;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.BROADCAST;
import static io.airlift.tpch.TpchTable.getTables;

public class TestHiveDistributedJoinQueriesWithDynamicFilteringAndFilterPushdown
        extends AbstractTestJoinQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(getTables());
    }

    @Override
    protected Session getSession()
    {
        return Session.builder(super.getSession())
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "true")
                .setSystemProperty(PUSHDOWN_SUBFIELDS_ENABLED, "true")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .setCatalogSessionProperty(HIVE_CATALOG, PUSHDOWN_FILTER_ENABLED, "true")
                .build();
    }

    @Test
    public void testMixedJoin()
    {
        // Mixed join could produce conjunction dynamic filters, we should be able to extract them out when integrating with filter pushdown
        assertQuery("SELECT * FROM\n" +
                "lineitem l1 LEFT OUTER JOIN part p1\n" +
                "ON l1.orderkey = p1.partkey AND p1.size = 47\n" +
                "INNER JOIN orders o1 ON l1.orderkey = o1.orderkey\n" +
                "AND o1.custkey = 397\n" +
                "LEFT OUTER JOIN part p2\n" +
                "ON p1.name = p2.name AND p1.partkey = p2.partkey\n" +
                "WHERE o1.shippriority = 0");
    }
}
