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
package com.facebook.presto.spark;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.CTE_MATERIALIZATION_STRATEGY;
import static com.facebook.presto.spark.PrestoSparkQueryRunner.createHivePrestoSparkQueryRunner;
import static io.airlift.tpch.TpchTable.getTables;

/**
 * Presto on Spark cannot run a materialized CTE, so {@code cte_materialization_strategy} has to be
 * a no-op there rather than a query failure.
 */
public class TestPrestoSparkCteMaterialization
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
    {
        return createHivePrestoSparkQueryRunner(
                getTables(),
                ImmutableMap.of("query.cte-partitioning-provider-catalog", "hive"),
                ImmutableMap.of("hive.temporary-table-storage-format", "PAGEFILE"),
                Optional.empty());
    }

    private Session materializedCteSession()
    {
        return Session.builder(getSession())
                .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "ALL")
                .build();
    }

    @Test
    public void testCteMaterializationStrategyAllIsIgnored()
    {
        assertQuery(
                materializedCteSession(),
                "WITH t AS (SELECT orderkey, custkey FROM orders) " +
                        "SELECT count(*) FROM t a JOIN t b ON a.orderkey = b.orderkey");
    }

    @Test
    public void testMaterializedCteInsertIsIgnored()
    {
        try {
            assertQuerySucceeds(
                    materializedCteSession(),
                    "CREATE TABLE hive.tpch.test_spark_materialized_cte AS " +
                            "WITH t AS (SELECT orderkey, custkey FROM orders) " +
                            "SELECT a.orderkey, a.custkey FROM t a JOIN t b ON a.orderkey = b.orderkey");
            assertQuery(
                    "SELECT count(*) FROM hive.tpch.test_spark_materialized_cte",
                    "SELECT count(*) FROM orders");
        }
        finally {
            assertQuerySucceeds("DROP TABLE IF EXISTS hive.tpch.test_spark_materialized_cte");
        }
    }
}
