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
package com.facebook.presto.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_DYNAMIC_FILTER_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static java.lang.String.format;

/**
 * Plan-level tests verifying optimizer produces dynamic filter assignments on join nodes.
 */
@Test(singleThreaded = true)
public class TestDynamicPartitionPruningPlan
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(HIVE)
                .build()
                .getQueryRunner();
    }

    @BeforeClass
    public void setupTestTables()
    {
        assertUpdate("CREATE TABLE plan_fact_orders (" +
                "order_id BIGINT, " +
                "customer_id BIGINT, " +
                "amount DECIMAL(10, 2), " +
                "order_date DATE" +
                ") WITH (partitioning = ARRAY['customer_id'])");

        for (int customerId = 1; customerId <= 3; customerId++) {
            assertUpdate(format(
                    "INSERT INTO plan_fact_orders VALUES (%d, %d, DECIMAL '100.00', DATE '2024-01-01')",
                    customerId, customerId), 1);
        }

        assertUpdate("CREATE TABLE plan_dim_customers (" +
                "customer_id BIGINT, " +
                "customer_name VARCHAR, " +
                "region VARCHAR)");

        assertUpdate(
                "INSERT INTO plan_dim_customers VALUES " +
                        "(1, 'Alice', 'WEST'), " +
                        "(2, 'Bob', 'WEST'), " +
                        "(3, 'Carol', 'EAST')", 3);
    }

    @AfterClass(alwaysRun = true)
    public void cleanupTestTables()
    {
        assertUpdate("DROP TABLE IF EXISTS plan_fact_orders");
        assertUpdate("DROP TABLE IF EXISTS plan_dim_customers");
    }

    @Test
    public void testJoinPlanHasDynamicFilterAssignments()
    {
        assertPlan(dppSession(),
                "SELECT f.order_id " +
                        "FROM plan_fact_orders f " +
                        "JOIN plan_dim_customers c ON f.customer_id = c.customer_id " +
                        "WHERE c.region = 'WEST'",
                output(
                        exchange(
                                join(
                                        INNER,
                                        ImmutableList.of(equiJoinClause("FACT_CID", "DIM_CID")),
                                        ImmutableMap.of("FACT_CID", "DIM_CID"),
                                        Optional.empty(),
                                        tableScan("plan_fact_orders", ImmutableMap.of("FACT_CID", "customer_id")),
                                        exchange(
                                                exchange(
                                                        project(
                                                                filter("DIM_REGION = 'WEST'",
                                                                        tableScan("plan_dim_customers", ImmutableMap.of(
                                                                                "DIM_CID", "customer_id",
                                                                                "DIM_REGION", "region"))))))))));
    }

    @Test
    public void testCostBasedJoinPlanProducesValidResults()
    {
        assertQuery(costBasedDppSession(),
                "SELECT f.order_id " +
                        "FROM plan_fact_orders f " +
                        "JOIN plan_dim_customers c ON f.customer_id = c.customer_id " +
                        "WHERE c.region = 'WEST'",
                "VALUES 1, 2");
    }

    @Test
    public void testCostBasedNoWhereClauseProducesCorrectResults()
    {
        assertQuery(costBasedDppSession(),
                "SELECT f.order_id " +
                        "FROM plan_fact_orders f " +
                        "JOIN plan_dim_customers c ON f.customer_id = c.customer_id",
                "VALUES 1, 2, 3");
    }

    private Session dppSession()
    {
        return Session.builder(getSession())
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "true")
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "ELIMINATE_CROSS_JOINS")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .build();
    }

    private Session costBasedDppSession()
    {
        return Session.builder(getSession())
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "COST_BASED")
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "ELIMINATE_CROSS_JOINS")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .build();
    }
}
