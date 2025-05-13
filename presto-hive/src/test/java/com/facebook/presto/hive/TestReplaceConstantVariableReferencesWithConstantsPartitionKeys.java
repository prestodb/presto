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
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.REWRITE_EXPRESSION_WITH_CONSTANT_EXPRESSION;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.airlift.tpch.TpchTable.CUSTOMER;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.NATION;
import static io.airlift.tpch.TpchTable.ORDERS;
import static io.airlift.tpch.TpchTable.SUPPLIER;

public class TestReplaceConstantVariableReferencesWithConstantsPartitionKeys
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(
                ImmutableList.of(ORDERS, LINE_ITEM, CUSTOMER, NATION, SUPPLIER),
                ImmutableMap.of(),
                Optional.empty());
    }

    private Session enableOptimization()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(REWRITE_EXPRESSION_WITH_CONSTANT_EXPRESSION, "true")
                .build();
    }

    @Test
    public void testAggregationWithFilterOnPartitionKey()
    {
        try {
            getQueryRunner().execute("CREATE TABLE orders_key_partitioned WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, totalprice, '2020-01-01' as ds FROM orders WHERE orderkey < 1000");

            assertPlan(
                    enableOptimization(),
                    "select avg(totalprice), orderkey, ds from orders_key_partitioned where ds = '2020-01-01' group by ds, orderkey",
                    output(
                            anyTree(
                                    project(
                                            ImmutableMap.of("ds", expression("'2020-01-01'")),
                                            anyTree(
                                                    aggregation(
                                                            singleGroupingSet("orderkey"),
                                                            ImmutableMap.of(Optional.of("average"), functionCall("avg", ImmutableList.of("totalprice"))),
                                                            ImmutableMap.of(),
                                                            Optional.empty(),
                                                            AggregationNode.Step.PARTIAL,
                                                            tableScan("orders_key_partitioned", ImmutableMap.of("totalprice", "totalprice", "orderkey", "orderkey"))))))));
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS orders_key_partitioned");
        }
    }
}
