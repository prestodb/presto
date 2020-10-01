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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableSample;
import com.facebook.presto.sql.analyzer.FeaturesConfig.ApproxResultsOption;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.APPROX_RESULTS_OPTION;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anySymbol;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.lateral;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.union;

public class TestAutoSampleTableReplace
        extends BasePlanTest
{
    @Test
    public void testScale()
    {
        assertPlanWithSamples("select count(orderkey) from orders",
                output(
                        project(ImmutableMap.of("count", expression("(\"sample_count\" * CAST(20 AS bigint))")),
                            aggregation(ImmutableMap.of("sample_count", functionCall("count", false, ImmutableList.of(anySymbol()))),
                                tableScan("orders", true)))),
                Collections.singletonList("orders"));
        assertPlanWithSamples("select count(distinct orderkey) from orders",
                output(
                        project(ImmutableMap.of("count", expression("(\"sample_count\" * CAST(20 as bigint))")),
                                aggregation(ImmutableMap.of("sample_count", functionCall("count", true, ImmutableList.of(anySymbol()))),
                                        tableScan("orders", true)))),
                Collections.singletonList("orders"));

        assertPlanWithSamples("select orderkey, count(*) from orders group by 1",
                output(
                        project(ImmutableMap.of("count", expression("(\"sample_count\" * CAST(20 as bigint))")),
                                aggregation(ImmutableMap.of("sample_count", functionCall("count", false, ImmutableList.of())),
                                        tableScan("orders", true)))),
                Collections.singletonList("orders"));

        assertPlanWithSamples("select count_if(totalprice > 10.0) from orders",
                output(
                        project(ImmutableMap.of("count_if", expression("(\"sample_count_if\" * CAST(20 as bigint))")),
                                aggregation(ImmutableMap.of("sample_count_if", functionCall("count_if", false, ImmutableList.of(anySymbol()))),
                                        project(
                                                tableScan("orders", true))))),
                Collections.singletonList("orders"));

        assertPlanWithSamples("select count(*) from orders",
                output(
                        project(ImmutableMap.of("count", expression("(\"sample_count\" * CAST(20 as bigint))")),
                                aggregation(ImmutableMap.of("sample_count", functionCall("count", false, ImmutableList.of())),
                                        tableScan("orders", true)))),
                Collections.singletonList("orders"));
    }

    @Test
    public void testLateral()
    {
        String sql = "SELECT count(name)\n" +
                "FROM customer\n" +
                "WHERE nationkey = (\n" +
                "  SELECT nationkey FROM nation WHERE name like 'A%'\n" +
                ")";

        assertPlanWithSamples(sql,
                output(
                    project(ImmutableMap.of("count", expression("(\"sample_count\" * CAST(20 as bigint))")),
                            aggregation(ImmutableMap.of("sample_count", functionCall("count", false, ImmutableList.of(anySymbol()))),
                                    anyTree(
                                            lateral(
                                                    ImmutableList.of(),
                                                    tableScan("customer", true),
                                                    anyTree(
                                                            tableScan("nation"))))))),
                Collections.singletonList("customer"));

        assertPlanWithSamples(sql,
                output(
                        project(ImmutableMap.of("count", expression("(\"sample_count\" * CAST(20 as bigint))")),
                                aggregation(ImmutableMap.of("sample_count", functionCall("count", false, ImmutableList.of(anySymbol()))),
                                        anyTree(
                                                lateral(
                                                        ImmutableList.of(),
                                                        tableScan("customer"),
                                                        anyTree(
                                                                tableScan("nation", true))))))),
                Collections.singletonList("nation"));

        // Both tables cannot be sampled
        assertPlanWithSamples(sql,
                output(
                        aggregation(ImmutableMap.of("sample_count", functionCall("count", false, ImmutableList.of(anySymbol()))),
                                anyTree(
                                        lateral(
                                                ImmutableList.of(),
                                                tableScan("customer"),
                                                anyTree(
                                                        tableScan("nation")))))),
                Arrays.asList("customer", "nation"),
                true);
    }

    @Test
    public void testInnerJoin()
    {
        // Scale sampled data
        String sql = "SELECT count(customer.name)\n" +
                "FROM customer\n" +
                "INNER JOIN nation on customer.nationkey = nation.nationkey and\n" +
                "nation.name = 'ALGERIA'";

        assertPlanWithSamples(sql,
                output(
                        project(ImmutableMap.of("count", expression("(\"sample_count\" * CAST(20 as bigint))")),
                                aggregation(ImmutableMap.of("sample_count", functionCall("count", false, ImmutableList.of(anySymbol()))),
                                        anyTree(
                                                join(
                                                        JoinNode.Type.INNER,
                                                        ImmutableList.of(equiJoinClause("cn", "nn")),
                                                        tableScan("customer", ImmutableMap.of("cn", "nationkey"), true),
                                                        tableScan("nation", ImmutableMap.of("nn", "nationkey"))))))),
                Collections.singletonList("customer"));

        // Scale non sampled data too in case of inner join
        sql = "SELECT count(nation.name)\n" +
                "FROM customer\n" +
                "INNER JOIN nation on customer.nationkey = nation.nationkey and\n" +
                "nation.name like 'A%'";
        assertPlanWithSamples(sql,
                output(
                        project(ImmutableMap.of("count", expression("(\"sample_count\" * CAST(20 as bigint))")),
                                aggregation(ImmutableMap.of("sample_count", functionCall("count", false, ImmutableList.of(anySymbol()))),
                                        anyTree(
                                                join(
                                                        JoinNode.Type.INNER,
                                                        ImmutableList.of(equiJoinClause("cn", "nn")),
                                                        tableScan("customer", ImmutableMap.of("cn", "nationkey"), true),
                                                        tableScan("nation", ImmutableMap.of("nn", "nationkey"))))))),
                Collections.singletonList("customer"));

        sql = "SELECT count(*)\n" +
                "FROM customer\n" +
                "INNER JOIN nation on customer.nationkey = nation.nationkey and\n" +
                "nation.name = 'ALGERIA'";
        assertPlanWithSamples(sql,
                output(
                        project(ImmutableMap.of("count", expression("(\"sample_count\" * CAST(20 as bigint))")),
                                aggregation(ImmutableMap.of("sample_count", functionCall("count", false, ImmutableList.of())),
                                        anyTree(
                                                join(
                                                        JoinNode.Type.INNER,
                                                        ImmutableList.of(equiJoinClause("cn", "nn")),
                                                        tableScan("customer", ImmutableMap.of("cn", "nationkey"), true),
                                                        tableScan("nation", ImmutableMap.of("nn", "nationkey"))))))),
                Collections.singletonList("customer"));
    }

    @Test
    public void testLeftJoin()
    {
        // Scale sampled data for left join between sampled and non sampled
        String sql = "SELECT count(customer.name)\n" +
                "FROM customer\n" +
                "LEFT JOIN nation on customer.nationkey = nation.nationkey";

        assertPlanWithSamples(sql,
                output(
                        project(ImmutableMap.of("count", expression("(\"sample_count\" * CAST(20 as bigint))")),
                                aggregation(ImmutableMap.of("sample_count", functionCall("count", false, ImmutableList.of(anySymbol()))),
                                        join(
                                                JoinNode.Type.LEFT,
                                                ImmutableList.of(equiJoinClause("cn", "nn")),
                                                tableScan("customer", ImmutableMap.of("cn", "nationkey"), true),
                                                tableScan("nation", ImmutableMap.of("nn", "nationkey")))))),
                Collections.singletonList("customer"));

        // Scale non-sampled data for left join between sampled and non sampled since the non-sampled effectively becomes sampled
        sql = "SELECT count(nation.name)\n" +
                "FROM customer\n" +
                "LEFT JOIN nation on customer.nationkey = nation.nationkey";

        assertPlanWithSamples(sql,
                output(
                        project(ImmutableMap.of("count", expression("(\"sample_count\" * CAST(20 as bigint))")),
                                aggregation(ImmutableMap.of("sample_count", functionCall("count", false, ImmutableList.of(anySymbol()))),
                                        join(
                                                JoinNode.Type.LEFT,
                                                ImmutableList.of(equiJoinClause("cn", "nn")),
                                                tableScan("customer", ImmutableMap.of("cn", "nationkey"), true),
                                                tableScan("nation", ImmutableMap.of("nn", "nationkey")))))),
                Collections.singletonList("customer"));

        // Don't scale from left join between non-sampled and sampled
        sql = "SELECT count(customer.name)\n" +
                "FROM customer\n" +
                "LEFT JOIN nation on customer.nationkey = nation.nationkey";
        assertPlanWithSamples(sql,
                output(
                        aggregation(ImmutableMap.of("count", functionCall("count", false, ImmutableList.of(anySymbol()))),
                                join(
                                        JoinNode.Type.LEFT,
                                        ImmutableList.of(equiJoinClause("cn", "nn")),
                                        tableScan("customer", ImmutableMap.of("cn", "nationkey")),
                                        tableScan("nation", ImmutableMap.of("nn", "nationkey"), true)))),
                Collections.singletonList("nation"));

        // count * will or will not get aggregated based on whether the left table is sampled or not
        sql = "SELECT count(*)\n" +
                "FROM customer\n" +
                "LEFT JOIN nation on customer.nationkey = nation.nationkey";

        assertPlanWithSamples(sql,
                output(
                        project(ImmutableMap.of("count", expression("(\"sample_count\" * CAST(20 as bigint))")),
                                aggregation(ImmutableMap.of("sample_count", functionCall("count", false, ImmutableList.of())),
                                        join(
                                                JoinNode.Type.LEFT,
                                                ImmutableList.of(equiJoinClause("cn", "nn")),
                                                tableScan("customer", ImmutableMap.of("cn", "nationkey"), true),
                                                tableScan("nation", ImmutableMap.of("nn", "nationkey")))))),
                Collections.singletonList("customer"));

        assertPlanWithSamples(sql,
                output(
                        aggregation(ImmutableMap.of("count", functionCall("count", false, ImmutableList.of())),
                                join(
                                        JoinNode.Type.LEFT,
                                        ImmutableList.of(equiJoinClause("cn", "nn")),
                                        tableScan("customer", ImmutableMap.of("cn", "nationkey")),
                                        tableScan("nation", ImmutableMap.of("nn", "nationkey"), true)))),
                Collections.singletonList("nation"));

        // Scale the sampled data on left join from non-sampled to sampled
        sql = "SELECT count(nation.name)\n" +
                "FROM customer\n" +
                "LEFT JOIN nation on customer.nationkey = nation.nationkey";
        assertPlanWithSamples(sql,
                output(
                        project(ImmutableMap.of("count", expression("(\"sample_count\" * CAST(20 as bigint))")),
                                aggregation(ImmutableMap.of("sample_count", functionCall("count", false, ImmutableList.of(anySymbol()))),
                                        join(
                                                JoinNode.Type.LEFT,
                                                ImmutableList.of(equiJoinClause("cn", "nn")),
                                                tableScan("customer", ImmutableMap.of("cn", "nationkey")),
                                                tableScan("nation", ImmutableMap.of("nn", "nationkey"), true))))),
                Collections.singletonList("nation"));
    }

    @Test
    public void testRightJoin()
    {
        // Scale sampled data for right join between sampled and non sampled
        String sql = "SELECT count(customer.name)\n" +
                "FROM customer\n" +
                "RIGHT JOIN nation on customer.nationkey = nation.nationkey";

        assertPlanWithSamples(sql,
                output(
                        project(ImmutableMap.of("count", expression("(\"sample_count\" * CAST(20 as bigint))")),
                                aggregation(ImmutableMap.of("sample_count", functionCall("count", false, ImmutableList.of(anySymbol()))),
                                        join(
                                                JoinNode.Type.RIGHT,
                                                ImmutableList.of(equiJoinClause("cn", "nn")),
                                                tableScan("customer", ImmutableMap.of("cn", "nationkey"), true),
                                                tableScan("nation", ImmutableMap.of("nn", "nationkey")))))),
                Collections.singletonList("customer"));

        // Don't scale non-sampled data on right join between sampled and non sampled
        sql = "SELECT count(nation.name)\n" +
                "FROM customer\n" +
                "RIGHT JOIN nation on customer.nationkey = nation.nationkey";

        assertPlanWithSamples(sql,
                output(
                        aggregation(ImmutableMap.of("count", functionCall("count", false, ImmutableList.of(anySymbol()))),
                                join(
                                        JoinNode.Type.RIGHT,
                                        ImmutableList.of(equiJoinClause("cn", "nn")),
                                        tableScan("customer", ImmutableMap.of("cn", "nationkey"), true),
                                        tableScan("nation", ImmutableMap.of("nn", "nationkey"))))),
                Collections.singletonList("customer"));

        // Count * will or will not get aggregated depending on whether the right table is sampled or not
        sql = "SELECT count(*)\n" +
                "FROM customer\n" +
                "RIGHT JOIN nation on customer.nationkey = nation.nationkey";

        assertPlanWithSamples(sql,
                output(
                        aggregation(ImmutableMap.of("count", functionCall("count", false, ImmutableList.of())),
                                join(
                                        JoinNode.Type.RIGHT,
                                        ImmutableList.of(equiJoinClause("cn", "nn")),
                                        tableScan("customer", ImmutableMap.of("cn", "nationkey"), true),
                                        tableScan("nation", ImmutableMap.of("nn", "nationkey"))))),
                Collections.singletonList("customer"));

        assertPlanWithSamples(sql,
                output(
                        project(ImmutableMap.of("count", expression("(\"sample_count\" * CAST(20 as bigint))")),
                                aggregation(ImmutableMap.of("sample_count", functionCall("count", false, ImmutableList.of())),
                                        join(
                                                JoinNode.Type.RIGHT,
                                                ImmutableList.of(equiJoinClause("cn", "nn")),
                                                tableScan("customer", ImmutableMap.of("cn", "nationkey")),
                                                tableScan("nation", ImmutableMap.of("nn", "nationkey"), true))))),
                Collections.singletonList("nation"));

        // Scale the non sampled data on right join between sampled and non sampled
        sql = "SELECT count(customer.name)\n" +
                "FROM customer\n" +
                "RIGHT JOIN nation on customer.nationkey = nation.nationkey";
        assertPlanWithSamples(sql,
                output(
                        project(ImmutableMap.of("count", expression("(\"sample_count\" * CAST(20 as bigint))")),
                            aggregation(ImmutableMap.of("sample_count", functionCall("count", false, ImmutableList.of(anySymbol()))),
                                    join(
                                            JoinNode.Type.RIGHT,
                                            ImmutableList.of(equiJoinClause("cn", "nn")),
                                            tableScan("customer", ImmutableMap.of("cn", "nationkey")),
                                            tableScan("nation", ImmutableMap.of("nn", "nationkey"), true))))),
                Collections.singletonList("nation"));

        // Scale the sampled data on right join between sampled and non sampled
        sql = "SELECT count(nation.name)\n" +
                "FROM customer\n" +
                "RIGHT JOIN nation on customer.nationkey = nation.nationkey";
        assertPlanWithSamples(sql,
                output(
                        project(ImmutableMap.of("count", expression("(\"sample_count\" * CAST(20 as bigint))")),
                                aggregation(ImmutableMap.of("sample_count", functionCall("count", false, ImmutableList.of(anySymbol()))),
                                        join(
                                                JoinNode.Type.RIGHT,
                                                ImmutableList.of(equiJoinClause("cn", "nn")),
                                                tableScan("customer", ImmutableMap.of("cn", "nationkey")),
                                                tableScan("nation", ImmutableMap.of("nn", "nationkey"), true))))),
                Collections.singletonList("nation"));
    }

    @Test
    public void testFullJoin()
    {
        // Only the sampled dataset will get scaled
        String sql = "SELECT count(customer.name)\n" +
                "FROM customer\n" +
                "FULL JOIN nation on customer.nationkey = nation.nationkey";

        assertPlanWithSamples(sql,
                output(
                        project(ImmutableMap.of("count", expression("(\"sample_count\" * CAST(20 as bigint))")),
                                aggregation(ImmutableMap.of("sample_count", functionCall("count", false, ImmutableList.of(anySymbol()))),
                                        join(
                                                JoinNode.Type.FULL,
                                                ImmutableList.of(equiJoinClause("cn", "nn")),
                                                tableScan("customer", ImmutableMap.of("cn", "nationkey"), true),
                                                tableScan("nation", ImmutableMap.of("nn", "nationkey")))))),
                Collections.singletonList("customer"));

        sql = "SELECT count(nation.name)\n" +
                "FROM customer\n" +
                "FULL JOIN nation on customer.nationkey = nation.nationkey";

        assertPlanWithSamples(sql,
                output(
                        aggregation(ImmutableMap.of("count", functionCall("count", false, ImmutableList.of(anySymbol()))),
                                join(
                                        JoinNode.Type.FULL,
                                        ImmutableList.of(equiJoinClause("cn", "nn")),
                                        tableScan("customer", ImmutableMap.of("cn", "nationkey"), true),
                                        tableScan("nation", ImmutableMap.of("nn", "nationkey"))))),
                Collections.singletonList("customer"));

        // Count * should not get aggregated irrespective of which table is sampled in the join
        sql = "SELECT count(*)\n" +
                "FROM customer\n" +
                "FULL JOIN nation on customer.nationkey = nation.nationkey";

        assertPlanWithSamples(sql,
                output(
                        aggregation(ImmutableMap.of("count", functionCall("count", false, ImmutableList.of())),
                                join(
                                        JoinNode.Type.FULL,
                                        ImmutableList.of(equiJoinClause("cn", "nn")),
                                        tableScan("customer", ImmutableMap.of("cn", "nationkey"), true),
                                        tableScan("nation", ImmutableMap.of("nn", "nationkey"))))),
                Collections.singletonList("customer"));

        assertPlanWithSamples(sql,
                output(
                        aggregation(ImmutableMap.of("count", functionCall("count", false, ImmutableList.of())),
                                join(
                                        JoinNode.Type.FULL,
                                        ImmutableList.of(equiJoinClause("cn", "nn")),
                                        tableScan("customer", ImmutableMap.of("cn", "nationkey")),
                                        tableScan("nation", ImmutableMap.of("nn", "nationkey"), true)))),
                Collections.singletonList("nation"));

        sql = "SELECT count(customer.name)\n" +
                "FROM customer\n" +
                "FULL JOIN nation on customer.nationkey = nation.nationkey";
        assertPlanWithSamples(sql,
                output(
                        aggregation(ImmutableMap.of("sample_count", functionCall("count", false, ImmutableList.of(anySymbol()))),
                                join(
                                        JoinNode.Type.FULL,
                                        ImmutableList.of(equiJoinClause("cn", "nn")),
                                        tableScan("customer", ImmutableMap.of("cn", "nationkey")),
                                        tableScan("nation", ImmutableMap.of("nn", "nationkey"), true)))),
                Collections.singletonList("nation"));

        sql = "SELECT count(nation.name)\n" +
                "FROM customer\n" +
                "FULL JOIN nation on customer.nationkey = nation.nationkey";
        assertPlanWithSamples(sql,
                output(
                        project(ImmutableMap.of("count", expression("(\"sample_count\" * CAST(20 as bigint))")),
                                aggregation(ImmutableMap.of("sample_count", functionCall("count", false, ImmutableList.of(anySymbol()))),
                                        join(
                                                JoinNode.Type.FULL,
                                                ImmutableList.of(equiJoinClause("cn", "nn")),
                                                tableScan("customer", ImmutableMap.of("cn", "nationkey")),
                                                tableScan("nation", ImmutableMap.of("nn", "nationkey"), true))))),
                Collections.singletonList("nation"));
    }

    @Test
    public void testNestedJoin()
    {
        // Don't scale from left join between non-sampled and sampled
        String sql = "WITH cust as (" +
                "SELECT customer.name, customer.address\n" +
                "FROM customer\n" +
                "LEFT JOIN nation on customer.nationkey = nation.nationkey and nation.name = 'ALGERIA')\n" +
                "SELECT count(supplier.name)\n" +
                "FROM supplier join cust on supplier.address = cust.address\n";

        assertPlanWithSamples(sql,
                output(
                        aggregation(ImmutableMap.of("count", functionCall("count", false, ImmutableList.of(anySymbol()))),
                                join(
                                        JoinNode.Type.INNER,
                                        ImmutableList.of(equiJoinClause("ad", "ad1")),
                                        tableScan("supplier", ImmutableMap.of("ad", "address")),
                                        join(
                                                JoinNode.Type.LEFT,
                                                ImmutableList.of(equiJoinClause("cn", "nn")),
                                                Optional.of("nm = 'ALGERIA'"),
                                                tableScan("customer", ImmutableMap.of("cn", "nationkey", "ad1", "address")),
                                                tableScan("nation", ImmutableMap.of("nn", "nationkey", "nm", "name"), true))))),
                Collections.singletonList("nation"));

        sql = "WITH cust as (" +
                "SELECT customer.name, customer.address\n" +
                "FROM customer\n" +
                "RIGHT JOIN nation on customer.nationkey = nation.nationkey and nation.name = 'ALGERIA')\n" +
                "SELECT count(supplier.name)\n" +
                "FROM supplier join cust on supplier.address = cust.address\n";

        assertPlanWithSamples(sql,
                output(
                        aggregation(ImmutableMap.of("count", functionCall("count", false, ImmutableList.of(anySymbol()))),
                                join(
                                        JoinNode.Type.INNER,
                                        ImmutableList.of(equiJoinClause("ad", "ad1")),
                                        tableScan("supplier", ImmutableMap.of("ad", "address")),
                                        join(
                                                JoinNode.Type.RIGHT,
                                                ImmutableList.of(equiJoinClause("cn", "nn")),
                                                Optional.of("nm = 'ALGERIA'"),
                                                tableScan("customer", ImmutableMap.of("cn", "nationkey", "ad1", "address"), true),
                                                tableScan("nation", ImmutableMap.of("nn", "nationkey", "nm", "name")))))),
                Collections.singletonList("customer"));

        sql = "WITH cust as (" +
            "SELECT customer.name, customer.address\n" +
            "FROM customer\n" +
            "LEFT JOIN nation on customer.nationkey = nation.nationkey and nation.name = 'ALGERIA')\n" +
            "SELECT count(supplier.name)\n" +
            "FROM supplier join cust on supplier.address = cust.address\n";

        assertPlanWithSamples(sql,
                output(
                        project(ImmutableMap.of("count", expression("(\"sample_count\" * CAST(20 as bigint))")),
                                aggregation(ImmutableMap.of("sample_count", functionCall("count", false, ImmutableList.of(anySymbol()))),
                                        join(
                                                JoinNode.Type.INNER,
                                                ImmutableList.of(equiJoinClause("ad", "ad1")),
                                                tableScan("supplier", ImmutableMap.of("ad", "address")),
                                                join(
                                                        JoinNode.Type.LEFT,
                                                        ImmutableList.of(equiJoinClause("cn", "nn")),
                                                        Optional.of("nm = 'ALGERIA'"),
                                                        tableScan("customer", ImmutableMap.of("cn", "nationkey", "ad1", "address"), true),
                                                        tableScan("nation", ImmutableMap.of("nn", "nationkey", "nm", "name"))))))),
                Collections.singletonList("customer"));

        sql = "WITH cust as (" +
                "SELECT customer.name, customer.address\n" +
                "FROM customer\n" +
                "RIGHT JOIN nation on customer.nationkey = nation.nationkey and nation.name = 'ALGERIA')\n" +
                "SELECT count(supplier.name)\n" +
                "FROM supplier join cust on supplier.address = cust.address\n";

        assertPlanWithSamples(sql,
                output(
                        project(ImmutableMap.of("count", expression("(\"sample_count\" * CAST(20 as bigint))")),
                                aggregation(ImmutableMap.of("sample_count", functionCall("count", false, ImmutableList.of(anySymbol()))),
                                        join(
                                                JoinNode.Type.INNER,
                                                ImmutableList.of(equiJoinClause("ad", "ad1")),
                                                tableScan("supplier", ImmutableMap.of("ad", "address")),
                                                join(
                                                        JoinNode.Type.RIGHT,
                                                        ImmutableList.of(equiJoinClause("cn", "nn")),
                                                        Optional.of("nm = 'ALGERIA'"),
                                                        tableScan("customer", ImmutableMap.of("cn", "nationkey", "ad1", "address")),
                                                        tableScan("nation", ImmutableMap.of("nn", "nationkey", "nm", "name"), true)))))),
                Collections.singletonList("nation"));
    }

    @Test
    public void testUnion()
    {
        // union between sampled and non-sampled does not scale
        String sql = "select custkey, count(*) as count from customer group by 1 union select custkey, count(*) as count from orders group by 1";

        assertPlanWithSamples(sql,
                anyTree(
                        union(
                                aggregation(ImmutableMap.of("count", functionCall("count", false, ImmutableList.of())),
                                        tableScan("customer")),
                                aggregation(ImmutableMap.of("sample_count_10", functionCall("count", false, ImmutableList.of())),
                                                tableScan("orders")))),
                Collections.singletonList("orders"),
                true);

        // union all
        sql = "select custkey, count(*) as count from customer group by 1 union all select custkey, count(*) as count from orders group by 1";

        assertPlanWithSamples(sql,
                anyTree(
                        union(
                                aggregation(ImmutableMap.of("count", functionCall("count", false, ImmutableList.of())),
                                        tableScan("customer")),
                                aggregation(ImmutableMap.of("sample_count_10", functionCall("count", false, ImmutableList.of())),
                                                tableScan("orders")))),
                Collections.singletonList("orders"),
                true);

        // Test that union of multiple different sample table fails
        assertPlanWithSamples(sql,
                anyTree(
                        union(
                                aggregation(ImmutableMap.of("count", functionCall("count", false, ImmutableList.of())),
                                        tableScan("customer")),
                                aggregation(ImmutableMap.of("count_10", functionCall("count", false, ImmutableList.of())),
                                        tableScan("orders")))),
                Arrays.asList("orders", "customer"),
                true);

        // union with the same sampled table multiple times works fine
        sql = "Select nationkey, count(*) nat_count from customer group by 1\n" +
                "union ALL\n" +
                "Select -1 nationkey, count(nationkey) nat_count from customer";

        PlanMatchPattern pattern = project(ImmutableMap.of("count", expression("(\"sample_count\" * CAST(20 as bigint))")),
                aggregation(ImmutableMap.of("sample_count", functionCall("count", false, ImmutableList.of(anySymbol()))),
                        tableScan("customer", ImmutableMap.of("nn", "nationkey"), true)));

        assertPlanWithSamples(sql,
                output(
                        union(
                                project(ImmutableMap.of("count", expression("(\"sample_count\" * CAST(20 as bigint))")),
                                        aggregation(ImmutableMap.of("sample_count", functionCall("count", false, ImmutableList.of())),
                                                tableScan("customer", ImmutableMap.of("nn", "nationkey"), true))),
                                anyTree(
                                        project(ImmutableMap.of("count_15", expression("(\"sample_count_15\" * CAST(20 as bigint))")),
                                                aggregation(ImmutableMap.of("sample_count_15", functionCall("count", false, ImmutableList.of(anySymbol()))),
                                                        tableScan("customer", ImmutableMap.of("nn", "nationkey"), true)))))),
                Collections.singletonList("customer"));

        // union of the sampled table (two times) with a non sampled table
        sql = "Select nationkey, count(*) nat_count from customer group by 1\n" +
                "union ALL\n" +
                "Select -1 nationkey, count(nationkey) nat_count from customer\n" +
                "union ALL\n" +
                "Select nationkey, count(*) nat_count from supplier group by 1";

        assertPlanWithSamples(sql,
                output(
                        union(
                                union(
                                        aggregation(ImmutableMap.of("sample_count", functionCall("count", false, ImmutableList.of())),
                                                        tableScan("customer", ImmutableMap.of("nn", "nationkey"))),
                                        anyTree(
                                                aggregation(ImmutableMap.of("sample_count_15", functionCall("count", false, ImmutableList.of(anySymbol()))),
                                                                tableScan("customer", ImmutableMap.of("nn", "nationkey"))))),
                                aggregation(ImmutableMap.of("count_21", functionCall("count", false, ImmutableList.of())),
                                        tableScan("supplier", ImmutableMap.of("sn", "nationkey"))))),
                Collections.singletonList("customer"),
                true);

        // Inner join of a full dataset with this sampled union scales it too
        sql = "Select nation.name, count(*) from nation join\n" +
                "(Select nationkey, count(*) nat_count from customer group by 1\n" +
                "union ALL\n" +
                "Select -1 nationkey, count(nationkey) nat_count from customer) b on nation.nationkey = b.nationkey group by 1";

        assertPlanWithSamples(sql,
                output(
                        project(ImmutableMap.of("count", expression("(\"sample_count\" * CAST(20 as bigint))")),
                                aggregation(ImmutableMap.of("sample_count", functionCall("count", false, ImmutableList.of())),
                                        anyTree(
                                                tableScan("nation"),
                                                union(
                                                        anyTree(
                                                                tableScan("customer", true)),
                                                        anyTree(
                                                                tableScan("customer", true))))))),
                Collections.singletonList("customer"));

        sql = "Select nation.name, count(*) from nation join\n" +
                "(Select nationkey, count(*) nat_count from customer group by 1\n" +
                "union ALL\n" +
                "Select nationkey, count(*) nat_count from supplier group by 1) b on nation.nationkey = b.nationkey group by 1";

        assertPlanWithSamples(sql,
                output(
                        aggregation(ImmutableMap.of("count", functionCall("count", false, ImmutableList.of())),
                                anyTree(
                                        tableScan("nation"),
                                        union(
                                                anyTree(
                                                        tableScan("customer")),
                                                anyTree(
                                                        tableScan("supplier")))))),
                Collections.singletonList("customer"),
                true);
    }

    public void assertPlanWithSamples(String sql, PlanMatchPattern pattern, List<String> sampleTables)
    {
        assertPlanWithSamples(sql, pattern, sampleTables, false);
    }

    public void assertPlanWithSamples(String sql, PlanMatchPattern pattern, List<String> sampleTables, boolean failOpen)
    {
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new TestAutoSampleTableReplaceOptimizer(getMetadata(), sampleTables, failOpen));
        Session session;
        if (failOpen) {
            session = Session.builder(this.getQueryRunner().getDefaultSession())
                    .setSystemProperty(APPROX_RESULTS_OPTION, ApproxResultsOption.SAMPLING.name())
                    .build();
        }
        else {
            session = Session.builder(this.getQueryRunner().getDefaultSession())
                    .setSystemProperty(APPROX_RESULTS_OPTION, ApproxResultsOption.SAMPLING_WITH_FAIL_CLOSE.name())
                    .build();
        }
        assertMinimallyOptimizedPlanWithSession(sql, session, pattern, optimizers);
    }

    private class TestAutoSampleTableReplaceOptimizer
            extends AutoSampleTableReplaceOptimizer
    {
        private List<String> sampleTables;

        public TestAutoSampleTableReplaceOptimizer(Metadata metadata, List<String> sampleTables, boolean failOpen)
        {
            super(metadata, failOpen);
            this.sampleTables = sampleTables;
        }

        // If the table is in the list, then return the same table as its sample with a sampling percentage of 5%
        public Optional<TableSample> getSampleTable(Session session, TableHandle tableHandle)
        {
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle).getMetadata();
            if (sampleTables.contains(tableMetadata.getTable().getTableName())) {
                return Optional.of(new TableSample(tableMetadata.getTable(), Optional.of(Integer.valueOf(5))));
            }
            return Optional.empty();
        }
    }
}
