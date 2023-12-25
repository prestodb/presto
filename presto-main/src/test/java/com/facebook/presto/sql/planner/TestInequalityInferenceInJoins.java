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
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.INFER_INEQUALITY_PREDICATES;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.spi.plan.JoinType.RIGHT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;

public class TestInequalityInferenceInJoins
        extends BasePlanTest
{
    public TestInequalityInferenceInJoins()
    {
        super(ImmutableMap.of(INFER_INEQUALITY_PREDICATES, Boolean.toString(true)));
    }

    @Test
    public void testSimpleInequalityInferencesForInnerJoins()
    {
        // Infer predicate on orders.totalprice and push it down to tablescan
        String query = "select count(*) from customer c, orders o where o.custkey=c.custkey and o.totalprice > c.acctbal and c.acctbal > 1000";
        assertPlan(query,
                output(
                        anyTree(
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("custkey_0", "custkey")),
                                        Optional.of("(totalprice) > (acctbal)"),
                                        anyTree(
                                                filter("(totalprice) > (DOUBLE'1000.0')",
                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "custkey_0", "custkey")))),
                                        anyTree(
                                                filter("(acctbal) > (DOUBLE'1000.0')",
                                                        tableScan("customer", ImmutableMap.of("custkey", "custkey", "acctbal", "acctbal"))))))));

        query = "select count(*) from customer c, orders o where o.custkey=c.custkey and o.totalprice >= c.acctbal and c.acctbal > 1000";
        assertPlan(query,
                output(
                        anyTree(
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("custkey_0", "custkey")),
                                        Optional.of("(totalprice) >= (acctbal)"),
                                        anyTree(
                                                filter("(totalprice) > (DOUBLE'1000.0')",
                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "custkey_0", "custkey")))),
                                        anyTree(
                                                filter("(acctbal) > (DOUBLE'1000.0')",
                                                        tableScan("customer", ImmutableMap.of("custkey", "custkey", "acctbal", "acctbal"))))))));

        query = "select count(*) from customer c, orders o where o.custkey=c.custkey and o.totalprice >= c.acctbal and c.acctbal >= 1000";
        assertPlan(query,
                output(
                        anyTree(
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("custkey_0", "custkey")),
                                        Optional.of("(totalprice) >= (acctbal)"),
                                        anyTree(
                                                filter("(totalprice) >= (DOUBLE'1000.0')",
                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "custkey_0", "custkey")))),
                                        anyTree(
                                                filter("(acctbal) >= (DOUBLE'1000.0')",
                                                        tableScan("customer", ImmutableMap.of("custkey", "custkey", "acctbal", "acctbal"))))))));

        query = "select count(*) from customer c, orders o where o.custkey=c.custkey and o.totalprice > c.acctbal and c.acctbal >= 1000";
        assertPlan(query,
                output(
                        anyTree(
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("custkey_0", "custkey")),
                                        Optional.of("(totalprice) > (acctbal)"),
                                        anyTree(
                                                filter("(totalprice) > (DOUBLE'1000.0')",
                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "custkey_0", "custkey")))),
                                        anyTree(
                                                filter("(acctbal) >= (DOUBLE'1000.0')",
                                                        tableScan("customer", ImmutableMap.of("custkey", "custkey", "acctbal", "acctbal"))))))));

        //Redo same combination of inequalities with LESS THAN
        query = "select count(*) from customer c, orders o where o.custkey=c.custkey and o.totalprice < c.acctbal and c.acctbal < 1000";
        assertPlan(query,
                output(
                        anyTree(
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("custkey", "custkey_0")),
                                        Optional.of("(totalprice) < (acctbal)"),
                                        anyTree(
                                                filter("(acctbal) < (DOUBLE'1000.0')",
                                                        tableScan("customer", ImmutableMap.of("custkey", "custkey", "acctbal", "acctbal")))),
                                        anyTree(
                                                filter("(totalprice) < (DOUBLE'1000.0')",
                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "custkey_0", "custkey"))))))));

        query = "select count(*) from customer c, orders o where o.custkey=c.custkey and o.totalprice <= c.acctbal and c.acctbal < 1000";
        assertPlan(query,
                output(
                        anyTree(
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("custkey", "custkey_0")),
                                        Optional.of("(totalprice) <= (acctbal)"),
                                        anyTree(
                                                filter("(acctbal) < (DOUBLE'1000.0')",
                                                        tableScan("customer", ImmutableMap.of("custkey", "custkey", "acctbal", "acctbal")))),
                                        anyTree(
                                                filter("(totalprice) < (DOUBLE'1000.0')",
                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "custkey_0", "custkey"))))))));

        query = "select count(*) from customer c, orders o where o.custkey=c.custkey and o.totalprice <= c.acctbal and c.acctbal <= 1000";
        assertPlan(query,
                output(
                        anyTree(
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("custkey", "custkey_0")),
                                        Optional.of("(totalprice) <= (acctbal)"),
                                        anyTree(
                                                filter("(acctbal) <= (DOUBLE'1000.0')",
                                                        tableScan("customer", ImmutableMap.of("custkey", "custkey", "acctbal", "acctbal")))),
                                        anyTree(
                                                filter("(totalprice) <= (DOUBLE'1000.0')",
                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "custkey_0", "custkey"))))))));

        query = "select count(*) from customer c, orders o where o.custkey=c.custkey and o.totalprice < c.acctbal and c.acctbal <= 1000";
        assertPlan(query,
                output(
                        anyTree(
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("custkey", "custkey_0")),
                                        Optional.of("(totalprice) < (acctbal)"),
                                        anyTree(
                                                filter("(acctbal) <= (DOUBLE'1000.0')",
                                                        tableScan("customer", ImmutableMap.of("custkey", "custkey", "acctbal", "acctbal")))),
                                        anyTree(
                                                filter("(totalprice) < (DOUBLE'1000.0')",
                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "custkey_0", "custkey"))))))));

        // Negative test - non-deterministic predicates are ignored
        query = "select count(*) from customer c, orders o where o.custkey=c.custkey and o.totalprice < c.acctbal and c.acctbal <= random()";
        assertPlan(query,
                output(
                        anyTree(
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("custkey", "custkey_0")),
                                        Optional.of("((acctbal) <= (random())) AND ((totalprice) < (acctbal))"),
                                        anyTree(
                                                tableScan("customer", ImmutableMap.of("custkey", "custkey", "acctbal", "acctbal"))),
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "custkey_0", "custkey")))))));
    }

    @Test
    public void testSimpleInequalityInferencesForOuterJoins()
    {
        // Infer predicate on orders.totalprice and push it down to tablescan
        String query = "select count(*) from customer c left join orders o on o.custkey=c.custkey and o.totalprice > c.acctbal where c.acctbal > 1000";
        assertPlan(query,
                output(
                        anyTree(
                                join(LEFT,
                                        ImmutableList.of(equiJoinClause("custkey", "custkey_0")),
                                        Optional.of("(totalprice) > (acctbal)"),
                                        anyTree(
                                                filter("(acctbal) > (DOUBLE'1000.0')",
                                                        tableScan("customer", ImmutableMap.of("custkey", "custkey", "acctbal", "acctbal")))),
                                        anyTree(
                                                filter("(totalprice) > (DOUBLE'1000.0')",
                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "custkey_0", "custkey"))))))));

        query = "select count(*) from customer c left join orders o on o.custkey=c.custkey and o.totalprice >= c.acctbal where c.acctbal > 1000";
        assertPlan(query,
                output(
                        anyTree(
                                join(LEFT,
                                        ImmutableList.of(equiJoinClause("custkey", "custkey_0")),
                                        Optional.of("(totalprice) >= (acctbal)"),
                                        anyTree(
                                                filter("(acctbal) > (DOUBLE'1000.0')",
                                                        tableScan("customer", ImmutableMap.of("custkey", "custkey", "acctbal", "acctbal")))),
                                        anyTree(
                                                filter("(totalprice) > (DOUBLE'1000.0')",
                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "custkey_0", "custkey"))))))));

        query = "select count(*) from customer c left join orders o on o.custkey=c.custkey and o.totalprice >= c.acctbal where c.acctbal >= 1000";
        assertPlan(query,
                output(
                        anyTree(
                                join(LEFT,
                                        ImmutableList.of(equiJoinClause("custkey", "custkey_0")),
                                        Optional.of("(totalprice) >= (acctbal)"),
                                        anyTree(
                                                filter("(acctbal) >= (DOUBLE'1000.0')",
                                                        tableScan("customer", ImmutableMap.of("custkey", "custkey", "acctbal", "acctbal")))),
                                        anyTree(
                                                filter("(totalprice) >= (DOUBLE'1000.0')",
                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "custkey_0", "custkey"))))))));

        query = "select count(*) from customer c left join orders o on o.custkey=c.custkey and o.totalprice > c.acctbal where c.acctbal >= 1000";
        assertPlan(query,
                output(
                        anyTree(
                                join(LEFT,
                                        ImmutableList.of(equiJoinClause("custkey", "custkey_0")),
                                        Optional.of("(totalprice) > (acctbal)"),
                                        anyTree(
                                                filter("(acctbal) >= (DOUBLE'1000.0')",
                                                        tableScan("customer", ImmutableMap.of("custkey", "custkey", "acctbal", "acctbal")))),
                                        anyTree(
                                                filter("(totalprice) > (DOUBLE'1000.0')",
                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "custkey_0", "custkey"))))))));

        //Redo same combination of inequalities with LESS THAN
        query = "select count(*) from customer c left join orders o on o.custkey=c.custkey and o.totalprice < c.acctbal where c.acctbal < 1000";
        assertPlan(query,
                output(
                        anyTree(
                                join(LEFT,
                                        ImmutableList.of(equiJoinClause("custkey", "custkey_0")),
                                        Optional.of("(totalprice) < (acctbal)"),
                                        anyTree(
                                                filter("(acctbal) < (DOUBLE'1000.0')",
                                                        tableScan("customer", ImmutableMap.of("custkey", "custkey", "acctbal", "acctbal")))),
                                        anyTree(
                                                filter("(totalprice) < (DOUBLE'1000.0')",
                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "custkey_0", "custkey"))))))));

        query = "select count(*) from customer c left join orders o on o.custkey=c.custkey and o.totalprice <= c.acctbal where c.acctbal < 1000";
        assertPlan(query,
                output(
                        anyTree(
                                join(LEFT,
                                        ImmutableList.of(equiJoinClause("custkey", "custkey_0")),
                                        Optional.of("(totalprice) <= (acctbal)"),
                                        anyTree(
                                                filter("(acctbal) < (DOUBLE'1000.0')",
                                                        tableScan("customer", ImmutableMap.of("custkey", "custkey", "acctbal", "acctbal")))),
                                        anyTree(
                                                filter("(totalprice) < (DOUBLE'1000.0')",
                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "custkey_0", "custkey"))))))));

        query = "select count(*) from customer c left join orders o on o.custkey=c.custkey and o.totalprice <= c.acctbal where c.acctbal <= 1000";
        assertPlan(query,
                output(
                        anyTree(
                                join(LEFT,
                                        ImmutableList.of(equiJoinClause("custkey", "custkey_0")),
                                        Optional.of("(totalprice) <= (acctbal)"),
                                        anyTree(
                                                filter("(acctbal) <= (DOUBLE'1000.0')",
                                                        tableScan("customer", ImmutableMap.of("custkey", "custkey", "acctbal", "acctbal")))),
                                        anyTree(
                                                filter("(totalprice) <= (DOUBLE'1000.0')",
                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "custkey_0", "custkey"))))))));

        query = "select count(*) from customer c left join orders o on o.custkey=c.custkey and o.totalprice < c.acctbal where c.acctbal <= 1000";
        assertPlan(query,
                output(
                        anyTree(
                                join(LEFT,
                                        ImmutableList.of(equiJoinClause("custkey", "custkey_0")),
                                        Optional.of("(totalprice) < (acctbal)"),
                                        anyTree(
                                                filter("(acctbal) <= (DOUBLE'1000.0')",
                                                        tableScan("customer", ImmutableMap.of("custkey", "custkey", "acctbal", "acctbal")))),
                                        anyTree(
                                                filter("(totalprice) < (DOUBLE'1000.0')",
                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "custkey_0", "custkey"))))))));

        // Predicate in ON clause is inferred and pushed to the inner side of the join
        query = "select count(*) from customer c left join orders o on o.custkey=c.custkey and o.totalprice < c.acctbal and c.acctbal <= 1000";
        assertPlan(query,
                output(
                        anyTree(
                                join(LEFT,
                                        ImmutableList.of(equiJoinClause("custkey", "custkey_0")),
                                        Optional.of("((totalprice) < (acctbal)) AND ((acctbal) <= (DOUBLE'1000.0'))"),
                                        anyTree(
                                                tableScan("customer", ImmutableMap.of("custkey", "custkey", "acctbal", "acctbal"))),
                                        anyTree(
                                                filter("(totalprice) < (DOUBLE'1000.0')",
                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "custkey_0", "custkey"))))))));

        // Negative tests
        // Outer join with no WHERE clause - nothing gets inferred
        query = "select count(*) from customer c left join orders o on o.custkey=c.custkey and o.totalprice < c.acctbal and o.totalprice < 1000";
        assertPlan(query,
                output(
                        anyTree(
                                join(LEFT,
                                        ImmutableList.of(equiJoinClause("custkey", "custkey_0")),
                                        Optional.of("(totalprice) < (acctbal)"),
                                        project(
                                                tableScan("customer", ImmutableMap.of("custkey", "custkey", "acctbal", "acctbal"))),
                                        anyTree(
                                                filter("(totalprice) < (DOUBLE'1000.0')",
                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "custkey_0", "custkey"))))))));

        query = "select count(*) from customer c left join orders o on o.custkey=c.custkey and o.totalprice < c.acctbal where o.totalprice < 1000";
        assertPlan(query,
                output(
                        anyTree(
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("custkey", "custkey_0")),
                                        Optional.of("(totalprice) < (acctbal)"),
                                        project(
                                                tableScan("customer", ImmutableMap.of("custkey", "custkey", "acctbal", "acctbal"))),
                                        anyTree(
                                                filter("(totalprice) < (DOUBLE'1000.0')",
                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "custkey_0", "custkey"))))))));
    }

    @Test
    public void testInequalityInferencesForExpressions()
    {
        String query = "select count(*) from orders o, customer c where o.custkey=c.custkey and o.orderkey + o.custkey <= c.nationkey and 100 <= o.orderkey + o.custkey";
        assertPlan(query,
                output(
                        anyTree(
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("custkey", "custkey_0")),
                                        Optional.of("orderkey + custkey <= nationkey"),
                                        anyTree(
                                                filter("BIGINT '100' <= orderkey+custkey",
                                                        tableScan("orders", ImmutableMap.of("orderkey", "orderkey", "custkey", "custkey")))),
                                        anyTree(
                                                filter("nationkey >= BIGINT '100'",
                                                        tableScan("customer", ImmutableMap.of("custkey_0", "custkey", "nationkey", "nationkey"))))))));

        query = "select count(*) from orders o left join customer c on o.custkey=c.custkey and o.orderkey + o.custkey <= c.nationkey where 100 <= o.orderkey + o.custkey";
        assertPlan(query,
                output(
                        anyTree(
                                join(LEFT,
                                        ImmutableList.of(equiJoinClause("custkey", "custkey_0")),
                                        Optional.of("orderkey + custkey <= nationkey"),
                                        anyTree(
                                                filter("BIGINT '100' <= orderkey+custkey",
                                                        tableScan("orders", ImmutableMap.of("orderkey", "orderkey", "custkey", "custkey")))),
                                        anyTree(
                                                filter("nationkey >= BIGINT '100'",
                                                        tableScan("customer", ImmutableMap.of("custkey_0", "custkey", "nationkey", "nationkey"))))))));

        query = "select count(*) from customer c right join orders o on o.custkey=c.custkey and o.orderkey + o.custkey <= c.nationkey where 100 <= o.orderkey + o.custkey";
        assertPlan(query,
                output(
                        anyTree(
                                join(RIGHT,
                                        ImmutableList.of(equiJoinClause("custkey", "custkey_0")),
                                        Optional.of("orderkey + custkey_0 <= nationkey"),
                                        anyTree(
                                                filter("nationkey >= BIGINT '100'",
                                                        tableScan("customer", ImmutableMap.of("custkey", "custkey", "nationkey", "nationkey")))),
                                        anyTree(
                                                filter("BIGINT '100' <= orderkey+custkey_0",
                                                        tableScan("orders", ImmutableMap.of("orderkey", "orderkey", "custkey_0", "custkey"))))))));

        // Canonicalize and infer orderkey+custkey <=> custkey+orderkey and therefore infer customer.nationkey >= 100
        query = "select count(*) from orders o, customer c where o.custkey=c.custkey and o.orderkey + o.custkey <= c.nationkey and 100 <= o.custkey + o.orderkey";
        assertPlan(query,
                output(
                        anyTree(
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("custkey", "custkey_0")),
                                        Optional.of("orderkey + custkey <= nationkey"),
                                        anyTree(
                                                filter("BIGINT '100' <= custkey + orderkey",
                                                        tableScan("orders", ImmutableMap.of("orderkey", "orderkey", "custkey", "custkey")))),
                                        anyTree(
                                                filter("nationkey >= BIGINT'100'",
                                                        tableScan("customer", ImmutableMap.of("custkey_0", "custkey", "nationkey", "nationkey"))))))));

        // Negative test - do not push down since the inferred predicate on (o.orderkey + c.nationkey) references variables from the outer table
        query = "select name, address, c.custkey from customer c left join orders o on c.custkey=o.custkey and c.custkey < o.orderkey + c.nationkey where 1000 < c.custkey";
        assertPlan(query,
                output(
                        join(
                                LEFT,
                                ImmutableList.of(equiJoinClause("custkey", "custkey_0")),
                                Optional.of("custkey < orderkey + nationkey"),
                                anyTree(
                                        filter("custkey > BIGINT'1000'",
                                                tableScan("customer", ImmutableMap.of("name", "name", "custkey", "custkey", "nationkey", "nationkey", "address", "address")))),
                                anyTree(
                                        filter("custkey_0 > BIGINT'1000'",
                                                tableScan("orders", ImmutableMap.of("orderkey", "orderkey", "custkey_0", "custkey")))))));

        // Infer sq.custkey > 100 in outer query block, push it into inner query block's where and then infer that
        // 1. customer.custkey > 100
        // 2. lineitem.partkey > 100 and lineitem.suppkey > 100
        query = "select o.totalprice, o.comment, sq.name, sq.quantity from orders o left join (select * from customer c left join lineitem l on c.custkey=l.suppkey and c.custkey < l.partkey) sq on o.custkey=sq.custkey and o.orderkey <= sq.custkey where o.orderkey > 100";
        assertPlan(query,
                output(
                        join(LEFT,
                                ImmutableList.of(equiJoinClause("custkey", "custkey_0")),
                                Optional.of("orderkey <= custkey_0"),
                                anyTree(
                                        filter("orderkey > BIGINT'100'",
                                                tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "custkey", "custkey", "comment", "comment", "orderkey", "orderkey")))),
                                anyTree(
                                        join(LEFT,
                                                ImmutableList.of(equiJoinClause("custkey_0", "suppkey")),
                                                anyTree(
                                                        filter("custkey_0 > BIGINT'100'",
                                                                tableScan("customer", ImmutableMap.of("custkey_0", "custkey", "name", "name")))),
                                                anyTree(
                                                        filter("partkey > BIGINT'100' AND suppkey > BIGINT'100' AND suppkey < partkey",
                                                                tableScan("lineitem", ImmutableMap.of("suppkey", "suppkey", "partkey", "partkey", "quantity", "quantity")))))))));
    }
}
