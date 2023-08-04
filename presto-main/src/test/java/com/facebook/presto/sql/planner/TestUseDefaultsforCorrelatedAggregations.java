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

import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;

public class TestUseDefaultsforCorrelatedAggregations
        extends BasePlanTest
{
    private ImmutableMap<String, String> nationColumns = ImmutableMap.<String, String>builder()
            .put("regionkey", "regionkey")
            .put("nationkey", "nationkey")
            .put("name", "name")
            .put("comment", "comment")
            .build();

    @Test
    public void testConversionOfCrossJoinWithLeftOuterJoinToInnerJoin()
    {
        String query = "select * from nation n where (select count(*) from customer c where n.nationkey=c.nationkey)>0";
        assertPlan(query,
                output(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("nationkey", "nationkey_1")),
                                anyTree(
                                        tableScan("nation", nationColumns)),
                                project(
                                        filter("coalesce(count, 0) > 0",
                                                aggregation(ImmutableMap.of("count", functionCall("count", ImmutableList.of("count_6"))),
                                                        FINAL,
                                                        anyTree(
                                                                aggregation(ImmutableMap.of("count_6", functionCall("count", ImmutableList.of())),
                                                                        PARTIAL,
                                                                        tableScan("customer", ImmutableMap.of("nationkey_1", "nationkey"))))))))));

        query = "select * from nation n where (select min(custkey) from customer c where n.nationkey=c.nationkey)>0";
        assertPlan(query,
                output(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("nationkey", "nationkey_1")),
                                anyTree(
                                        tableScan("nation", nationColumns)),
                                project(
                                        filter("min > 0",
                                                aggregation(ImmutableMap.of("min", functionCall("min", ImmutableList.of("min_27"))),
                                                        FINAL,
                                                        anyTree(
                                                                aggregation(ImmutableMap.of("min_27", functionCall("min", ImmutableList.of("custkey"))),
                                                                        PARTIAL,
                                                                        tableScan("customer", ImmutableMap.of("nationkey_1", "nationkey", "custkey", "custkey"))))))))));

        query = "select * from nation n where (select max(custkey) from customer c where n.nationkey=c.nationkey)>0";
        assertPlan(query,
                output(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("nationkey", "nationkey_1")),
                                anyTree(
                                        tableScan("nation", nationColumns)),
                                project(
                                        filter("max > 0",
                                                aggregation(ImmutableMap.of("max", functionCall("max", ImmutableList.of("max_27"))),
                                                        FINAL,
                                                        anyTree(
                                                                aggregation(ImmutableMap.of("max_27", functionCall("max", ImmutableList.of("custkey"))),
                                                                        PARTIAL,
                                                                        tableScan("customer", ImmutableMap.of("nationkey_1", "nationkey", "custkey", "custkey"))))))))));

        query = "select * from nation n where (select sum(custkey) from customer c where n.nationkey=c.nationkey)>0";
        assertPlan(query,
                output(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("nationkey", "nationkey_1")),
                                anyTree(
                                        tableScan("nation", nationColumns)),
                                project(
                                        filter("sum > 0",
                                                aggregation(ImmutableMap.of("sum", functionCall("sum", ImmutableList.of("sum_27"))),
                                                        FINAL,
                                                        anyTree(
                                                                aggregation(ImmutableMap.of("sum_27", functionCall("sum", ImmutableList.of("custkey"))),
                                                                        PARTIAL,
                                                                        tableScan("customer", ImmutableMap.of("nationkey_1", "nationkey", "custkey", "custkey"))))))))));

        query = "select * from nation n where (select avg(custkey) from customer c where n.nationkey=c.nationkey)>0";
        assertPlan(query,
                output(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("nationkey", "nationkey_1")),
                                anyTree(
                                        tableScan("nation", nationColumns)),
                                project(
                                        filter("avg > double '0.0'",
                                                aggregation(ImmutableMap.of("avg", functionCall("avg", ImmutableList.of("avg_27"))),
                                                        FINAL,
                                                        anyTree(
                                                                aggregation(ImmutableMap.of("avg_27", functionCall("avg", ImmutableList.of("custkey"))),
                                                                        PARTIAL,
                                                                        tableScan("customer", ImmutableMap.of("nationkey_1", "nationkey", "custkey", "custkey"))))))))));

        query = "select * from nation n where (select arbitrary(custkey) from customer c where n.nationkey=c.nationkey)>0";
        assertPlan(query,
                output(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("nationkey", "nationkey_1")),
                                anyTree(
                                        tableScan("nation", nationColumns)),
                                project(
                                        filter("arbitrary > 0",
                                                aggregation(ImmutableMap.of("arbitrary", functionCall("arbitrary", ImmutableList.of("arbitrary_27"))),
                                                        FINAL,
                                                        anyTree(
                                                                aggregation(ImmutableMap.of("arbitrary_27", functionCall("arbitrary", ImmutableList.of("custkey"))),
                                                                        PARTIAL,
                                                                        tableScan("customer", ImmutableMap.of("nationkey_1", "nationkey", "custkey", "custkey"))))))))));

        // Negative tests
        query = "select * from nation n where (select max_by(acctbal, nationkey) from customer c where n.nationkey=c.nationkey)>0";
        assertPlan(query,
                output(
                        anyTree(
                                join(INNER,
                                        ImmutableList.of(),
                                        join(LEFT,
                                                ImmutableList.of(equiJoinClause("nationkey", "nationkey_1")),
                                                anyTree(
                                                        tableScan("nation", nationColumns)),
                                                anyTree(
                                                        tableScan("customer", ImmutableMap.of("nationkey_1", "nationkey", "acctbal", "acctbal")))),
                                        anyTree(
                                                values("expr_25", "expr_26"))))));

        query = "select * from nation n where (select count_if(acctbal > 0) from customer c where n.nationkey=c.nationkey)>0";
        assertPlan(query,
                output(
                        anyTree(
                                join(INNER,
                                        ImmutableList.of(),
                                        join(LEFT,
                                                ImmutableList.of(equiJoinClause("nationkey", "nationkey_1")),
                                                anyTree(
                                                        tableScan("nation", nationColumns)),
                                                anyTree(
                                                        tableScan("customer", ImmutableMap.of("nationkey_1", "nationkey", "acctbal", "acctbal")))),
                                        anyTree(
                                                values("expr_24"))))));
    }
}
