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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCALE_FACTOR;

public class TestTransformCorrelatedSingleRowSubqueryToProject
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new TransformCorrelatedSingleRowSubqueryToProject())
                .on(p -> p.values(p.variable("a")))
                .doesNotFire();
    }

    @Test
    public void testRewrite()
    {
        tester().assertThat(new TransformCorrelatedSingleRowSubqueryToProject())
                .on(p ->
                        p.lateral(
                                ImmutableList.of(p.variable("l_nationkey")),
                                p.tableScan(new TableHandle(
                                                new ConnectorId("local"),
                                                new TpchTableHandle("nation", TINY_SCALE_FACTOR),
                                                TestingTransactionHandle.create(),
                                                Optional.empty()),
                                        ImmutableList.of(p.variable("l_nationkey")),
                                        ImmutableMap.of(p.variable("l_nationkey"), new TpchColumnHandle("nationkey",
                                                BIGINT))),
                                p.project(
                                        assignment(p.variable("l_expr2"), expression("l_nationkey + 1")),
                                        p.values(
                                                ImmutableList.of(),
                                                ImmutableList.of(ImmutableList.of())))))
                .matches(project(
                        ImmutableMap.of(
                                ("l_expr2"), PlanMatchPattern.expression("l_nationkey + 1"),
                                "l_nationkey", PlanMatchPattern.expression("l_nationkey")),
                        tableScan("nation", ImmutableMap.of("l_nationkey", "nationkey"))));
    }

    @Test
    public void testDoesNotFireWithEmptyValuesNode()
    {
        tester().assertThat(new TransformCorrelatedSingleRowSubqueryToProject())
                .on(p ->
                        p.lateral(
                                ImmutableList.of(p.variable("a")),
                                p.values(p.variable("a")),
                                p.values(p.variable("a"))))
                .doesNotFire();
    }
}
