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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.sql.planner.assertions.ExpectedValueProvider;
import io.prestosql.sql.planner.iterative.IterativeOptimizer;
import io.prestosql.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import io.prestosql.sql.planner.optimizations.UnaliasSymbolReferences;
import io.prestosql.sql.planner.plan.WindowNode;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.specification;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.window;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;

public class TestCanonicalize
        extends BasePlanTest
{
    @Test
    public void testJoin()
    {
        assertPlan(
                "SELECT *\n" +
                        "FROM (\n" +
                        "    SELECT EXTRACT(DAY FROM DATE '2017-01-01')\n" +
                        ") t\n" +
                        "CROSS JOIN (VALUES 1)",
                anyTree(
                        join(INNER, ImmutableList.of(), Optional.empty(),
                                project(
                                        ImmutableMap.of("X", expression("BIGINT '1'")),
                                        values(ImmutableMap.of())),
                                values(ImmutableMap.of()))));
    }

    @Test
    public void testDuplicatesInWindowOrderBy()
    {
        ExpectedValueProvider<WindowNode.Specification> specification = specification(
                ImmutableList.of(),
                ImmutableList.of("A"),
                ImmutableMap.of("A", SortOrder.ASC_NULLS_LAST));

        assertPlan(
                "WITH x as (SELECT a, a as b FROM (VALUES 1) t(a))" +
                        "SELECT *, row_number() OVER(ORDER BY a ASC, b DESC)" +
                        "FROM x",
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specification)
                                        .addFunction(functionCall("row_number", Optional.empty(), ImmutableList.of())),
                                values("A"))),
                ImmutableList.of(
                        new UnaliasSymbolReferences(),
                        new IterativeOptimizer(
                                new RuleStatsRecorder(),
                                getQueryRunner().getStatsCalculator(),
                                getQueryRunner().getCostCalculator(),
                                ImmutableSet.of(new RemoveRedundantIdentityProjections()))));
    }
}
