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

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.function.Predicate;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.RowType.field;
import static com.facebook.presto.spi.type.RowType.from;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneFilterColumnFields
        extends BaseRuleTest
{
    @Test
    public void testNotAllNestedFieldsReferenced()
    {
        tester().assertThat(new PruneFilterColumnFields())
                .on(p -> buildProjectedFilter(p, symbol -> symbol.getName().equals("expr_y")))
                .matches(
                        strictProject(
                                ImmutableMap.of("expr", expression("a.y")),
                                filter(
                                        "a.x > 5",
                                        strictProject(
                                                ImmutableMap.of("a", expression(new SymbolReference("a", ImmutableSet.of("x", "y")))),
                                                values("a")))));
    }

    @Test
    public void testWholeRowReferenced()
    {
        tester().assertThat(new PruneFilterColumns())
                .on(p -> buildProjectedFilter(p, symbol -> symbol.getName().equals("a")))
                .doesNotFire();
    }

    @Test
    public void testWholeRowAndNestedFieldsReferenced()
    {
        tester().assertThat(new PruneFilterColumns())
                .on(p -> buildProjectedFilter(p, symbol -> true))
                .doesNotFire();
    }

    private ProjectNode buildProjectedFilter(PlanBuilder planBuilder, Predicate<Symbol> projectionFilter)
    {
        Symbol a = planBuilder.symbol("a", from(ImmutableList.of(field("x", BIGINT), field("y", BIGINT), field("z", BIGINT))));
        Assignments assignments = Assignments.builder()
                .put(a, a.toSymbolReference())
                .put(planBuilder.symbol("expr_x"), new DereferenceExpression(a.toSymbolReference(), new Identifier("x")))
                .put(planBuilder.symbol("expr_y"), new DereferenceExpression(a.toSymbolReference(), new Identifier("y")))
                .put(planBuilder.symbol("expr_z"), new DereferenceExpression(a.toSymbolReference(), new Identifier("z")))
                .build();

        return planBuilder.project(
                assignments.filter(projectionFilter),
                planBuilder.filter(
                        planBuilder.expression("a.x > 5"),
                        planBuilder.values(a)));
    }
}
