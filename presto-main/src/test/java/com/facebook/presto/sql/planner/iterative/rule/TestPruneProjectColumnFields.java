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

import com.facebook.presto.spi.type.Type;
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
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneProjectColumnFields
        extends BaseRuleTest
{
    @Test
    public void testNotAllNestedFieldsReferenced()
    {
        tester().assertThat(new PruneProjectColumnFields())
                .on(p -> buildProject(p, symbol -> symbol.getName().equals("expr_y")))
                .matches(
                        strictProject(
                                ImmutableMap.of("expr", expression("a.y")),
                                strictProject(
                                        ImmutableMap.of("a", expression(new SymbolReference("a", ImmutableSet.of("y")))),
                                        values("a"))));
    }

    @Test
    public void testMixIntermediateRowAndLeafFieldReferenced()
    {
        tester().assertThat(new PruneProjectColumnFields())
                .on(p -> buildProject(p, symbol -> symbol.getName().equals("expr_y") || symbol.getName().equals("expr_i")))
                .matches(
                        strictProject(
                                ImmutableMap.of("expr_y", expression("a.y"), "expr_i", expression("a.y.i")),
                                strictProject(
                                        ImmutableMap.of("a", expression(new SymbolReference("a", ImmutableSet.of("y")))),
                                        values("a"))));
    }

    @Test
    public void testWholeRowReferenced()
    {
        tester().assertThat(new PruneProjectColumnFields())
                .on(p -> buildProject(p, symbol -> symbol.getName().equals("a")))
                .doesNotFire();
    }

    private ProjectNode buildProject(PlanBuilder planBuilder, Predicate<Symbol> projectionFilter)
    {
        Type nestedFieldType = from(ImmutableList.of(field("i", BIGINT), field("j", BIGINT)));
        Type columnType = from(ImmutableList.of(field("x", BIGINT), field("y", nestedFieldType)));
        Symbol a = planBuilder.symbol("a", columnType);
        Symbol y = planBuilder.symbol("expr_y", nestedFieldType);
        Symbol i = planBuilder.symbol("expr_i");
        DereferenceExpression expY = new DereferenceExpression(a.toSymbolReference(), new Identifier("y"));
        DereferenceExpression expI = new DereferenceExpression(expY, new Identifier("i"));
        Assignments assignments = Assignments.builder()
                .put(a, a.toSymbolReference())
                .put(y, expY)
                .put(i, expI)
                .build();
        return planBuilder.project(
                assignments.filter(projectionFilter),
                planBuilder.project(
                        Assignments.identity(a),
                        planBuilder.values(a)));
    }
}
