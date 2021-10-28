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

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.assertions.ExpressionMatcher;
import com.facebook.presto.sql.planner.assertions.RowNumberSymbolMatcher;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.OFFSET_CLAUSE_ENABLED;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.rowNumber;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.sort;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.tree.SortItem.NullOrdering.FIRST;
import static com.facebook.presto.sql.tree.SortItem.Ordering.ASCENDING;

public class TestImplementOffset
        extends BaseRuleTest
{
    @Test
    public void testReplaceOffsetOverValues()
    {
        tester().assertThat(new ImplementOffset())
                .setSystemProperty(OFFSET_CLAUSE_ENABLED, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    return p.offset(
                            2,
                            p.values(a, b));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("a", new ExpressionMatcher("a"), "b", new ExpressionMatcher("b")),
                                filter(
                                        "row_num > BIGINT '2'",
                                        rowNumber(
                                                pattern -> pattern
                                                        .partitionBy(ImmutableList.of()),
                                                values("a", "b"))
                                                .withAlias("row_num", new RowNumberSymbolMatcher()))));
    }

    @Test
    public void testReplaceOffsetOverSort()
    {
        tester().assertThat(new ImplementOffset())
                .setSystemProperty(OFFSET_CLAUSE_ENABLED, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    return p.offset(
                            2,
                            p.sort(
                                    ImmutableList.of(a),
                                    p.values(a, b)));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("a", new ExpressionMatcher("a"), "b", new ExpressionMatcher("b")),
                                filter(
                                        "row_num > BIGINT '2'",
                                        rowNumber(
                                                pattern -> pattern
                                                        .partitionBy(ImmutableList.of()),
                                                sort(
                                                        ImmutableList.of(sort("a", ASCENDING, FIRST)),
                                                        values("a", "b")))
                                                .withAlias("row_num", new RowNumberSymbolMatcher()))));
    }
}
