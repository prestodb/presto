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
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.markDistinct;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignmentsAsSymbolReferences;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.asSymbolReference;

public class TestPruneMarkDistinctColumns
        extends BaseRuleTest
{
    @Test
    public void testMarkerSymbolNotReferenced()
    {
        tester().assertThat(new PruneMarkDistinctColumns())
                .on(p ->
                {
                    VariableReferenceExpression key = p.variable("key");
                    VariableReferenceExpression key2 = p.variable("key2");
                    VariableReferenceExpression mark = p.variable("mark");
                    VariableReferenceExpression unused = p.variable("unused");
                    return p.project(
                            assignment(key2, asSymbolReference(key)),
                            p.markDistinct(mark, ImmutableList.of(key), p.values(key, unused)));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("key2", expression("key")),
                                values(ImmutableList.of("key", "unused"))));
    }

    @Test
    public void testSourceSymbolNotReferenced()
    {
        tester().assertThat(new PruneMarkDistinctColumns())
                .on(p ->
                {
                    VariableReferenceExpression key = p.variable("key");
                    VariableReferenceExpression mark = p.variable("mark");
                    VariableReferenceExpression hash = p.variable("hash");
                    VariableReferenceExpression unused = p.variable("unused");
                    return p.project(
                            identityAssignmentsAsSymbolReferences(mark),
                            p.markDistinct(
                                    mark,
                                    ImmutableList.of(key),
                                    hash,
                                    p.values(key, hash, unused)));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("mark", expression("mark")),
                                markDistinct("mark", ImmutableList.of("key"), "hash",
                                        strictProject(
                                                ImmutableMap.of(
                                                        "key", expression("key"),
                                                        "hash", expression("hash")),
                                                values(ImmutableList.of("key", "hash", "unused"))))));
    }

    @Test
    public void testKeySymbolNotReferenced()
    {
        tester().assertThat(new PruneMarkDistinctColumns())
                .on(p ->
                {
                    VariableReferenceExpression key = p.variable("key");
                    VariableReferenceExpression mark = p.variable("mark");
                    return p.project(
                            identityAssignmentsAsSymbolReferences(mark),
                            p.markDistinct(mark, ImmutableList.of(key), p.values(key)));
                })
                .doesNotFire();
    }

    @Test
    public void testAllOutputsReferenced()
    {
        tester().assertThat(new PruneMarkDistinctColumns())
                .on(p ->
                {
                    VariableReferenceExpression key = p.variable("key");
                    VariableReferenceExpression mark = p.variable("mark");
                    return p.project(
                            identityAssignmentsAsSymbolReferences(key, mark),
                            p.markDistinct(mark, ImmutableList.of(key), p.values(key)));
                })
                .doesNotFire();
    }
}
