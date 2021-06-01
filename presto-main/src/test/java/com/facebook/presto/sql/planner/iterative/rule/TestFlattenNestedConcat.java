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

import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.FlattenNestedConcat.flattenExpression;
import static org.testng.Assert.assertEquals;

public class TestFlattenNestedConcat
        extends BasePlanTest
{
    private static void assertRewritten(String from, String to)
    {
        assertEquals(flattenExpression(PlanBuilder.expression(from)), PlanBuilder.expression(to));
    }

    private static void assertNotRewritten(String expression)
    {
        assertEquals(flattenExpression(PlanBuilder.expression(expression)), PlanBuilder.expression(expression));
    }

    @Test
    public void testRewriteMapConcat()
    {
        assertRewritten(
                "map_concat(map(ARRAY[1,2], ARRAY['a','b']), map_concat(map(ARRAY[3,4], ARRAY['c','d']), map(ARRAY[5,6], ARRAY['e', 'f'])))",
                "map_concat(map(ARRAY[1,2], ARRAY['a','b']), map(ARRAY[3,4], ARRAY['c','d']), map(ARRAY[5,6], ARRAY['e', 'f']))");

        assertRewritten(
                "map_concat(map_concat(map_concat(map,map),map),map,map_concat(map,map))",
                "map_concat(map,map,map,map,map,map)");
    }

    @Test
    public void testRewriteArrayConcat()
    {
        assertRewritten(
                "map(array_concat(array_concat(ARRAY[1]),array_concat(ARRAY[2])), ARRAY[3,4])",
                "map(array_concat(ARRAY[1],ARRAY[2]), ARRAY[3,4])");

        assertRewritten(
                "array_concat(array[1,2],array[3,4], array_concat(array[5,6],array_concat(array[7,8])))",
                "array_concat(array[1,2],array[3,4],array[5,6],array[7,8])");
    }

    @Test
    public void testRewriteMapConcatAndArrayConcat()
    {
        // nested map_concat and array_concat
        assertRewritten(
                "map_concat(map(array_concat(array_concat(ARRAY[1]),array_concat(ARRAY[2])), ARRAY['a','b']), map_concat(map(ARRAY[3,4], ARRAY['c','d']), map(ARRAY[5,6], ARRAY['e', 'f'])))",
                "map_concat(map(array_concat(ARRAY[1],ARRAY[2]), ARRAY['a','b']), map(ARRAY[3,4], ARRAY['c','d']), map(ARRAY[5,6], ARRAY['e', 'f']))");
    }

    @Test
    public void testDoesNotRewriteMapConcatAndArrayConcat()
    {
        // assert does not rewrite non direct children functions
        assertNotRewritten(
                "map_concat(map, map(map_concat), map)");

        assertNotRewritten(
                "map_concat(map(map_concat(map,map),map),map,map(map,map))");

        assertNotRewritten(
                "array_concat(array, array[array_concat], array)");
    }

    @Test
    public void testNullValuesArePreserved()
    {
        Expression fromExpression =
                new FunctionCall(
                        QualifiedName.of("map_concat"),
                        ImmutableList.of(
                                new FunctionCall(QualifiedName.of("map_concat"), ImmutableList.of(
                                        new FunctionCall(QualifiedName.of("map"), ImmutableList.of(
                                                new ArrayConstructor(ImmutableList.of(new LongLiteral("1"))),
                                                new ArrayConstructor(ImmutableList.of(new LongLiteral("2"))))),
                                        new FunctionCall(QualifiedName.of("map"), ImmutableList.of(
                                                new ArrayConstructor(ImmutableList.of(new NullLiteral())),
                                                new ArrayConstructor(ImmutableList.of(new NullLiteral())))))),
                                new FunctionCall(QualifiedName.of("map"), ImmutableList.of(
                                        new ArrayConstructor(ImmutableList.of(new LongLiteral("5"))),
                                        new ArrayConstructor(ImmutableList.of(new LongLiteral("6"))))),
                                new FunctionCall(QualifiedName.of("map"), ImmutableList.of(
                                        new ArrayConstructor(ImmutableList.of(new LongLiteral("7"))),
                                        new ArrayConstructor(ImmutableList.of(new LongLiteral("8")))))));

        Expression toExpression =
                new FunctionCall(
                        QualifiedName.of("map_concat"),
                        ImmutableList.of(
                                new FunctionCall(QualifiedName.of("map"), ImmutableList.of(
                                        new ArrayConstructor(ImmutableList.of(new LongLiteral("1"))),
                                        new ArrayConstructor(ImmutableList.of(new LongLiteral("2"))))),
                                new FunctionCall(QualifiedName.of("map"), ImmutableList.of(
                                        new ArrayConstructor(ImmutableList.of(new NullLiteral())),
                                        new ArrayConstructor(ImmutableList.of(new NullLiteral())))),
                                new FunctionCall(QualifiedName.of("map"), ImmutableList.of(
                                        new ArrayConstructor(ImmutableList.of(new LongLiteral("5"))),
                                        new ArrayConstructor(ImmutableList.of(new LongLiteral("6"))))),
                                new FunctionCall(QualifiedName.of("map"), ImmutableList.of(
                                        new ArrayConstructor(ImmutableList.of(new LongLiteral("7"))),
                                        new ArrayConstructor(ImmutableList.of(new LongLiteral("8")))))));

        assertRewritten(fromExpression.toString(), toExpression.toString());
    }

    @Test
    public void testDoesNotChangePlan()
    {
        assertPlan("select map_concat(map(ARRAY[1,2], ARRAY['a','b']), map_concat(map(ARRAY[3,4], ARRAY['c','d']), map(ARRAY[5,6], ARRAY['e', 'f'])))",
                output(
                        project(
                                values())));
    }
}
