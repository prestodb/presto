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

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;

public class TestSimplifyMapFromEntries
        extends BaseRuleTest
{
    private final TestingRowExpressionTranslator translator = new TestingRowExpressionTranslator();

    @Test
    public void testRewriteSingleEntry()
    {
        assertRewritten(
                "map_from_entries(ARRAY[ROW(K1, V1)])",
                "MAP(ARRAY[K1], ARRAY[V1])");
    }

    @Test
    public void testRewriteMultipleEntries()
    {
        assertRewritten(
                "map_from_entries(ARRAY[ROW(K1, V1), ROW(K2, V2)])",
                "MAP(ARRAY[K1, K2], ARRAY[V1, V2])");
    }

    @Test
    public void testNoRewriteVariableArgument()
    {
        Type arrayOfRow = new ArrayType(RowType.anonymous(ImmutableList.of(BIGINT, BIGINT)));
        Map<String, Type> types = ImmutableMap.of("M", arrayOfRow);
        RowExpression inputExpression = translator.translate("map_from_entries(M)", types);
        tester().assertThat(new SimplifyRowExpressions(getMetadata(), getExpressionManager()).projectRowExpressionRewriteRule())
                .on(p -> p.project(
                        assignment(p.variable("x"), inputExpression),
                        p.values(p.variable("M", arrayOfRow))))
                .doesNotFire();
    }

    @Test
    public void testNoRewriteRowKeyType()
    {
        // ROW key types have different null-handling error semantics between
        // map_from_entries and MAP() constructor, so we must skip the rewrite
        RowType rowKeyType = RowType.anonymous(ImmutableList.of(BIGINT, BIGINT));
        Map<String, Type> types = ImmutableMap.of("K1", rowKeyType, "V1", VARCHAR);
        RowExpression inputExpression = translator.translate("map_from_entries(ARRAY[ROW(K1, V1)])", types);
        tester().assertThat(new SimplifyRowExpressions(getMetadata(), getExpressionManager()).projectRowExpressionRewriteRule())
                .on(p -> p.project(
                        assignment(p.variable("x"), inputExpression),
                        p.values(p.variable("K1", rowKeyType), p.variable("V1", VARCHAR))))
                .doesNotFire();
    }

    private void assertRewritten(String inputExpressionStr, String expectedExpressionStr)
    {
        Map<String, Type> types = ImmutableMap.of("K1", BIGINT, "K2", BIGINT, "V1", VARCHAR, "V2", VARCHAR);
        RowExpression inputExpression = translator.translate(inputExpressionStr, types);

        tester().assertThat(new SimplifyRowExpressions(getMetadata(), getExpressionManager()).projectRowExpressionRewriteRule())
                .on(p -> p.project(
                        assignment(p.variable("x"), inputExpression),
                        p.values(p.variable("K1", BIGINT), p.variable("K2", BIGINT), p.variable("V1", VARCHAR), p.variable("V2", VARCHAR))))
                .matches(project(
                        ImmutableMap.of("x", expression(expectedExpressionStr)),
                        values("K1", "K2", "V1", "V2")));
    }
}
