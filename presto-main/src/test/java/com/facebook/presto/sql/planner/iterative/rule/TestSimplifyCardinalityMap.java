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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.PlannerUtils.createMapType;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;

public class TestSimplifyCardinalityMap
        extends BaseRuleTest
{
    private final TestingRowExpressionTranslator testSqlToRowExpressionTranslator = new TestingRowExpressionTranslator();

    @Test
    public void testRewriteMapValuesCardinality()
    {
        assertRewritten("cardinality(map_values(m))", "cardinality(m)");
    }

    @Test
    public void testRewriteMapValuesMixedCasesCardinality()
    {
        assertRewritten("CaRDinality(map_keys(m))", "cardinaLITY(m)");
    }

    @Test
    public void testNoRewriteMapValuesCardinality()
    {
        assertRewriteDoesNotFire("cardinality(map(ARRAY[1,3], ARRAY[2,4]))");
    }

    @Test
    public void testNestedRewriteMapValuesCardinality()
    {
        assertRewritten(
                "cardinality(map(ARRAY[cardinality(map_values(m_1))], ARRAY[cardinality(map_values(m_2))]))",
                "cardinality(map(ARRAY[cardinality(m_1)], ARRAY[cardinality(m_2)]))");
    }

    @Test
    public void testNestedRewriteMapKeysCardinality()
    {
        assertRewritten(
                "cardinality(map(ARRAY[cardinality(map_keys(m_1)),3], ARRAY[2,cardinality(map_keys(m_2))]))",
                "cardinality(map(ARRAY[cardinality(m_1),3], ARRAY[2,cardinality(m_2)]))");
    }

    @Test
    public void testAnotherNestedRewriteMapValuesCardinality()
    {
        assertRewritten(
                "cardinality(map(ARRAY[cardinality(map_values(map(ARRAY[1,3], ARRAY[2,4]))),3], ARRAY[2,cardinality(map_values(m_2))]))",
                "cardinality(map(ARRAY[cardinality(map(ARRAY[1,3], ARRAY[2,4])),3], ARRAY[2,cardinality(m_2)]))");
    }

    private void assertRewriteDoesNotFire(String expression)
    {
        RowExpression inputExpression = testSqlToRowExpressionTranslator.translate(expression, ImmutableMap.of());
        tester().assertThat(new SimplifyCardinalityMap().projectRowExpressionRewriteRule())
                .on(p -> p.project(assignment(p.variable("x"), inputExpression), p.values()))
                .doesNotFire();
    }

    private void assertRewritten(String inputExpressionStr,
                                           String expectedExpressionStr)
    {
        Type mapType = createMapType(getFunctionManager(), BIGINT, BIGINT);
        Map<String, Type> types = ImmutableMap.of("m", mapType, "m_1", mapType, "m_2", mapType);
        RowExpression inputExpression = testSqlToRowExpressionTranslator.translate(inputExpressionStr, types);

        tester().assertThat(new SimplifyCardinalityMap().projectRowExpressionRewriteRule())
                .on(p -> p.project(assignment(p.variable("x"), inputExpression), p.values(p.variable("m", mapType), p.variable("m_1", mapType), p.variable("m_2", mapType))))
                .matches(project(ImmutableMap.of("x", expression(expectedExpressionStr)), values("m", "m_1", "m_2")));
    }
}
