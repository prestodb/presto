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
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_CASE_EXPRESSION_PREDICATE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestRewriteCaseExpressionPredicate
        extends BaseRuleTest
{
    private static final MetadataManager METADATA = createTestMetadataManager();
    private static final Map<String, Type> TYPE_MAP = ImmutableMap.of("col1", INTEGER, "col2", INTEGER, "col3", VARCHAR);

    private final TestingRowExpressionTranslator testSqlToRowExpressionTranslator = new TestingRowExpressionTranslator();

    @Test
    public void testRewriterDoesNotFireOnPredicateWithoutCaseExpression()
    {
        assertRewriteDoesNotFire("col1 > 1");
    }

    @Test
    public void testRewriterDoesNotFireOnPredicateWithoutComparisonFunction()
    {
        assertRewriteDoesNotFire("(case when col1=1 then 'case1' when col2=2 then 'case2' else 'default' end)");
    }

    @Test
    public void testRewriterDoesNotFireOnPredicateWithFunctionCallOnComparisonValue()
    {
        assertRewriteDoesNotFire("(case when col1=1 then 'case1' when col2=2 then 'case2' else 'default' end) = UPPER('case1')");
        assertRewriteDoesNotFire("(case when col1=1 then 10 when col1=2 then 20 else 30 end) = ceil(col1)");
        assertRewriteDoesNotFire("(case when col1=1 then 10 when col2=2 then 20 else 30 end) = ceil(col1)");
    }

    @Test
    public void testRewriterDoesNotFireOnSearchCaseExpressionThatDoesNotMeetRewriteConditions()
    {
        // All LHS expressions are not the same
        assertRewriteDoesNotFire("(case when col1=1 then 'case1' when col2=2 then 'case2' else 'default' end) = 'case1'");
        assertRewriteDoesNotFire("(case when col1=1 then 'case1' when ceil(col1)=2 then 'case2' else 'default' end) = 'case1'");

        // Any expression is non deterministic
        assertRewriteDoesNotFire("(case when random(col1)=1 then 'case1' when random(col1)=2 then 'case2' else 'default' end) = 'case1'");
        assertRewriteDoesNotFire("(case when col1=1 then 2 when col1=2 then 3 else 4 end) = rand()");

        // All expressions are not equals function
        assertRewriteDoesNotFire("(case when col1>1 then 1 when col1>2 then 2 else 3 end) > 2");
        assertRewriteDoesNotFire("(case when col1<1 then 1 when col1<2 then 2 else 3 end) < 2");

        // All RHS expressions are not Constant Expression
        assertRewriteDoesNotFire("(case when col1=1 then 'case1' when col1=ceil(1) then 'case2' else 'default' end) = 'case1'");

        // All RHS expressions are not unique
        assertRewriteDoesNotFire("(case when col1=1 then 'case1' when col1=1 then 'case2' else 'default' end) = 'case1'");
        assertRewriteDoesNotFire("(case when col1=CAST(1 as SMALLINT) then 'case1' when col1=CAST(1 as TINYINT) then 'case2' else 'default' end) = 'case1'");

        // RHS expression is NULL
        assertRewriteDoesNotFire("(case when col1=1 then 'case1' when col1=NULL then 'case2' else 'default' end) = 'case1'");
    }

    @Test
    public void testSimpleCaseExpressionRewrite()
    {
        assertRewrittenExpression(
                "(case col1 when 1 then 'case1' when 2 then 'case2' else 'default' end) = 'case1'",
                "('case1' = 'case1' AND col1 IS NOT NULL AND col1 = 1) OR ('case2' = 'case1' AND col1 IS NOT NULL AND col1 = 2) OR ('default' = 'case1' AND (col1 IS NULL OR (NOT(col1 = 1) AND NOT(col1 = 2))))");

        assertRewrittenExpression(
                "(case col1 when 1 then 'case1' when 2 then 'case2' else 'default' end) = 'case2'",
                "('case1' = 'case2' AND col1 IS NOT NULL AND col1 = 1) OR ('case2' = 'case2' AND col1 IS NOT NULL AND col1 = 2) OR ('default' = 'case2' AND (col1 IS NULL OR (NOT(col1 = 1) AND NOT(col1 = 2))))");

        assertRewrittenExpression(
                "(case col1 when 1 then 'case1' when 2 then 'case2' else 'default' end) = 'default'",
                "('case1' = 'default' AND col1 IS NOT NULL AND col1 = 1) OR ('case2' = 'default' AND col1 IS NOT NULL AND col1 = 2) OR ('default' = 'default' AND (col1 IS NULL OR (NOT(col1 = 1) AND NOT(col1 = 2))))");
    }

    @Test
    public void testSearchedCaseExpressionRewrite()
    {
        assertRewrittenExpression(
                "(case when col1=1 then 'case1' when col1=2 then 'case2' else 'default' end) = 'case1'",
                "('case1' = 'case1' AND col1 IS NOT NULL AND col1 = 1) OR ('case2' = 'case1' AND col1 IS NOT NULL AND col1 = 2) OR ('default' = 'case1' AND (col1 IS NULL OR (NOT(col1 = 1) AND NOT(col1 = 2))))");

        assertRewrittenExpression(
                "(case when col3='a' then 'case1' when col3='b' then 'case2' else 'default' end) = 'case1'",
                "('case1' = 'case1' AND col3 IS NOT NULL AND col3 = 'a') OR ('case2' = 'case1' AND col3 IS NOT NULL AND col3 = 'b') OR ('default' = 'case1' AND (col3 IS NULL OR (NOT(col3 = 'a') AND NOT(col3 = 'b'))))");

        assertRewrittenExpression(
                "(case when col1=1 then 'case1' when col1=2 then 'case2' else 'default' end) = 'default'",
                "('case1' = 'default' AND col1 IS NOT NULL AND col1 = 1) OR ('case2' = 'default' AND col1 IS NOT NULL AND col1 = 2) OR ('default' = 'default' AND (col1 IS NULL OR (NOT(col1 = 1) AND NOT(col1 = 2))))");
    }

    @Test
    public void testRewriterOnCaseExpressionInRightSideOfComparisonFunction()
    {
        assertRewrittenExpression(
                "(case col1 when 1 then 10 when 2 then 20 else 30 end) > 20",
                "(10 > 20 AND col1 IS NOT NULL AND col1 = 1) OR (20 > 20 AND col1 IS NOT NULL AND col1 = 2) OR (30 > 20 AND (col1 IS NULL OR (NOT(col1 = 1) AND NOT(col1 = 2))))");

        assertRewrittenExpression(
                "25 < (case col1 when 1 then 10 when 2 then 20 else 30 end)",
                "(25 < 10 AND col1 IS NOT NULL AND col1 = 1) OR (25 < 20 AND col1 IS NOT NULL AND col1 = 2) OR (25 < 30 AND (col1 IS NULL OR (NOT(col1 = 1) AND NOT(col1 = 2))))");
    }

    @Test
    public void testRewriterWhenMoreThanOneConditionMatches()
    {
        assertRewrittenExpression(
                "(case col1 when 1 then 'case' when 2 then 'case' else 'default' end) = 'case'",
                "('case' = 'case' AND col1 IS NOT NULL AND col1 = 1) OR ('case' = 'case' AND col1 IS NOT NULL AND col1 = 2) OR ('default' = 'case' AND (col1 IS NULL OR (NOT(col1 = 1) AND NOT(col1 = 2))))");

        assertRewrittenExpression(
                "(case col1 when 1 then concat('default', 'AndCase1') when 2 then 'case2' else 'defaultAndCase1' end) = 'defaultAndCase1'",
                "(concat('default', 'AndCase1') = 'defaultAndCase1' AND col1 IS NOT NULL AND col1 = 1) OR ('case2' = 'defaultAndCase1' AND col1 IS NOT NULL AND col1 = 2) OR ('defaultAndCase1' = 'defaultAndCase1' AND (col1 IS NULL OR (NOT(col1 = 1) AND NOT(col1 = 2))))");

        assertRewrittenExpression(
                "(case col3 when 'data1' then 'case1' when 'data2' then 'case2' else col3 end) = 'case1'",
                "('case1' = 'case1' AND col3 IS NOT NULL AND col3 = 'data1') OR ('case2' = 'case1' AND col3 IS NOT NULL AND col3 = 'data2') OR (col3 = 'case1' AND (col3 IS NULL OR (NOT(col3 = 'data1') AND NOT(col3 = 'data2'))))");
    }

    @Test
    public void testRewriterOnCaseExpressionWithoutElseClause()
    {
        assertRewrittenExpression(
                "(case col1 when 1 then 'case1' when 2 then 'case2' end) = 'case1'",
                "('case1' = 'case1' AND col1 IS NOT NULL AND col1 = 1) OR ('case2' = 'case1' AND col1 IS NOT NULL AND col1 = 2) OR (null = 'case1' AND (col1 IS NULL OR (NOT(col1 = 1) AND NOT(col1 = 2))))");

        assertRewrittenExpression(
                "(case col1 when 1 then 'case1' when 2 then 'case2' end) = 'case3'",
                "('case1' = 'case3' AND col1 IS NOT NULL AND col1 = 1) OR ('case2' = 'case3' AND col1 IS NOT NULL AND col1 = 2) OR (null = 'case3' AND (col1 IS NULL OR (NOT(col1 = 1) AND NOT(col1 = 2))))");

        assertRewrittenExpression(
                "(case col1 when 1 then 'case1' when 2 then 'case2' end) = 'case2'",
                "('case1' = 'case2' AND col1 IS NOT NULL AND col1 = 1) OR ('case2' = 'case2' AND col1 IS NOT NULL AND col1 = 2) OR (null = 'case2' AND (col1 IS NULL OR (NOT(col1 = 1) AND NOT(col1 = 2))))");
    }

    @Test
    public void testRewriterOnCaseExpressionWithCastFunction()
    {
        // When left hand and right hand side of the expression are of different types, RowExpressionInterpreter identifies the common super type and adds a CAST function
        assertRewrittenExpression(
                "cast((case col1 when 1 then 'case11' when 2 then 'case2' else 'def' end) as VARCHAR(6)) = 'case11'",
                "(cast('case11' as VARCHAR(6)) = 'case11' AND col1 IS NOT NULL AND col1 = 1) OR (cast('case2' as VARCHAR(6)) = 'case11' AND col1 IS NOT NULL AND col1 = 2) OR (cast('def' as VARCHAR(6)) = 'case11' AND (col1 IS NULL OR (NOT(col1 = 1) AND NOT(col1 = 2))))");

        assertRewrittenExpression(
                "(case col1 when 1 then 'case1' when 2 then 'case2' else 'default' end) = cast('case1' AS VARCHAR)",
                "('case1' = cast('case1' AS VARCHAR) AND col1 IS NOT NULL AND col1 = 1) OR ('case2' = cast('case1' AS VARCHAR) AND col1 IS NOT NULL AND col1 = 2) OR ('default' = cast('case1' AS VARCHAR) AND (col1 IS NULL OR (NOT(col1 = 1) AND NOT(col1 = 2))))");

        assertRewrittenExpression(
                "(case when col1=cast('1' as INTEGER) then 'case1' when col1=cast('2' as INTEGER) then 'case2' else 'default' end) = 'case1'",
                "('case1' = 'case1' AND col1 IS NOT NULL AND col1 = cast('1' as INTEGER)) OR ('case2' = 'case1' AND col1 IS NOT NULL AND col1 = cast('2' as INTEGER)) OR ('default' = 'case1' AND (col1 IS NULL OR (NOT(col1 = cast('1' as INTEGER)) AND NOT(col1 = cast('2' as INTEGER)))))");
    }

    @Test
    public void testIfSubExpressionsAreRewritten()
    {
        assertRewrittenExpression(
                "((case col1 when 1 then 'a' else 'b' end) = 'a') = true",
                "(('a' = 'a' AND col1 IS NOT NULL AND col1 = 1) OR ('b' = 'a' AND (col1 IS NULL OR NOT(col1=1)))) = true");
    }

    private void assertRewriteDoesNotFire(String expression)
    {
        tester().assertThat(new RewriteCaseExpressionPredicate(METADATA).filterRowExpressionRewriteRule())
                .setSystemProperty(OPTIMIZE_CASE_EXPRESSION_PREDICATE, "true")
                .on(p -> p.filter(testSqlToRowExpressionTranslator.translate(expression, TYPE_MAP), p.values()))
                .doesNotFire();
    }

    private void assertRewrittenExpression(String inputExpressionStr,
                                           String expectedExpressionStr)
    {
        RowExpression inputExpression = testSqlToRowExpressionTranslator.translate(inputExpressionStr, TYPE_MAP);

        tester().assertThat(new RewriteCaseExpressionPredicate(METADATA).filterRowExpressionRewriteRule())
                .setSystemProperty(OPTIMIZE_CASE_EXPRESSION_PREDICATE, "true")
                .on(p -> p.filter(inputExpression, p.values(p.variable("col1"), p.variable("col2"), p.variable("col3"))))
                .matches(filter(expectedExpressionStr, values("col1", "col2", "col3")));
    }
}
