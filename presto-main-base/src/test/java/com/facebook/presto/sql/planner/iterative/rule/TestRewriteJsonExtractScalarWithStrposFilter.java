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

import static com.facebook.presto.SystemSessionProperties.REWRITE_JSON_EXTRACT_SCALAR_STRPOS_FILTER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestRewriteJsonExtractScalarWithStrposFilter
        extends BaseRuleTest
{
    private static final MetadataManager METADATA = createTestMetadataManager();
    private static final Map<String, Type> TYPE_MAP = ImmutableMap.of("col", VARCHAR);

    private final TestingRowExpressionTranslator testSqlToRowExpressionTranslator = new TestingRowExpressionTranslator();

    @Test
    public void testBasicRewrite()
    {
        assertRewrittenExpression(
                "json_extract_scalar(col, '$.key') = 'some_value'",
                "strpos(col, 'some_value') > 0 AND json_extract_scalar(col, '$.key') = 'some_value'");
    }

    @Test
    public void testReversedOperands()
    {
        assertRewrittenExpression(
                "'some_value' = json_extract_scalar(col, '$.key')",
                "strpos(col, 'some_value') > 0 AND 'some_value' = json_extract_scalar(col, '$.key')");
    }

    @Test
    public void testMultipleConjuncts()
    {
        assertRewrittenExpression(
                "json_extract_scalar(col, '$.a') = 'foo' AND json_extract_scalar(col, '$.b') = 'bar'",
                "strpos(col, 'foo') > 0 AND json_extract_scalar(col, '$.a') = 'foo' AND strpos(col, 'bar') > 0 AND json_extract_scalar(col, '$.b') = 'bar'");
    }

    @Test
    public void testDoesNotFireForUnsafeStringWithQuote()
    {
        // Double quote is JSON-escapable — not safe for strpos optimization
        assertRewriteDoesNotFire("json_extract_scalar(col, '$.key') = 'he\"llo'");
    }

    @Test
    public void testDoesNotFireForUnsafeStringWithBackslash()
    {
        // Backslash is JSON-escapable — not safe for strpos optimization
        assertRewriteDoesNotFire("json_extract_scalar(col, '$.key') = 'path\\to'");
    }

    @Test
    public void testDoesNotFireForNonEqualityComparison()
    {
        assertRewriteDoesNotFire("json_extract_scalar(col, '$.key') > 'some_value'");
        assertRewriteDoesNotFire("json_extract_scalar(col, '$.key') != 'some_value'");
    }

    @Test
    public void testDoesNotFireForNonConstantComparison()
    {
        Map<String, Type> twoColTypes = ImmutableMap.of("col", VARCHAR, "other_col", VARCHAR);
        RowExpression inputExpression = testSqlToRowExpressionTranslator.translate(
                "json_extract_scalar(col, '$.key') = other_col",
                twoColTypes);

        tester().assertThat(new RewriteJsonExtractScalarWithStrposFilter(getFunctionManager()).filterRowExpressionRewriteRule())
                .setSystemProperty(REWRITE_JSON_EXTRACT_SCALAR_STRPOS_FILTER, "true")
                .on(p -> p.filter(inputExpression, p.values(p.variable("col", VARCHAR), p.variable("other_col", VARCHAR))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithoutSessionProperty()
    {
        RowExpression inputExpression = testSqlToRowExpressionTranslator.translate(
                "json_extract_scalar(col, '$.key') = 'some_value'",
                TYPE_MAP);

        tester().assertThat(new RewriteJsonExtractScalarWithStrposFilter(getFunctionManager()).filterRowExpressionRewriteRule())
                .setSystemProperty(REWRITE_JSON_EXTRACT_SCALAR_STRPOS_FILTER, "false")
                .on(p -> p.filter(inputExpression, p.values(p.variable("col", VARCHAR))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForNonJsonExtractScalar()
    {
        // Regular string equality should not be rewritten
        assertRewriteDoesNotFire("col = 'some_value'");
    }

    @Test
    public void testDoesNotFireForEmptyString()
    {
        assertRewriteDoesNotFire("json_extract_scalar(col, '$.key') = ''");
    }

    private void assertRewriteDoesNotFire(String expression)
    {
        RowExpression inputExpression = testSqlToRowExpressionTranslator.translate(expression, TYPE_MAP);

        tester().assertThat(new RewriteJsonExtractScalarWithStrposFilter(getFunctionManager()).filterRowExpressionRewriteRule())
                .setSystemProperty(REWRITE_JSON_EXTRACT_SCALAR_STRPOS_FILTER, "true")
                .on(p -> p.filter(inputExpression, p.values(p.variable("col", VARCHAR))))
                .doesNotFire();
    }

    private void assertRewrittenExpression(String inputExpressionStr, String expectedExpressionStr)
    {
        RowExpression inputExpression = testSqlToRowExpressionTranslator.translate(inputExpressionStr, TYPE_MAP);

        tester().assertThat(new RewriteJsonExtractScalarWithStrposFilter(getFunctionManager()).filterRowExpressionRewriteRule())
                .setSystemProperty(REWRITE_JSON_EXTRACT_SCALAR_STRPOS_FILTER, "true")
                .on(p -> p.filter(inputExpression, p.values(p.variable("col", VARCHAR))))
                .matches(filter(expectedExpressionStr, values("col")));
    }
}
