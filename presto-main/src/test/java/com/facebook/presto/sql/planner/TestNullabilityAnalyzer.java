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
package com.facebook.presto.sql.planner;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.iterative.rule.LambdaCaptureDesugaringRewriter;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Collection;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.RowType.field;
import static com.facebook.presto.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static org.testng.Assert.assertEquals;

public class TestNullabilityAnalyzer
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();
    private static final TestingRowExpressionTranslator TRANSLATOR = new TestingRowExpressionTranslator(METADATA);
    private static final TypeProvider TYPES = TypeProvider.viewOf(new ImmutableMap.Builder<String, Type>()
            .put("a", BIGINT)
            .put("b", new ArrayType(BIGINT))
            .put("c", RowType.from(ImmutableList.of(field("field_1", BIGINT))))
            .build());

    private static final NullabilityAnalyzer analyzer = new NullabilityAnalyzer(METADATA.getFunctionAndTypeManager());

    @Test
    void test()
    {
        assertNullability("TRY_CAST(JSON '123' AS VARCHAR)", true);
        assertNullability("TRY_CAST(a AS VARCHAR)", true);
        assertNullability("CAST(a AS VARCHAR)", true);

        assertNullability("TRY_CAST('123' AS VARCHAR)", false);
        assertNullability("CAST('123' AS VARCHAR)", false);

        assertNullability("a = 1", false);
        assertNullability("(a/9+1)*5-10 > 10", false);
        assertNullability("1", false);
        assertNullability("a", false);
        assertNullability("TRY(a + 1)", true);

        assertNullability("IF(a > 10, 1)", true);
        assertNullability("a IN (1, NULL)", true);
        assertNullability("CASE WHEN a> 10 THEN 1 END", true);
        assertNullability("c.field_1", true);
        assertNullability("b[0]", true);
        assertNullability("a BETWEEN 1 AND 2", false);

        // nested
        assertNullability("1 = TRY(a + 1)", true);
    }

    private void assertNullability(String expression, boolean mayReturnNullForNotNullInput)
    {
        Expression rawExpression = rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(expression, new ParsingOptions()));
        Expression desugaredExpression = new TestingDesugarExpressions(TYPES.allVariables()).rewrite(rawExpression);
        RowExpression rowExpression = TRANSLATOR.translate(desugaredExpression, TYPES);
        assertEquals(analyzer.mayReturnNullOnNonNullInput(rowExpression), mayReturnNullForNotNullInput);
    }

    private static class TestingDesugarExpressions
    {
        private final PlanVariableAllocator variableAllocator;

        public TestingDesugarExpressions(Collection<VariableReferenceExpression> variables)
        {
            this.variableAllocator = new PlanVariableAllocator(variables);
        }

        public Expression rewrite(Expression expression)
        {
            expression = DesugarTryExpressionRewriter.rewrite(expression);
            expression = LambdaCaptureDesugaringRewriter.rewrite(expression, variableAllocator);
            return expression;
        }
    }
}
