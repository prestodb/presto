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
package com.facebook.presto.sql;

import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NodeRef;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.math.BigDecimal;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.Decimals.encodeScaledValue;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.AMBIGUOUS_ATTRIBUTE;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

public class TestSqlToRowExpressionTranslator
{
    private final TestingRowExpressionTranslator translator = new TestingRowExpressionTranslator();

    @Test
    public void testExactCaseRowFieldDereference()
    {
        RowType rowType = RowType.from(ImmutableList.of(
                RowType.field("currencyCode", VARCHAR),
                RowType.field("currencycode", BIGINT)));
        ImmutableMap<String, Type> types = ImmutableMap.of("x", rowType);

        assertEquals(translator.translate("x.\"currencyCode\"", types).getType(), VARCHAR);
        assertEquals(translator.translate("x.\"currencycode\"", types).getType(), BIGINT);
        SemanticException ambiguousFallback = expectThrows(SemanticException.class, () -> translator.translate("x.\"CURRENCYCODE\"", types));
        assertEquals(ambiguousFallback.getCode(), AMBIGUOUS_ATTRIBUTE);

        for (String fieldName : ImmutableList.of("currencyCode", "currencycode", "CURRENCYCODE")) {
            SemanticException ambiguousUnquoted = expectThrows(SemanticException.class, () -> translator.translate("x." + fieldName, types));
            assertEquals(ambiguousUnquoted.getCode(), AMBIGUOUS_ATTRIBUTE);
        }

        RowType duplicateExactNames = RowType.from(ImmutableList.of(
                RowType.field("currencyCode", VARCHAR),
                RowType.field("currencyCode", BIGINT)));
        SemanticException ambiguousExactMatch = expectThrows(SemanticException.class, () -> translator.translate(
                "x.\"currencyCode\"",
                ImmutableMap.of("x", duplicateExactNames)));
        assertEquals(ambiguousExactMatch.getCode(), AMBIGUOUS_ATTRIBUTE);
    }

    @Test(timeOut = 10_000)
    public void testPossibleExponentialOptimizationTime()
    {
        Expression expression = new LongLiteral("1");
        ImmutableMap.Builder<NodeRef<Expression>, Type> types = ImmutableMap.builder();
        types.put(NodeRef.of(expression), BIGINT);
        for (int i = 0; i < 100; i++) {
            expression = new CoalesceExpression(expression, new LongLiteral("2"));
            types.put(NodeRef.of(expression), BIGINT);
        }
        translator.translateAndOptimize(expression, types.build());
    }

    @Test
    public void testRewriteIsNotNullPredicate()
    {
        assertEquals(
                translator.translate("x is NOT NULL", ImmutableMap.of("x", BIGINT)),
                translator.translate("NOT(x IS NULL)", ImmutableMap.of("x", BIGINT)));
    }

    @Test
    public void testRewriteCurrentTime()
    {
        assertEquals(
                translator.translate("CURRENT_TIME", ImmutableMap.of()),
                translator.translate("\"current_time\"()", ImmutableMap.of()));
    }

    @Test
    public void testRewriteCurrentUser()
    {
        assertEquals(
                translator.translate("CURRENT_USER", ImmutableMap.of()),
                translator.translate("\"$current_user\"()", ImmutableMap.of()));
    }

    @Test
    public void testRewriteYearExtract()
    {
        assertEquals(
                translator.translate("EXTRACT(YEAR FROM CURRENT_DATE)", ImmutableMap.of()),
                translator.translate("year(\"current_date\"())", ImmutableMap.of()));
    }

    @Test
    public void testTry()
    {
        assertEquals(
                translator.translate("1 + try(2)", ImmutableMap.of()),
                translator.translate("1 + \"$internal$try\"(() -> 2)", ImmutableMap.of()));
    }

    @Test
    public void testOptimizeDecimalLiteral()
    {
        // Short decimal
        assertEquals(translator.translateAndOptimize(expression("CAST(NULL AS DECIMAL(7,2))")), constant(null, createDecimalType(7, 2)));
        assertEquals(translator.translateAndOptimize(expression("DECIMAL '42'")), constant(42L, createDecimalType(2, 0)));
        assertEquals(translator.translateAndOptimize(expression("CAST(42 AS DECIMAL(7,2))")), constant(4200L, createDecimalType(7, 2)));
        assertEquals(translator.translateAndOptimize(translator.simplifyExpression(expression("CAST(42 AS DECIMAL(7,2))"))), constant(4200L, createDecimalType(7, 2)));

        // Long decimal
        assertEquals(translator.translateAndOptimize(expression("CAST(NULL AS DECIMAL(35,2))")), constant(null, createDecimalType(35, 2)));
        assertEquals(
                translator.translateAndOptimize(expression("DECIMAL '123456789012345678901234567890'")),
                constant(encodeScaledValue(new BigDecimal("123456789012345678901234567890")), createDecimalType(30, 0)));
        assertEquals(
                translator.translateAndOptimize(expression("CAST(DECIMAL '123456789012345678901234567890' AS DECIMAL(35,2))")),
                constant(encodeScaledValue(new BigDecimal("123456789012345678901234567890.00")), createDecimalType(35, 2)));
        assertEquals(
                translator.translateAndOptimize(translator.simplifyExpression(expression("CAST(DECIMAL '123456789012345678901234567890' AS DECIMAL(35,2))"))),
                constant(encodeScaledValue(new BigDecimal("123456789012345678901234567890.00")), createDecimalType(35, 2)));
    }
}
