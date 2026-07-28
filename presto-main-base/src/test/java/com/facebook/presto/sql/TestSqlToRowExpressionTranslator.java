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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NodeRef;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.math.BigDecimal;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.CharType.createCharType;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.Decimals.encodeScaledValue;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestSqlToRowExpressionTranslator
{
    private final TestingRowExpressionTranslator translator = new TestingRowExpressionTranslator();

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
    public void testRewriteConstantLikeWithEscapeToEquals()
    {
        // The exact shape JDBC drivers emit for getColumns/getTables: an escaped underscore is a
        // literal, so the whole predicate is equality. Rewriting it lets metadata-listing connectors
        // (system.jdbc.*) prune to one table instead of scanning the whole catalog.
        assertRewrittenToEquals("x LIKE 'dex\\_views' ESCAPE '\\'", "dex_views");
    }

    @Test
    public void testRewriteConstantLikeNoWildcardToEquals()
    {
        assertRewrittenToEquals("x LIKE 'refresh'", "refresh");
    }

    @Test
    public void testRewriteConstantLikeEscapedPercentToEquals()
    {
        assertRewrittenToEquals("x LIKE '50\\%' ESCAPE '\\'", "50%");
    }

    @Test
    public void testConstantLikeWithWildcardNotRewritten()
    {
        // Genuine wildcards must not collapse to "column = literal": unescaped '_' (single-char match),
        // '%' (any sequence), and an escaped literal followed by a real wildcard.
        assertNotRewrittenToColumnEquals("x LIKE 'dex_views'", VARCHAR);
        assertNotRewrittenToColumnEquals("x LIKE 'refresh%'", VARCHAR);
        assertNotRewrittenToColumnEquals("x LIKE 'dex\\_views%' ESCAPE '\\'", VARCHAR);
    }

    @Test
    public void testRewriteConstantLikeOnBoundedVarchar()
    {
        // A literal longer than the column's declared length still rewrites: the constant is unbounded
        // VARCHAR, and varchar comparison ignores the declared length, so the equality is false -- the
        // same answer LIKE gives. Declaring the constant as varchar(3) would make its slice exceed its
        // own type bound.
        RowExpression translated = assertRewrittenToEquals("x LIKE 'abcdef'", "abcdef", createVarcharType(3));
        assertEquals(((CallExpression) translated).getArguments().get(1).getType(), VARCHAR);
    }

    @Test
    public void testConstantLikeOnCharNotRewritten()
    {
        // CHAR equality pads with trailing spaces; LIKE does not. "x LIKE 'ab'" is false for char(3)
        // 'ab ', but "x = 'ab'" would be true, so the rewrite must not apply.
        assertNotRewrittenToColumnEquals("x LIKE 'ab'", createCharType(3));
    }

    private RowExpression assertRewrittenToEquals(String likeSql, String expectedLiteral)
    {
        return assertRewrittenToEquals(likeSql, expectedLiteral, VARCHAR);
    }

    private RowExpression assertRewrittenToEquals(String likeSql, String expectedLiteral, Type valueType)
    {
        RowExpression translated = translator.translate(likeSql, ImmutableMap.of("x", valueType));
        assertTrue(isColumnEquals(translated), "expected \"x = literal\", got: " + translated);
        RowExpression rhs = ((CallExpression) translated).getArguments().get(1);
        assertTrue(rhs instanceof ConstantExpression, "expected a constant right-hand side, got: " + rhs);
        assertTrue(((ConstantExpression) rhs).getValue() instanceof Slice);
        assertEquals(((ConstantExpression) rhs).getValue(), utf8Slice(expectedLiteral));
        return translated;
    }

    private void assertNotRewrittenToColumnEquals(String likeSql, Type valueType)
    {
        RowExpression translated = translator.translate(likeSql, ImmutableMap.of("x", valueType));
        assertFalse(
                isColumnEquals(translated),
                "expected LIKE to be preserved, but it collapsed to \"x = literal\": " + translated);
    }

    // True only for "<column> = <expr>" (the shape this rewrite produces). Distinguishes it from the
    // pre-existing prefix/suffix optimization, which compares a SUBSTR/STRPOS call rather than the column.
    private static boolean isColumnEquals(RowExpression expression)
    {
        if (!(expression instanceof CallExpression)) {
            return false;
        }
        CallExpression call = (CallExpression) expression;
        return "=".equals(call.getDisplayName())
                && call.getArguments().size() == 2
                && call.getArguments().get(0) instanceof VariableReferenceExpression;
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
