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

import com.facebook.presto.Session;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import com.facebook.presto.sql.planner.DesugarAtTimeZoneRewriter;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.Map;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.SystemSessionProperties.LEGACY_TIMESTAMP_WITH_TIMEZONE;
import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_ENABLED;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.Decimals.encodeScaledValue;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

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
    public void testNativeAtTimeZoneUsesCanonicalFunction()
    {
        for (boolean legacyTimestampWithTimezone : new boolean[] {true, false}) {
            MetadataManager metadata = MetadataManager.createTestMetadataManager(
                    new FunctionsConfig().setLegacyTimestampWithTimezone(legacyTimestampWithTimezone));
            Session session = Session.builder(TEST_SESSION)
                    .setSystemProperty(LEGACY_TIMESTAMP_WITH_TIMEZONE, String.valueOf(legacyTimestampWithTimezone))
                    .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true")
                    .build();
            TestingRowExpressionTranslator translator = new TestingRowExpressionTranslator(metadata, session);
            Expression atTimeZone = expression("CURRENT_TIMESTAMP AT TIME ZONE 'UTC'");

            RowExpression translated = translator.translate(atTimeZone, TypeProvider.empty());
            assertTrue(translated instanceof CallExpression);
            assertEquals(((CallExpression) translated).getDisplayName(), "at_timezone");
        }
    }

    @Test
    public void testNativeAtTimeZoneRejectsSessionModeMismatch()
    {
        for (boolean configuredLegacyTimestampWithTimezone : new boolean[] {true, false}) {
            MetadataManager metadata = MetadataManager.createTestMetadataManager(
                    new FunctionsConfig().setLegacyTimestampWithTimezone(configuredLegacyTimestampWithTimezone));
            Session session = Session.builder(TEST_SESSION)
                    .setSystemProperty(LEGACY_TIMESTAMP_WITH_TIMEZONE, String.valueOf(!configuredLegacyTimestampWithTimezone))
                    .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true")
                    .build();
            TestingRowExpressionTranslator translator = new TestingRowExpressionTranslator(metadata, session);

            PrestoException exception = expectThrows(
                    PrestoException.class,
                    () -> translator.translate(expression("CURRENT_TIMESTAMP AT TIME ZONE 'UTC'"), TypeProvider.empty()));
            assertEquals(exception.getErrorCode(), INVALID_SESSION_PROPERTY.toErrorCode());
        }
    }

    @Test
    public void testNativeTimeAtTimeZoneAllowsSessionModeMismatch()
    {
        for (boolean configuredLegacyTimestampWithTimezone : new boolean[] {true, false}) {
            MetadataManager metadata = MetadataManager.createTestMetadataManager(
                    new FunctionsConfig().setLegacyTimestampWithTimezone(configuredLegacyTimestampWithTimezone));
            Session session = Session.builder(TEST_SESSION)
                    .setSystemProperty(LEGACY_TIMESTAMP_WITH_TIMEZONE, String.valueOf(!configuredLegacyTimestampWithTimezone))
                    .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true")
                    .build();
            TestingRowExpressionTranslator translator = new TestingRowExpressionTranslator(metadata, session);

            for (String sql : new String[] {
                    "TIME '10:00:00' AT TIME ZONE '+02:00'",
                    "TIME '10:00:00 +00:00' AT TIME ZONE '+02:00'"}) {
                RowExpression translated = translator.translate(expression(sql), TypeProvider.empty());
                assertTrue(translated instanceof CallExpression);
                assertEquals(((CallExpression) translated).getDisplayName(), "at_timezone");
            }
        }
    }

    @Test
    public void testJavaAtTimeZoneUsesConversionFunction()
    {
        MetadataManager metadata = MetadataManager.createTestMetadataManager();
        Session session = Session.builder(TEST_SESSION)
                .setSystemProperty(LEGACY_TIMESTAMP_WITH_TIMEZONE, "false")
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "false")
                .build();
        TestingRowExpressionTranslator translator = new TestingRowExpressionTranslator(metadata, session);
        Expression atTimeZone = expression("CURRENT_TIMESTAMP AT TIME ZONE 'UTC'");

        RowExpression translated = translator.translate(atTimeZone, TypeProvider.empty());
        assertTrue(translated instanceof CallExpression);
        assertEquals(((CallExpression) translated).getDisplayName(), "at_timezone_convert");
    }

    @Test
    public void testAtTimeZoneDesugaringUsesJavaFunction()
    {
        MetadataManager metadata = MetadataManager.createTestMetadataManager();
        Session session = Session.builder(TEST_SESSION)
                .setSystemProperty(LEGACY_TIMESTAMP_WITH_TIMEZONE, "false")
                .build();
        TestingRowExpressionTranslator translator = new TestingRowExpressionTranslator(metadata, session);
        Expression atTimeZone = expression("CURRENT_TIMESTAMP AT TIME ZONE 'UTC'");
        Map<NodeRef<Expression>, Type> expressionTypes = translator.getExpressionTypes(atTimeZone, TypeProvider.empty());

        FunctionCall rewritten = (FunctionCall) DesugarAtTimeZoneRewriter.rewrite(
                atTimeZone,
                expressionTypes);
        assertEquals(rewritten.getName(), QualifiedName.of("at_timezone_convert"));
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
