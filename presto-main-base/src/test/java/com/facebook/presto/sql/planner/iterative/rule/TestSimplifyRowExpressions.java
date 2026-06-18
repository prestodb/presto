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
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.nodeManager.PluginNodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.expressions.ExpressionOptimizerManager;
import com.facebook.presto.sql.expressions.JsonCodecRowExpressionSerde;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.OR;
import static com.facebook.presto.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.fail;

public class TestSimplifyRowExpressions
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final MetadataManager METADATA = createTestMetadataManager();
    private RuleTester ruleTester;
    private static final Map<String, Type> TYPES = Streams.concat(
            Stream.of("A", "B", "C", "D", "E", "F", "I", "V", "X", "Y", "Z"),
            IntStream.range(1, 61).boxed().map(i -> format("A%s", i)))
            .collect(toMap(Function.identity(), string -> BOOLEAN));

    @BeforeClass
    public void setUp()
    {
        ruleTester = new RuleTester();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(ruleTester);
        ruleTester = null;
    }

    @Test
    public void testProjectRuleDoesNotFireForVariableReferenceAssignments()
    {
        // ProjectRowExpressionRewrite should not fire when all assignments are
        // VariableReferenceExpression (passthrough variables) since no rewriter
        // transforms a bare variable reference
        SimplifyRowExpressions simplifyRowExpressions = new SimplifyRowExpressions(
                ruleTester.getMetadata(),
                ruleTester.getExpressionManager());
        ruleTester.assertThat(simplifyRowExpressions.projectRowExpressionRewriteRule())
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression y = p.variable("y", BIGINT);
                    VariableReferenceExpression passthrough1 = p.variable("passthrough1", BIGINT);
                    VariableReferenceExpression passthrough2 = p.variable("passthrough2", BIGINT);
                    return p.project(
                            Assignments.builder()
                                    .put(passthrough1, x)
                                    .put(passthrough2, y)
                                    .build(),
                            p.values(x, y));
                })
                .doesNotFire();
    }

    @Test
    public void testLeafExpressionsShortCircuit()
    {
        // SimplifyRowExpressions.rewrite() should return the exact same object
        // for leaf expressions (VariableReferenceExpression and ConstantExpression)
        // without going through the 3-pass pipeline
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        ExpressionOptimizerManager expressionOptimizerManager = new ExpressionOptimizerManager(
                new PluginNodeManager(nodeManager),
                METADATA.getFunctionAndTypeManager(),
                new JsonCodecRowExpressionSerde(jsonCodec(RowExpression.class)));

        VariableReferenceExpression variable = new VariableReferenceExpression(Optional.empty(), "x", BIGINT);
        RowExpression simplifiedVariable = SimplifyRowExpressions.rewrite(variable, METADATA, TEST_SESSION, expressionOptimizerManager);
        assertSame(simplifiedVariable, variable, "VariableReferenceExpression should be returned as-is");

        ConstantExpression constant = new ConstantExpression(42L, BIGINT);
        RowExpression simplifiedConstant = SimplifyRowExpressions.rewrite(constant, METADATA, TEST_SESSION, expressionOptimizerManager);
        assertSame(simplifiedConstant, constant, "ConstantExpression should be returned as-is");
    }

    @Test
    public void testPushesDownNegations()
    {
        assertSimplifies("NOT X", "NOT X");
        assertSimplifies("NOT NOT X", "X");
        assertSimplifies("NOT NOT NOT X", "NOT X");
        assertSimplifies("NOT NOT NOT X", "NOT X");

        assertSimplifies("NOT (X > Y)", "X <= Y");
        assertSimplifies("NOT (X > (NOT NOT Y))", "X <= Y");
        assertSimplifies("X > (NOT NOT Y)", "X > Y");
        assertSimplifies("NOT (X AND Y AND (NOT (Z OR V)))", "(NOT X) OR (NOT Y) OR (Z OR V)");
        assertSimplifies("NOT (X OR Y OR (NOT (Z OR V)))", "(NOT X) AND (NOT Y) AND (Z OR V)");
        assertSimplifies("NOT (X OR Y OR (Z OR V))", "(NOT X) AND (NOT Y) AND ((NOT Z) AND (NOT V))");

        assertSimplifies("NOT (X IS DISTINCT FROM Y)", "NOT (X IS DISTINCT FROM Y)");
    }

    @Test
    public void testNestedExpressions()
    {
        assertSimplifies(
                "(true and coalesce(X, true) IN (true, false)) IN (true, false)",
                "(coalesce(X, true) IN (true, false)) IN (true, false)");
    }

    @Test
    public void testExtractCommonPredicates()
    {
        assertSimplifies("TRUE", "TRUE");
        assertSimplifies("IF(X, X, Y)", "IF(X, X, Y)");

        assertSimplifies("X AND Y", "X AND Y");
        assertSimplifies("X OR Y", "X OR Y");
        assertSimplifies("X AND X", "X");
        assertSimplifies("true AND X", "X");
        assertSimplifies("X OR X", "X");
        assertSimplifies("(X OR Y) AND (X OR Y)", "X OR Y");

        assertSimplifies("(A AND V) OR V", "V");
        assertSimplifies("(A OR V) AND V", "V");
        assertSimplifies("(A OR B OR C) AND (A OR B)", "A OR B");
        assertSimplifies("(A AND B) OR (A AND B AND C)", "A AND B");
        assertSimplifies("I = ((A OR B) AND (A OR B OR C))", "I = (A OR B)");
        assertSimplifies("(X OR Y) AND (X OR Z)", "(X OR Y) AND (X OR Z)");
        assertSimplifies("(X AND Y AND V) OR (X AND Y AND Z)", "(X AND Y) AND (V OR Z)");
        assertSimplifies("((X OR Y OR V) AND (X OR Y OR Z)) = I", "((X OR Y) OR (V AND Z)) = I");

        assertSimplifies("((X OR V) AND V) OR ((X OR V) AND V)", "V");
        assertSimplifies("((X OR V) AND X) OR ((X OR V) AND V)", "X OR V");

        assertSimplifies("((X OR V) AND Z) OR ((X OR V) AND V)", "(X OR V) AND (Z OR V)");

        assertSimplifies("X AND ((Y AND Z) OR (Y AND V) OR (Y AND X))", "X AND Y");

        assertSimplifies("(A AND B AND C AND D) OR (A AND B AND E) OR (A AND F)", "A AND ((B AND C AND D) OR (B AND E) OR F)");
        assertSimplifies("((A AND B) OR (A AND C)) AND D", "A AND (B OR C) AND D");
        assertSimplifies("((A OR B) AND (A OR C)) OR D", "(A OR B OR D) AND (A OR C OR D)");
        assertSimplifies("(((A AND B) OR (A AND C)) AND D) OR E", "(A OR E) AND (B OR C OR E) AND (D OR E)");
        assertSimplifies("(((A OR B) AND (A OR C)) OR D) AND E", "(A OR (B AND C) OR D) AND E");

        assertSimplifies("(A AND B) OR (C AND D)", "(A OR C) AND (A OR D) AND (B OR C) AND (B OR D)");
        // No distribution since it would add too many new terms
        assertSimplifies("(A AND B) OR (C AND D) OR (E AND F)", "(A AND B) OR (C AND D) OR (E AND F)");

        // Test overflow handling for large disjunct expressions
        assertSimplifies("(A1 AND A2) OR (A3 AND A4) OR (A5 AND A6) OR (A7 AND A8) OR (A9 AND A10)" +
                        " OR (A11 AND A12) OR (A13 AND A14) OR (A15 AND A16) OR (A17 AND A18) OR (A19 AND A20)" +
                        " OR (A21 AND A22) OR (A23 AND A24) OR (A25 AND A26) OR (A27 AND A28) OR (A29 AND A30)" +
                        " OR (A31 AND A32) OR (A33 AND A34) OR (A35 AND A36) OR (A37 AND A38) OR (A39 AND A40)" +
                        " OR (A41 AND A42) OR (A43 AND A44) OR (A45 AND A46) OR (A47 AND A48) OR (A49 AND A50)" +
                        " OR (A51 AND A52) OR (A53 AND A54) OR (A55 AND A56) OR (A57 AND A58) OR (A59 AND A60)",
                "(A1 AND A2) OR (A3 AND A4) OR (A5 AND A6) OR (A7 AND A8) OR (A9 AND A10)" +
                        " OR (A11 AND A12) OR (A13 AND A14) OR (A15 AND A16) OR (A17 AND A18) OR (A19 AND A20)" +
                        " OR (A21 AND A22) OR (A23 AND A24) OR (A25 AND A26) OR (A27 AND A28) OR (A29 AND A30)" +
                        " OR (A31 AND A32) OR (A33 AND A34) OR (A35 AND A36) OR (A37 AND A38) OR (A39 AND A40)" +
                        " OR (A41 AND A42) OR (A43 AND A44) OR (A45 AND A46) OR (A47 AND A48) OR (A49 AND A50)" +
                        " OR (A51 AND A52) OR (A53 AND A54) OR (A55 AND A56) OR (A57 AND A58) OR (A59 AND A60)");
    }

    @Test
    public void testSimplifyNestedIf()
    {
        // Basic: IF(X, IF(Y, V, null), null) → IF(X AND Y, V, null)
        assertSimplifies(
                "IF(X, IF(Y, V, CAST(null AS boolean)), CAST(null AS boolean))",
                "IF(X AND Y, V)");

        // Omitted ELSE (defaults to null): IF(X, IF(Y, V))
        assertSimplifies(
                "IF(X, IF(Y, V))",
                "IF(X AND Y, V)");

        // Triple nesting flattened in a single pass (bottom-up)
        assertSimplifies(
                "IF(X, IF(Y, IF(Z, V, CAST(null AS boolean)), CAST(null AS boolean)), CAST(null AS boolean))",
                "IF((X AND Y) AND Z, V)");

        // Matching non-null else branches: IF(X, IF(Y, V, Z), Z) → IF(X AND Y, V, Z)
        assertSimplifies(
                "IF(X, IF(Y, V, Z), Z)",
                "IF(X AND Y, V, Z)");

        // No simplification: else branches differ
        assertSimplifies(
                "IF(X, IF(Y, V, Z), A)",
                "IF(X, IF(Y, V, Z), A)");

        // No simplification: true branch is not an IF
        assertSimplifies(
                "IF(X, V, CAST(null AS boolean))",
                "IF(X, V)");

        // No simplification: inner condition is non-deterministic
        assertSimplifies(
                "IF(X, IF(random() > 0.5e0, V, Z), Z)",
                "IF(X, IF(random() > 0.5e0, V, Z), Z)");
    }

    @Test
    public void testCastBigintToBoundedVarchar()
    {
        // the varchar type length is enough to contain the number's representation
        assertSimplifies("CAST(12300000000 AS varchar(11))", "'12300000000'");
        // The last argument "'-12300000000'" is varchar(12). Need varchar(50) to the following test pass.
        //assertSimplifies("CAST(-12300000000 AS varchar(50))", "CAST('-12300000000' AS varchar(50))", "'-12300000000'");

        // cast from bigint to varchar fails, so the expression is not modified
        try {
            assertSimplifies("CAST(12300000000 AS varchar(3))", "CAST(12300000000 AS varchar(3))");
            fail("Expected to throw an PrestoException exception");
        }
        catch (PrestoException e) {
            try {
                assertEquals(e.getErrorCode(), INVALID_CAST_ARGUMENT.toErrorCode());
                assertEquals(e.getMessage(), "Value 12300000000 cannot be represented as varchar(3)");
            }
            catch (Throwable failure) {
                failure.addSuppressed(e);
                throw failure;
            }
        }

        try {
            assertSimplifies("CAST(-12300000000 AS varchar(3))", "CAST(-12300000000 AS varchar(3))");
        }
        catch (PrestoException e) {
            try {
                assertEquals(e.getErrorCode(), INVALID_CAST_ARGUMENT.toErrorCode());
                assertEquals(e.getMessage(), "Value -12300000000 cannot be represented as varchar(3)");
            }
            catch (Throwable failure) {
                failure.addSuppressed(e);
                throw failure;
            }
        }
    }

    @Test
    public void testMapFromEntriesRewrite()
    {
        // Single entry
        assertMapFromEntries(
                "map_from_entries(ARRAY[ROW(K1, V1)])",
                "MAP(ARRAY[K1], ARRAY[V1])");

        // Multiple entries
        assertMapFromEntries(
                "map_from_entries(ARRAY[ROW(K1, V1), ROW(K2, V2)])",
                "MAP(ARRAY[K1, K2], ARRAY[V1, V2])");
    }

    @Test
    public void testMapFromEntriesNoRewrite()
    {
        // Non-array-constructor argument (variable) — should not rewrite
        Map<String, Type> types = ImmutableMap.of("M", new ArrayType(RowType.anonymous(ImmutableList.of(BIGINT, BIGINT))));
        assertSimplifies("map_from_entries(M)", "map_from_entries(M)", types);
    }

    private static void assertMapFromEntries(String expression, String expected)
    {
        Map<String, Type> types = ImmutableMap.of("K1", BIGINT, "K2", BIGINT, "V1", BIGINT, "V2", BIGINT);
        assertSimplifies(expression, expected, types);
    }

    private static void assertSimplifies(String expression, String rowExpressionExpected)
    {
        assertSimplifies(expression, rowExpressionExpected, TYPES);
    }

    private static void assertSimplifies(String expression, String rowExpressionExpected, Map<String, Type> types)
    {
        Expression actualExpression = rewriteIdentifiersToSymbolReferences(SQL_PARSER.createExpression(expression));

        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        ExpressionOptimizerManager expressionOptimizerManager = new ExpressionOptimizerManager(new PluginNodeManager(nodeManager), METADATA.getFunctionAndTypeManager(), new JsonCodecRowExpressionSerde(jsonCodec(RowExpression.class)));

        TestingRowExpressionTranslator translator = new TestingRowExpressionTranslator(METADATA);
        RowExpression actualRowExpression = translator.translate(actualExpression, TypeProvider.viewOf(types));
        RowExpression simplifiedRowExpression = SimplifyRowExpressions.rewrite(actualRowExpression, METADATA, TEST_SESSION, expressionOptimizerManager);
        Expression expectedByRowExpression = rewriteIdentifiersToSymbolReferences(SQL_PARSER.createExpression(rowExpressionExpected));
        RowExpression simplifiedByExpression = translator.translate(expectedByRowExpression, TypeProvider.viewOf(types));
        assertEquals(normalize(simplifiedRowExpression), normalize(simplifiedByExpression));
    }

    private static RowExpression normalize(RowExpression expression)
    {
        return RowExpressionTreeRewriter.rewriteWith(new NormalizeRowExpressionRewriter(), expression);
    }

    private static class NormalizeRowExpressionRewriter
            extends RowExpressionRewriter<Void>
    {
        @Override
        public RowExpression rewriteSpecialForm(SpecialFormExpression node, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
        {
            if (!node.getForm().equals(AND) && !node.getForm().equals(OR)) {
                return null;
            }
            List<RowExpression> predicates = LogicalRowExpressions.extractPredicates(node.getForm(), node).stream()
                    .map(p -> treeRewriter.rewrite(p, context))
                    .sorted(Comparator.comparing(RowExpression::toString))
                    .collect(toList());
            return specialForm(node.getForm(), node.getType(), predicates);
        }
    }
}
