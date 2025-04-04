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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.Set;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestExpressionEquivalence
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final MetadataManager METADATA = MetadataManager.createTestMetadataManager();
    private static final ExpressionEquivalence EQUIVALENCE = new ExpressionEquivalence(METADATA, SQL_PARSER);

    @Test
    public void testEquivalent()
    {
        assertEquivalent("CAST(null AS BIGINT)", "CAST(null as BIGINT)");
        assertEquivalent("a_bigint < b_double", "b_double > a_bigint");
        assertEquivalent("true", "true");
        assertEquivalent("4", "4");
        assertEquivalent("4.4", "4.4");
        assertEquivalent("'foo'", "'foo'");

        assertEquivalent("4 = 5", "5 = 4");
        assertEquivalent("4.4 = 5.5", "5.5 = 4.4");
        assertEquivalent("'foo' = 'bar'", "'bar' = 'foo'");
        assertEquivalent("4 <> 5", "5 <> 4");
        assertEquivalent("4 is distinct from 5", "5 is distinct from 4");
        assertEquivalent("4 < 5", "5 > 4");
        assertEquivalent("4 <= 5", "5 >= 4");

        assertEquivalent("mod(4, 5)", "mod(4, 5)");

        assertEquivalent("a_bigint", "a_bigint");
        assertEquivalent("a_bigint = b_bigint", "b_bigint = a_bigint");
        assertEquivalent("a_bigint < b_bigint", "b_bigint > a_bigint");

        assertEquivalent("a_bigint < b_double", "b_double > a_bigint");

        assertEquivalent("true and false", "false and true");
        assertEquivalent("4 <= 5 and 6 < 7", "7 > 6 and 5 >= 4");
        assertEquivalent("4 <= 5 or 6 < 7", "7 > 6 or 5 >= 4");
        assertEquivalent("a_bigint <= b_bigint and c_bigint < d_bigint", "d_bigint > c_bigint and b_bigint >= a_bigint");
        assertEquivalent("a_bigint <= b_bigint or c_bigint < d_bigint", "d_bigint > c_bigint or b_bigint >= a_bigint");

        assertEquivalent("4 <= 5 and 4 <= 5", "4 <= 5");
        assertEquivalent("4 <= 5 and 6 < 7", "7 > 6 and 5 >= 4 and 5 >= 4");
        assertEquivalent("2 <= 3 and 4 <= 5 and 6 < 7", "7 > 6 and 5 >= 4 and 3 >= 2");

        assertEquivalent("4 <= 5 or 4 <= 5", "4 <= 5");
        assertEquivalent("4 <= 5 or 6 < 7", "7 > 6 or 5 >= 4 or 5 >= 4");
        assertEquivalent("2 <= 3 or 4 <= 5 or 6 < 7", "7 > 6 or 5 >= 4 or 3 >= 2");

        assertEquivalent("a_boolean and b_boolean and c_boolean", "c_boolean and b_boolean and a_boolean");
        assertEquivalent("(a_boolean and b_boolean) and c_boolean", "(c_boolean and b_boolean) and a_boolean");
        assertEquivalent("a_boolean and (b_boolean or c_boolean)", "a_boolean and (c_boolean or b_boolean) and a_boolean");

        assertEquivalent(
                "(a_boolean or b_boolean or c_boolean) and (d_boolean or e_boolean) and (f_boolean or g_boolean or h_boolean)",
                "(h_boolean or g_boolean or f_boolean) and (b_boolean or a_boolean or c_boolean) and (e_boolean or d_boolean)");

        assertEquivalent(
                "(a_boolean and b_boolean and c_boolean) or (d_boolean and e_boolean) or (f_boolean and g_boolean and h_boolean)",
                "(h_boolean and g_boolean and f_boolean) or (b_boolean and a_boolean and c_boolean) or (e_boolean and d_boolean)");

        assertEquivalent(
                "reduce(ARRAY [b_boolean], false, (s, x) -> s AND x, s -> s)",
                "reduce(ARRAY [b_boolean], false, (s, x) -> x AND s, s -> s)");
    }

    private static void assertEquivalent(@Language("SQL") String left, @Language("SQL") String right)
    {
        ParsingOptions parsingOptions = new ParsingOptions(AS_DOUBLE /* anything */);
        Expression leftExpression = rewriteIdentifiersToSymbolReferences(SQL_PARSER.createExpression(left, parsingOptions));
        Expression rightExpression = rewriteIdentifiersToSymbolReferences(SQL_PARSER.createExpression(right, parsingOptions));

        Set<Symbol> symbols = extractUnique(ImmutableList.of(leftExpression, rightExpression));
        TypeProvider types = TypeProvider.viewOf(symbols.stream()
                .collect(toImmutableMap(Symbol::getName, TestExpressionEquivalence::generateType)));

        assertTrue(
                EQUIVALENCE.areExpressionsEquivalent(TEST_SESSION, leftExpression, rightExpression, types),
                String.format("Expected (%s) and (%s) to be equivalent", left, right));
        assertTrue(
                EQUIVALENCE.areExpressionsEquivalent(TEST_SESSION, rightExpression, leftExpression, types),
                String.format("Expected (%s) and (%s) to be equivalent", right, left));
    }

    @Test
    public void testNotEquivalent()
    {
        assertNotEquivalent("CAST(null AS BOOLEAN)", "false");
        assertNotEquivalent("false", "CAST(null AS BOOLEAN)");
        assertNotEquivalent("true", "false");
        assertNotEquivalent("4", "5");
        assertNotEquivalent("4.4", "5.5");
        assertNotEquivalent("'foo'", "'bar'");

        assertNotEquivalent("4 = 5", "5 = 6");
        assertNotEquivalent("4 <> 5", "5 <> 6");
        assertNotEquivalent("4 is distinct from 5", "5 is distinct from 6");
        assertNotEquivalent("4 < 5", "5 > 6");
        assertNotEquivalent("4 <= 5", "5 >= 6");

        assertNotEquivalent("mod(4, 5)", "mod(5, 4)");

        assertNotEquivalent("a_bigint", "b_bigint");
        assertNotEquivalent("a_bigint = b_bigint", "b_bigint = c_bigint");
        assertNotEquivalent("a_bigint < b_bigint", "b_bigint > c_bigint");

        assertNotEquivalent("a_bigint < b_double", "b_double > c_bigint");

        assertNotEquivalent("4 <= 5 and 6 < 7", "7 > 6 and 5 >= 6");
        assertNotEquivalent("4 <= 5 or 6 < 7", "7 > 6 or 5 >= 6");
        assertNotEquivalent("a_bigint <= b_bigint and c_bigint < d_bigint", "d_bigint > c_bigint and b_bigint >= c_bigint");
        assertNotEquivalent("a_bigint <= b_bigint or c_bigint < d_bigint", "d_bigint > c_bigint or b_bigint >= c_bigint");

        assertNotEquivalent(
                "reduce(ARRAY [b_boolean], false, (s, x) -> s AND x, s -> s)",
                "reduce(ARRAY [b_boolean], false, (s, x) -> s OR x, s -> s)");
    }

    private static void assertNotEquivalent(@Language("SQL") String left, @Language("SQL") String right)
    {
        ParsingOptions parsingOptions = new ParsingOptions(AS_DOUBLE /* anything */);
        Expression leftExpression = rewriteIdentifiersToSymbolReferences(SQL_PARSER.createExpression(left, parsingOptions));
        Expression rightExpression = rewriteIdentifiersToSymbolReferences(SQL_PARSER.createExpression(right, parsingOptions));

        Set<Symbol> symbols = extractUnique(ImmutableList.of(leftExpression, rightExpression));
        TypeProvider types = TypeProvider.viewOf(symbols.stream()
                .collect(toImmutableMap(Symbol::getName, TestExpressionEquivalence::generateType)));

        assertFalse(
                EQUIVALENCE.areExpressionsEquivalent(TEST_SESSION, leftExpression, rightExpression, types),
                String.format("Expected (%s) and (%s) to not be equivalent", left, right));
        assertFalse(
                EQUIVALENCE.areExpressionsEquivalent(TEST_SESSION, rightExpression, leftExpression, types),
                String.format("Expected (%s) and (%s) to not be equivalent", right, left));
    }

    private static Set<Symbol> extractUnique(Iterable<? extends Expression> expressions)
    {
        ImmutableSet.Builder<Symbol> unique = ImmutableSet.builder();
        for (Expression expression : expressions) {
            unique.addAll(VariablesExtractor.extractAllSymbols(expression));
        }
        return unique.build();
    }

    private static Type generateType(Symbol symbol)
    {
        String typeName = Splitter.on('_').limit(2).splitToList(symbol.getName()).get(1);
        return METADATA.getType(new TypeSignature(typeName, ImmutableList.of()));
    }
}
