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

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static com.facebook.presto.sql.planner.SymbolsExtractor.extractAll;
import static com.facebook.presto.sql.planner.SymbolsExtractor.extractUnique;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;

public class TestSymbolExtractor
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();
    private static final TestingRowExpressionTranslator TRANSLATOR = new TestingRowExpressionTranslator(METADATA);
    private static final Map<Symbol, Type> SYMBOL_TYPES = ImmutableMap.of(new Symbol("a"), BIGINT, new Symbol("b"), BIGINT, new Symbol("c"), BIGINT);

    @Test
    public void testSimple()
    {
        assertSymbols("a > b");
        assertSymbols("a + b > c");
        assertSymbols("sin(a) - b");
        assertSymbols("sin(a) + cos(a) - b");
        assertSymbols("sin(a) + cos(a) + a - b");
        assertSymbols("COALESCE(a, b, 1)");
        assertSymbols("a IN (a, b, c)");
        assertSymbols("transform(sequence(1, 5), a -> a + b)");
        assertSymbols("bigint '1'");
    }

    private static void assertSymbols(String expression)
    {
        Expression expected = rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(expression, new ParsingOptions()));
        RowExpression actual = TRANSLATOR.translate(expected, TypeProvider.copyOf(SYMBOL_TYPES));
        assertEquals(extractUnique(expected), extractUnique(actual));
        assertEquals(
                extractAll(expected).stream().sorted().collect(toImmutableList()),
                extractAll(actual).stream().map(variable -> new Symbol(variable.getName())).sorted().collect(toImmutableList()));
    }
}
