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
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static com.facebook.presto.sql.planner.VariablesExtractor.extractAll;
import static com.facebook.presto.sql.planner.VariablesExtractor.extractUnique;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;

public class TestVariableExtractor
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();
    private static final TestingRowExpressionTranslator TRANSLATOR = new TestingRowExpressionTranslator(METADATA);
    private static final TypeProvider SYMBOL_TYPES = TypeProvider.fromVariables(ImmutableList.of(
            new VariableReferenceExpression("a", BIGINT),
            new VariableReferenceExpression("b", BIGINT),
            new VariableReferenceExpression("c", BIGINT)));

    @Test
    public void testSimple()
    {
        assertVariables("a > b");
        assertVariables("a + b > c");
        assertVariables("sin(a) - b");
        assertVariables("sin(a) + cos(a) - b");
        assertVariables("sin(a) + cos(a) + a - b");
        assertVariables("COALESCE(a, b, 1)");
        assertVariables("a IN (a, b, c)");
        assertVariables("transform(sequence(1, 5), a -> a + b)");
        assertVariables("bigint '1'");
    }

    private static void assertVariables(String expression)
    {
        Expression expected = rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(expression, new ParsingOptions()));
        RowExpression actual = TRANSLATOR.translate(expected, SYMBOL_TYPES);
        assertEquals(VariablesExtractor.extractUnique(expected, SYMBOL_TYPES), extractUnique(actual));
        assertEquals(
                VariablesExtractor.extractAll(expected, SYMBOL_TYPES).stream().sorted().collect(toImmutableList()),
                extractAll(actual).stream().sorted().collect(toImmutableList()));
    }
}
