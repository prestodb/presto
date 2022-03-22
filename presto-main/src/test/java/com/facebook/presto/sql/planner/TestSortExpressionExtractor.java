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
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;

public class TestSortExpressionExtractor
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();
    private static final TestingRowExpressionTranslator TRANSLATOR = new TestingRowExpressionTranslator(METADATA);
    private static final Set<VariableReferenceExpression> BUILD_VARIABLES = ImmutableSet.of(
            new VariableReferenceExpression("b1", BIGINT),
            new VariableReferenceExpression("b2", BIGINT));
    private static final TypeProvider TYPES = TypeProvider.fromVariables(ImmutableList.of(
            new VariableReferenceExpression("b1", BIGINT),
            new VariableReferenceExpression("b2", BIGINT),
            new VariableReferenceExpression("p1", BIGINT),
            new VariableReferenceExpression("p2", BIGINT)));

    @Test
    public void testGetSortExpression()
    {
        assertGetSortExpression("p1 > b1", "b1");

        assertGetSortExpression("b2 <= p1", "b2");

        assertGetSortExpression("b2 > p1", "b2");

        assertGetSortExpression("b2 > sin(p1)", "b2");

        assertNoSortExpression("b2 > random(p1)");

        assertGetSortExpression("b2 > random(p1) AND b2 > p1", "b2", "b2 > p1");

        assertGetSortExpression("b2 > random(p1) AND b1 > p1", "b1", "b1 > p1");

        assertNoSortExpression("b1 > p1 + b2");

        assertNoSortExpression("sin(b1) > p1");

        assertNoSortExpression("b1 <= p1 OR b2 <= p1");

        assertNoSortExpression("sin(b2) > p1 AND (b2 <= p1 OR b2 <= p1 + 10)");

        assertGetSortExpression("sin(b2) > p1 AND (b2 <= p1 AND b2 <= p1 + 10)", "b2", "b2 <= p1", "b2 <= p1 + 10");

        assertGetSortExpression("b1 > p1 AND b1 <= p1", "b1");

        assertGetSortExpression("b1 > p1 AND b1 <= p1 AND b2 > p1", "b1", "b1 > p1", "b1 <= p1");

        assertGetSortExpression("b1 > p1 AND b1 <= p1 AND b2 > p1 AND b2 < p1 + 10 AND b2 > p2", "b2", "b2 > p1", "b2 < p1 + 10", "b2 > p2");
    }

    private Expression expression(String sql)
    {
        return rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(sql));
    }

    private void assertNoSortExpression(String expression)
    {
        assertNoSortExpression(expression(expression));
    }

    private void assertNoSortExpression(Expression expression)
    {
        Optional<SortExpressionContext> actual = SortExpressionExtractor.extractSortExpression(
                BUILD_VARIABLES,
                TRANSLATOR.translate(expression, TYPES),
                METADATA.getFunctionAndTypeManager());
        assertEquals(actual, Optional.empty());
    }

    private void assertGetSortExpression(String expression, String expectedSymbol)
    {
        assertGetSortExpression(expression(expression), expectedSymbol);
    }

    private void assertGetSortExpression(Expression expression, String expectedSymbol)
    {
        // for now we expect that search expressions contain all the conjuncts from filterExpression as more complex cases are not supported yet.
        assertGetSortExpression(expression, expectedSymbol, extractConjuncts(expression));
    }

    private void assertGetSortExpression(String expression, String expectedSymbol, String... searchExpressions)
    {
        assertGetSortExpression(expression(expression), expectedSymbol, searchExpressions);
    }

    private void assertGetSortExpression(Expression expression, String expectedSymbol, String... searchExpressions)
    {
        List<Expression> searchExpressionList = Arrays.stream(searchExpressions)
                .map(this::expression)
                .collect(toImmutableList());
        assertGetSortExpression(expression, expectedSymbol, searchExpressionList);
    }

    private static void assertGetSortExpression(Expression expression, String expectedSymbol, List<Expression> searchExpressions)
    {
        Optional<SortExpressionContext> expected = Optional.of(new SortExpressionContext(
                new VariableReferenceExpression(expectedSymbol, BIGINT),
                searchExpressions.stream().map(e -> TRANSLATOR.translate(e, TYPES)).collect(toImmutableList())));
        Optional<SortExpressionContext> actual = SortExpressionExtractor.extractSortExpression(
                BUILD_VARIABLES,
                TRANSLATOR.translate(expression, TYPES),
                METADATA.getFunctionAndTypeManager());
        assertEquals(actual, expected);
    }
}
