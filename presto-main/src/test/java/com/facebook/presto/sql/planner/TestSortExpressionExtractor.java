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

import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;

import static org.testng.AssertJUnit.assertEquals;

public class TestSortExpressionExtractor
{
    private static final Set<Symbol> BUILD_SYMBOLS = ImmutableSet.of(new Symbol("b1"), new Symbol("b2"));

    @Test
    public void testGetSortExpression()
    {
        assertGetSortExpression(
                new ComparisonExpression(
                        ComparisonExpressionType.GREATER_THAN,
                        new SymbolReference("p1"),
                        new SymbolReference("b1")),
                "b1");

        assertGetSortExpression(
                new ComparisonExpression(
                        ComparisonExpressionType.LESS_THAN_OR_EQUAL,
                        new SymbolReference("b2"),
                        new SymbolReference("p1")),
                "b2");

        assertGetSortExpression(
                new ComparisonExpression(
                        ComparisonExpressionType.GREATER_THAN,
                        new SymbolReference("b2"),
                        new SymbolReference("p1")),
                "b2");

        assertGetSortExpression(
                new ComparisonExpression(
                        ComparisonExpressionType.GREATER_THAN,
                        new SymbolReference("b2"),
                        new FunctionCall(QualifiedName.of("sin"), ImmutableList.of(new SymbolReference("p1")))),
                "b2");

        assertGetSortExpression(
                new ComparisonExpression(
                        ComparisonExpressionType.GREATER_THAN,
                        new SymbolReference("b2"),
                        new FunctionCall(QualifiedName.of("random"), ImmutableList.of(new SymbolReference("p1")))));

        assertGetSortExpression(
                new ComparisonExpression(
                        ComparisonExpressionType.GREATER_THAN,
                        new SymbolReference("b1"),
                        new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.ADD, new SymbolReference("b2"), new SymbolReference("p1"))));

        assertGetSortExpression(
                new ComparisonExpression(
                        ComparisonExpressionType.GREATER_THAN,
                        new FunctionCall(QualifiedName.of("sin"), ImmutableList.of(new SymbolReference("b1"))),
                        new SymbolReference("p1")));
    }

    private static void assertGetSortExpression(Expression expression)
    {
        Optional<Expression> actual = SortExpressionExtractor.extractSortExpression(BUILD_SYMBOLS, expression);
        assertEquals(Optional.empty(), actual);
    }

    private static void assertGetSortExpression(Expression expression, String expectedSymbol)
    {
        Optional<Expression> expected = Optional.of(new SymbolReference(expectedSymbol));
        Optional<Expression> actual = SortExpressionExtractor.extractSortExpression(BUILD_SYMBOLS, expression);
        assertEquals(expected, actual);
    }
}
