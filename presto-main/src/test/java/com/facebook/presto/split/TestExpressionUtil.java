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
package com.facebook.presto.split;

import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpression.Type;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.InConstantRangePredicate;
import com.facebook.presto.spi.UncertainRangeConstant;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Arrays;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertEquals;

public class TestExpressionUtil
{
    @Test
    public void testLessThanLong()
    {
        // c1 < 1
        Map<ColumnHandle, InConstantRangePredicate> pushdown =
            ExpressionUtil.extractConstantRanges(
                    compareSymbolToLiteral(Type.LESS_THAN, "c1", 1),
                    declareSymbolList("c1"));

        assertEquals(pushdown.size(), 1);
        assertSameInConstantRangePredicate(findPredicateForSymbol(pushdown, "c1"),
                InConstantRangePredicate.Type.LONG,
                new LongUncertainRangeConstant(null, false, 1L, false));
    }

    @Test
    public void testGreaterhanLong()
    {
        // c1 > 1
        Map<ColumnHandle, InConstantRangePredicate> pushdown =
            ExpressionUtil.extractConstantRanges(
                    compareSymbolToLiteral(Type.GREATER_THAN, "c1", 1),
                    declareSymbolList("c1"));

        assertEquals(pushdown.size(), 1);
        assertSameInConstantRangePredicate(findPredicateForSymbol(pushdown, "c1"),
                InConstantRangePredicate.Type.LONG,
                new LongUncertainRangeConstant(1L, false, null, false));
    }

    @Test
    public void testGreaterThanString()
    {
        // c1 > "a"
        Map<ColumnHandle, InConstantRangePredicate> pushdown =
            ExpressionUtil.extractConstantRanges(
                    compareSymbolToLiteral(Type.GREATER_THAN, "c1", "a"),
                    declareSymbolList("c1"));

        assertSameInConstantRangePredicate(findPredicateForSymbol(pushdown, "c1"),
                InConstantRangePredicate.Type.STRING,
                new StringUncertainRangeConstant("a", false, null, false));
    }

    @Test
    public void testLessThanOrEqualToLong()
    {
        // c1 <= 1
        Map<ColumnHandle, InConstantRangePredicate> pushdown =
            ExpressionUtil.extractConstantRanges(
                    compareSymbolToLiteral(Type.LESS_THAN_OR_EQUAL, "c1", 1),
                    declareSymbolList("c1"));

        InConstantRangePredicate pred = findPredicateForSymbol(pushdown, "c1");
        assertSameInConstantRangePredicate(pred, InConstantRangePredicate.Type.LONG,
                new LongUncertainRangeConstant(null, false, 1L, true));
    }

    @Test
    public void testLessThanOrEqualToString()
    {
        // c1 <= "a"
        Map<ColumnHandle, InConstantRangePredicate> pushdown =
            ExpressionUtil.extractConstantRanges(
                    compareSymbolToLiteral(Type.LESS_THAN_OR_EQUAL, "c1", "a"),
                    declareSymbolList("c1"));

        InConstantRangePredicate pred = findPredicateForSymbol(pushdown, "c1");
        assertSameInConstantRangePredicate(pred, InConstantRangePredicate.Type.STRING,
                new StringUncertainRangeConstant(null, false, "a", true));
    }

    @Test
    public void testLiteralToSymbolLessThan()
    {
        // 1 < c1
        Map<ColumnHandle, InConstantRangePredicate> pushdown =
            ExpressionUtil.extractConstantRanges(
                    compareLiteralToSymbol(Type.LESS_THAN, 1, "c1"),
                    declareSymbolList("c1"));

        InConstantRangePredicate pred = findPredicateForSymbol(pushdown, "c1");
        assertSameInConstantRangePredicate(pred, InConstantRangePredicate.Type.LONG,
                new LongUncertainRangeConstant(1L, false, null, false));
    }

    @Test
    public void testLiteralToSymbolLessThanOrEqual()
    {
        // 1 <= c1
        Map<ColumnHandle, InConstantRangePredicate> pushdown =
            ExpressionUtil.extractConstantRanges(
                    compareLiteralToSymbol(Type.LESS_THAN_OR_EQUAL, 1, "c1"),
                    declareSymbolList("c1"));

        InConstantRangePredicate pred = findPredicateForSymbol(pushdown, "c1");
        assertSameInConstantRangePredicate(pred, InConstantRangePredicate.Type.LONG,
                new LongUncertainRangeConstant(1L, true, null, false));
    }

    @Test
    public void testLiteralToSymbolEqualTo()
    {
        // 1 = c1
        Map<ColumnHandle, InConstantRangePredicate> pushdown =
            ExpressionUtil.extractConstantRanges(
                    compareLiteralToSymbol(Type.EQUAL, 1, "c1"),
                    declareSymbolList("c1"));

        InConstantRangePredicate pred = findPredicateForSymbol(pushdown, "c1");
        assertSameInConstantRangePredicate(pred, InConstantRangePredicate.Type.LONG,
                new LongUncertainRangeConstant(1L, true, 1L, true));
    }

    @Test
    public void testLogicalAnd()
    {
        // 1 < c1 AND c1 <= 10
        Map<ColumnHandle, InConstantRangePredicate> pushdown =
            ExpressionUtil.extractConstantRanges(
                    new LogicalBinaryExpression(
                        LogicalBinaryExpression.Type.AND,
                        compareLiteralToSymbol(Type.LESS_THAN, 1, "c1"),
                        compareSymbolToLiteral(Type.LESS_THAN_OR_EQUAL, "c1", 10)),
                    declareSymbolList("c1"));

        InConstantRangePredicate pred = findPredicateForSymbol(pushdown, "c1");
        assertSameInConstantRangePredicate(pred, InConstantRangePredicate.Type.LONG,
                new LongUncertainRangeConstant(1L, false, 10L, true));
    }

    @Test
    public void testLogicalAndOr()
    {
        // c1 < 0 OR (1 <= c1 AND c1 < 10)
        Map<ColumnHandle, InConstantRangePredicate> pushdown =
            ExpressionUtil.extractConstantRanges(
                    new LogicalBinaryExpression(
                        LogicalBinaryExpression.Type.OR,
                        compareSymbolToLiteral(Type.LESS_THAN, "c1", 0),
                        new LogicalBinaryExpression(
                            LogicalBinaryExpression.Type.AND,
                            compareLiteralToSymbol(Type.LESS_THAN_OR_EQUAL, 1, "c1"),
                            compareSymbolToLiteral(Type.LESS_THAN, "c1", 10))),
                    declareSymbolList("c1"));

        InConstantRangePredicate pred = findPredicateForSymbol(pushdown, "c1");
        assertSameInConstantRangePredicate(pred, InConstantRangePredicate.Type.LONG,
                new LongUncertainRangeConstant(null, false, 0L, false),
                new LongUncertainRangeConstant(1L, true, 10L, false));
    }

    @Test
    public void testMixedType()
    {
        // c1 < 0 OR c1 < "a"
        Map<ColumnHandle, InConstantRangePredicate> pushdown =
            ExpressionUtil.extractConstantRanges(
                    new LogicalBinaryExpression(
                        LogicalBinaryExpression.Type.OR,
                        compareSymbolToLiteral(Type.LESS_THAN, "c1", 0),
                        compareSymbolToLiteral(Type.LESS_THAN, "c1", "a")),
                    declareSymbolList("c1"));

        InConstantRangePredicate pred = findPredicateForSymbol(pushdown, "c1");
        assertNull(pred);
    }

    @Test
    public void testNestedMixedType()
    {
        // c1 < 0 OR (c1 > 0 OR c1 > "a")
        Map<ColumnHandle, InConstantRangePredicate> pushdown =
            ExpressionUtil.extractConstantRanges(
                    new LogicalBinaryExpression(
                        LogicalBinaryExpression.Type.OR,
                        compareSymbolToLiteral(Type.LESS_THAN, "c1", 0),
                        new LogicalBinaryExpression(
                            LogicalBinaryExpression.Type.OR,
                            compareSymbolToLiteral(Type.GREATER_THAN, "c1", 0),
                            compareSymbolToLiteral(Type.GREATER_THAN, "c1", "a"))),
                    declareSymbolList("c1"));

        InConstantRangePredicate pred = findPredicateForSymbol(pushdown, "c1");
        assertNull(pred);
    }

    private static InConstantRangePredicate findPredicateForSymbol(Map<ColumnHandle, InConstantRangePredicate> pushdown, String name)
    {
        for (Map.Entry<ColumnHandle, InConstantRangePredicate> entry : pushdown.entrySet()) {
            if (entry.getKey() instanceof DummyColumnHandler &&
                    ((DummyColumnHandler) entry.getKey()).getName().equals(name)) {
                return entry.getValue();
            }
        }
        return null;
    }

    public static void assertSameInConstantRangePredicate(InConstantRangePredicate predicate,
            InConstantRangePredicate.Type type, UncertainRangeConstant... possibleRanges)
    {
        assertNotNull(predicate);
        assertEquals(predicate.getType(), type);
        assertEquals(predicate.getPossibleRanges(), Arrays.asList(possibleRanges));
    }

    private static class DummyColumnHandler implements ColumnHandle
    {
        private final String name;

        public DummyColumnHandler(String name) {
            this.name = name;
        }

        public String getName()
        {
            return name;
        }
    }

    private Map<Symbol, ColumnHandle> declareSymbolList(String... names)
    {
        ImmutableMap.Builder<Symbol, ColumnHandle> builder = new ImmutableMap.Builder<>();
        for(String name : names) {
            builder.put(new Symbol(name), new DummyColumnHandler(name));
        }
        return builder.build();
    }

    private static ComparisonExpression compareSymbolToLiteral(Type type, String leftSymbol, String rightValue) {
        return new ComparisonExpression(type,
                new QualifiedNameReference(new QualifiedName(leftSymbol)),
                new StringLiteral(rightValue));
    }

    private static ComparisonExpression compareSymbolToLiteral(Type type, String leftSymbol, long rightValue) {
        return new ComparisonExpression(type,
                new QualifiedNameReference(new QualifiedName(leftSymbol)),
                new LongLiteral(Long.toString(rightValue)));
    }

    private static ComparisonExpression compareLiteralToSymbol(Type type, String leftValue, String rightSymbol) {
        return new ComparisonExpression(type,
                new StringLiteral(leftValue),
                new QualifiedNameReference(new QualifiedName(rightSymbol)));
    }

    private static ComparisonExpression compareLiteralToSymbol(Type type, long leftValue, String rightSymbol) {
        return new ComparisonExpression(type,
                new LongLiteral(Long.toString(leftValue)),
                new QualifiedNameReference(new QualifiedName(rightSymbol)));
    }
}

