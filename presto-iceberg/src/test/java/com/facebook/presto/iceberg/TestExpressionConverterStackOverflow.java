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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.DateType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.ExpressionVisitors.ExpressionVisitor;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.LongStream;

import static com.facebook.presto.iceberg.IcebergColumnHandle.primitiveIcebergColumnHandle;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestExpressionConverterStackOverflow
{
    // 5000 is above the ~3000-partition threshold where the stack overflow was observed in production.
    private static final int LARGE_PARTITION_COUNT = 5_000;

    private static final IcebergColumnHandle CREATED_DATE_HANDLE = primitiveIcebergColumnHandle(
            1, "created_date", DateType.DATE, Optional.empty());

    private static final Types.StructType DATE_SCHEMA = Types.StructType.of(
            Types.NestedField.optional(1, "created_date", Types.DateType.get()));

    // Regression test for the primary bug: a 5000-point domain must not overflow ExpressionVisitors.
    @Test
    public void testLargeMultiValueDomainProducesInExpression()
    {
        Domain domain = Domain.multipleValues(DateType.DATE, buildDateValues(LARGE_PARTITION_COUNT));
        TupleDomain<IcebergColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(CREATED_DATE_HANDLE, domain));

        Expression expression = ExpressionConverter.toIcebergExpression(tupleDomain);

        // Should not overflow — in() is depth 1 regardless of value count.
        String result = visitExpression(expression);
        assertNotNull(result);
        assertTrue(result.toLowerCase(java.util.Locale.ROOT).contains("in"),
                "Expected in() predicate but got: " + result);
    }

    // Single equality value takes the equal() branch, not in(); verify the short-circuit.
    @Test
    public void testSingleEqualityValueUsesEqualPredicate()
    {
        Domain domain = Domain.singleValue(DateType.DATE, 19000L);
        TupleDomain<IcebergColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(CREATED_DATE_HANDLE, domain));

        Expression expression = ExpressionConverter.toIcebergExpression(tupleDomain);
        String result = visitExpression(expression);
        assertNotNull(result);
        assertFalse(result.toLowerCase(java.util.Locale.ROOT).contains("in"),
                "Expected equal() predicate for single value but got: " + result);
        assertTrue(evalOnDate(expression, 19000L), "19000 should match equal(19000)");
        assertFalse(evalOnDate(expression, 19001L), "19001 should not match equal(19000)");
    }

    // Small domains (100 values) still convert to in() after the fix.
    @Test
    public void testSmallDomainConvertsCorrectly()
    {
        Domain domain = Domain.multipleValues(DateType.DATE, buildDateValues(100));
        TupleDomain<IcebergColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(CREATED_DATE_HANDLE, domain));

        Expression expression = ExpressionConverter.toIcebergExpression(tupleDomain);
        String result = visitExpression(expression);
        assertNotNull(result);
        assertTrue(result.toLowerCase(java.util.Locale.ROOT).contains("in"),
                "Expected in() predicate but got: " + result);
    }

    // Null-allowed domain must include an isNull() predicate OR'd with the value predicate.
    @Test
    public void testNullAllowedDomainIncludesIsNull()
    {
        Domain domain = Domain.create(
                SortedRangeSet.copyOf(DateType.DATE, ImmutableList.of(
                        Range.equal(DateType.DATE, 19000L))),
                true);
        TupleDomain<IcebergColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(CREATED_DATE_HANDLE, domain));

        Expression expression = ExpressionConverter.toIcebergExpression(tupleDomain);
        assertTrue(evalOnDate(expression, 19000L), "19000 should match");
        assertFalse(evalOnDate(expression, 19001L), "19001 should not match");
        assertTrue(evalOnNull(expression), "null should match null-allowed domain");
    }

    // Upper-bounded-only range (date < 19050) must match values below the bound and exclude the bound itself.
    @Test
    public void testUpperBoundedOnlyRangeConvertsCorrectly()
    {
        Domain domain = Domain.create(
                SortedRangeSet.copyOf(DateType.DATE, ImmutableList.of(
                        Range.lessThan(DateType.DATE, 19050L))),
                false);
        TupleDomain<IcebergColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(CREATED_DATE_HANDLE, domain));

        Expression expression = ExpressionConverter.toIcebergExpression(tupleDomain);
        assertTrue(evalOnDate(expression, 19000L), "19000 is below 19050, should match");
        assertTrue(evalOnDate(expression, 19049L), "19049 is below 19050, should match");
        assertFalse(evalOnDate(expression, 19050L), "19050 is the exclusive upper bound, should not match");
        assertFalse(evalOnDate(expression, 19100L), "19100 is above 19050, should not match");
    }

    // BETWEEN range (lo <= date <= hi, lo != hi) exercises the cachedLow/cachedHigh paths in pass 2.
    @Test
    public void testBetweenRangeConvertsCorrectly()
    {
        Domain domain = Domain.create(
                SortedRangeSet.copyOf(DateType.DATE, ImmutableList.of(
                        Range.range(DateType.DATE, 19000L, true, 19100L, true))),
                false);
        TupleDomain<IcebergColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(CREATED_DATE_HANDLE, domain));

        Expression expression = ExpressionConverter.toIcebergExpression(tupleDomain);
        assertTrue(evalOnDate(expression, 19000L), "19000 is the inclusive lower bound, should match");
        assertTrue(evalOnDate(expression, 19050L), "19050 is inside the range, should match");
        assertTrue(evalOnDate(expression, 19100L), "19100 is the inclusive upper bound, should match");
        assertFalse(evalOnDate(expression, 18999L), "18999 is below the range, should not match");
        assertFalse(evalOnDate(expression, 19101L), "19101 is above the range, should not match");
    }

    // Domain mixing point equalities with an open range: each branch is evaluated independently.
    @Test
    public void testMixedEqualityAndRangeDomainConvertsCorrectly()
    {
        Domain domain = Domain.create(
                SortedRangeSet.copyOf(DateType.DATE, ImmutableList.of(
                        Range.equal(DateType.DATE, 19000L),
                        Range.equal(DateType.DATE, 19001L),
                        Range.greaterThan(DateType.DATE, 19100L))),
                false);
        TupleDomain<IcebergColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(CREATED_DATE_HANDLE, domain));

        Expression expression = ExpressionConverter.toIcebergExpression(tupleDomain);
        assertTrue(evalOnDate(expression, 19000L), "19000 is an equality value, should match");
        assertTrue(evalOnDate(expression, 19001L), "19001 is an equality value, should match");
        assertTrue(evalOnDate(expression, 19101L), "19101 is above 19100, should match");
        assertFalse(evalOnDate(expression, 19002L), "19002 is not in any range, should not match");
        assertFalse(evalOnDate(expression, 19099L), "19099 is below the open range, should not match");
        assertFalse(evalOnDate(expression, 19100L), "19100 is the exclusive lower bound, should not match");
    }

    // Equality value above a bounded open range must remain an independent disjunct.
    // This verifies that the two-pass rewrite preserves mixed-range semantics.
    @Test
    public void testEqualityValueAboveBoundedRangeConvertsCorrectly()
    {
        Domain domain = Domain.create(
                SortedRangeSet.copyOf(DateType.DATE, ImmutableList.of(
                        Range.range(DateType.DATE, 19010L, false, 19020L, false),
                        Range.equal(DateType.DATE, 19050L))),
                false);
        TupleDomain<IcebergColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(CREATED_DATE_HANDLE, domain));

        Expression expression = ExpressionConverter.toIcebergExpression(tupleDomain);
        assertTrue(evalOnDate(expression, 19011L), "19011 is inside (19010, 19020), should match");
        assertTrue(evalOnDate(expression, 19019L), "19019 is inside (19010, 19020), should match");
        assertTrue(evalOnDate(expression, 19050L), "19050 is the equality value, should match");
        assertFalse(evalOnDate(expression, 19010L), "19010 is the exclusive lower bound, should not match");
        assertFalse(evalOnDate(expression, 19020L), "19020 is the exclusive upper bound, should not match");
        assertFalse(evalOnDate(expression, 19021L), "19021 is between the range and equality, should not match");
        assertFalse(evalOnDate(expression, 19049L), "19049 is between the range and equality, should not match");
    }

    // --- helpers ---

    private static List<Long> buildDateValues(int count)
    {
        return LongStream.range(19000, 19000 + count)
                .boxed()
                .collect(ImmutableList.toImmutableList());
    }

    private static boolean evalOnDate(Expression expression, long prestoDateValue)
    {
        int icebergDateValue = toIntExact(prestoDateValue);
        Evaluator evaluator = new Evaluator(DATE_SCHEMA, expression);
        return evaluator.eval(new StructLike()
        {
            @Override
            public int size()
            {
                return 1;
            }

            @SuppressWarnings("unchecked")
            @Override
            public <T> T get(int pos, Class<T> javaClass)
            {
                return (T) (Integer) icebergDateValue;
            }

            @Override
            public <T> void set(int pos, T value) {}
        });
    }

    private static boolean evalOnNull(Expression expression)
    {
        Evaluator evaluator = new Evaluator(DATE_SCHEMA, expression);
        return evaluator.eval(new StructLike()
        {
            @Override
            public int size()
            {
                return 1;
            }

            @Override
            public <T> T get(int pos, Class<T> javaClass)
            {
                return null;
            }

            @Override
            public <T> void set(int pos, T value) {}
        });
    }

    private static String visitExpression(Expression expression)
    {
        return ExpressionVisitors.visit(expression, new ExpressionVisitor<String>()
        {
            @Override
            public String alwaysTrue()
            {
                return "TRUE";
            }

            @Override
            public String alwaysFalse()
            {
                return "FALSE";
            }

            @Override
            public String not(String result)
            {
                return "NOT(" + result + ")";
            }

            @Override
            public String and(String left, String right)
            {
                return "AND(" + left + "," + right + ")";
            }

            @Override
            public String or(String left, String right)
            {
                return "OR(" + left + "," + right + ")";
            }

            @Override
            public <T> String predicate(org.apache.iceberg.expressions.BoundPredicate<T> pred)
            {
                return pred.toString();
            }

            @Override
            public <T> String predicate(org.apache.iceberg.expressions.UnboundPredicate<T> pred)
            {
                return pred.toString();
            }
        });
    }
}
