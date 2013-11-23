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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.SortedRangeSet;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.TupleDomain.withColumnDomains;
import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.ExpressionUtils.or;
import static com.facebook.presto.sql.planner.DomainTranslator.ExtractionResult;
import static com.facebook.presto.sql.planner.DomainTranslator.fromPredicate;
import static com.facebook.presto.sql.planner.DomainTranslator.toPredicate;
import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.GREATER_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.IS_DISTINCT_FROM;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.LESS_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.NOT_EQUAL;

public class TestDomainTranslator
{
    private static final Symbol A = new Symbol("a");
    private static final ColumnHandle ACH = new TestingColumnHandle(A);
    private static final Symbol B = new Symbol("b");
    private static final ColumnHandle BCH = new TestingColumnHandle(B);
    private static final Symbol C = new Symbol("c");
    private static final ColumnHandle CCH = new TestingColumnHandle(C);
    private static final Symbol D = new Symbol("d");
    private static final ColumnHandle DCH = new TestingColumnHandle(D);
    private static final Symbol E = new Symbol("e");
    private static final ColumnHandle ECH = new TestingColumnHandle(E);
    private static final Symbol F = new Symbol("f");
    private static final ColumnHandle FCH = new TestingColumnHandle(F);
    private static final Symbol G = new Symbol("g");
    private static final ColumnHandle GCH = new TestingColumnHandle(G);

    private static final Map<Symbol, Type> TYPES = ImmutableMap.<Symbol, Type>builder()
            .put(A, Type.BIGINT)
            .put(B, Type.DOUBLE)
            .put(C, Type.VARCHAR)
            .put(D, Type.BOOLEAN)
            .put(E, Type.BIGINT)
            .put(F, Type.DOUBLE)
            .put(G, Type.VARCHAR)
            .build();

    private static final BiMap<Symbol, ColumnHandle> COLUMN_HANDLES = ImmutableBiMap.<Symbol, ColumnHandle>builder()
            .put(A, ACH)
            .put(B, BCH)
            .put(C, CCH)
            .put(D, DCH)
            .put(E, ECH)
            .put(F, FCH)
            .put(G, GCH)
            .build();

    @Test
    public void testNoneRoundTrip()
            throws Exception
    {
        TupleDomain tupleDomain = TupleDomain.none();
        ExtractionResult result = fromPredicate(toPredicate(tupleDomain, COLUMN_HANDLES.inverse()), TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), tupleDomain);
    }

    @Test
    public void testAllRoundTrip()
            throws Exception
    {
        TupleDomain tupleDomain = TupleDomain.all();
        ExtractionResult result = fromPredicate(toPredicate(tupleDomain, COLUMN_HANDLES.inverse()), TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), tupleDomain);
    }

    @Test
    public void testRoundTrip()
            throws Exception
    {
        TupleDomain tupleDomain = withColumnDomains(ImmutableMap.<ColumnHandle, Domain>builder()
                .put(ACH, Domain.singleValue(1L))
                .put(BCH, Domain.onlyNull(Double.class))
                .put(CCH, Domain.notNull(String.class))
                .put(DCH, Domain.singleValue(true))
                .put(ECH, Domain.singleValue(2L))
                .put(FCH, Domain.create(SortedRangeSet.of(Range.lessThanOrEqual(1.1), Range.equal(2.0), Range.range(3.0, false, 3.5, true)), true))
                .put(GCH, Domain.create(SortedRangeSet.of(Range.lessThanOrEqual("2013-01-01"), Range.greaterThan("2013-10-01")), false))
                .build());

        ExtractionResult result = fromPredicate(toPredicate(tupleDomain, COLUMN_HANDLES.inverse()), TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), tupleDomain);
    }

    @Test
    public void testToPredicateNone()
            throws Exception
    {
        TupleDomain tupleDomain = withColumnDomains(ImmutableMap.<ColumnHandle, Domain>builder()
                .put(ACH, Domain.singleValue(1L))
                .put(BCH, Domain.onlyNull(Double.class))
                .put(CCH, Domain.notNull(String.class))
                .put(DCH, Domain.none(Boolean.class))
                .build());

        Assert.assertEquals(toPredicate(tupleDomain, COLUMN_HANDLES.inverse()), FALSE_LITERAL);
    }

    @Test
    public void testToPredicateAllIgnored()
            throws Exception
    {
        TupleDomain tupleDomain = withColumnDomains(ImmutableMap.<ColumnHandle, Domain>builder()
                .put(ACH, Domain.singleValue(1L))
                .put(BCH, Domain.onlyNull(Double.class))
                .put(CCH, Domain.notNull(String.class))
                .put(DCH, Domain.all(Boolean.class))
                .build());

        ExtractionResult result = fromPredicate(toPredicate(tupleDomain, COLUMN_HANDLES.inverse()), TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>builder()
                .put(ACH, Domain.singleValue(1L))
                .put(BCH, Domain.onlyNull(Double.class))
                .put(CCH, Domain.notNull(String.class))
                .build()));
    }

    @Test
    public void testToPredicate()
            throws Exception
    {
        TupleDomain tupleDomain;

        tupleDomain = withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.notNull(Long.class)));
        Assert.assertEquals(toPredicate(tupleDomain, COLUMN_HANDLES.inverse()), isNotNull(A));

        tupleDomain = withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.onlyNull(Long.class)));
        Assert.assertEquals(toPredicate(tupleDomain, COLUMN_HANDLES.inverse()), isNull(A));

        tupleDomain = withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.none(Long.class)));
        Assert.assertEquals(toPredicate(tupleDomain, COLUMN_HANDLES.inverse()), FALSE_LITERAL);

        tupleDomain = withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.all(Long.class)));
        Assert.assertEquals(toPredicate(tupleDomain, COLUMN_HANDLES.inverse()), TRUE_LITERAL);

        tupleDomain = withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.greaterThan(1L)), false)));
        Assert.assertEquals(toPredicate(tupleDomain, COLUMN_HANDLES.inverse()), greaterThan(A, longLiteral(1L)));

        tupleDomain = withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.greaterThanOrEqual(1L)), false)));
        Assert.assertEquals(toPredicate(tupleDomain, COLUMN_HANDLES.inverse()), greaterThanOrEqual(A, longLiteral(1L)));

        tupleDomain = withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThan(1L)), false)));
        Assert.assertEquals(toPredicate(tupleDomain, COLUMN_HANDLES.inverse()), lessThan(A, longLiteral(1L)));

        tupleDomain = withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.range(0L, false, 1L, true)), false)));
        Assert.assertEquals(toPredicate(tupleDomain, COLUMN_HANDLES.inverse()), and(greaterThan(A, longLiteral(0L)), lessThanOrEqual(A, longLiteral(1L))));

        tupleDomain = withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThanOrEqual(1L)), false)));
        Assert.assertEquals(toPredicate(tupleDomain, COLUMN_HANDLES.inverse()), lessThanOrEqual(A, longLiteral(1L)));

        tupleDomain = withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.singleValue(1L)));
        Assert.assertEquals(toPredicate(tupleDomain, COLUMN_HANDLES.inverse()), equal(A, longLiteral(1L)));

        tupleDomain = withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.equal(1L), Range.equal(2L)), false)));
        Assert.assertEquals(toPredicate(tupleDomain, COLUMN_HANDLES.inverse()), in(A, ImmutableList.of(1L, 2L)));

        tupleDomain = withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThan(1L)), true)));
        Assert.assertEquals(toPredicate(tupleDomain, COLUMN_HANDLES.inverse()), or(lessThan(A, longLiteral(1L)), isNull(A)));
    }

    @Test
    public void testFromUnknownPredicate()
            throws Exception
    {
        ExtractionResult result = fromPredicate(unprocessableExpression1(A), TYPES, COLUMN_HANDLES);
        Assert.assertTrue(result.getTupleDomain().isAll());
        Assert.assertEquals(result.getRemainingExpression(), unprocessableExpression1(A));

        // Test the complement
        result = fromPredicate(not(unprocessableExpression1(A)), TYPES, COLUMN_HANDLES);
        Assert.assertTrue(result.getTupleDomain().isAll());
        Assert.assertEquals(result.getRemainingExpression(), not(unprocessableExpression1(A)));
    }

    @Test
    public void testFromAndPredicate()
            throws Exception
    {
        Expression originalPredicate = and(
                and(greaterThan(A, longLiteral(1L)), unprocessableExpression1(A)),
                and(lessThan(A, longLiteral(5L)), unprocessableExpression2(A)));
        ExtractionResult result = fromPredicate(originalPredicate, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), and(unprocessableExpression1(A), unprocessableExpression2(A)));
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.range(1L, false, 5L, false)), false))));

        // Test complements
        originalPredicate = not(and(
                and(greaterThan(A, longLiteral(1L)), unprocessableExpression1(A)),
                and(lessThan(A, longLiteral(5L)), unprocessableExpression2(A))));
        result = fromPredicate(originalPredicate, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), originalPredicate);
        Assert.assertTrue(result.getTupleDomain().isAll());

        originalPredicate = not(and(
                not(and(greaterThan(A, longLiteral(1L)), unprocessableExpression1(A))),
                not(and(lessThan(A, longLiteral(5L)), unprocessableExpression2(A)))));
        result = fromPredicate(originalPredicate, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), originalPredicate);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.notNull(Long.class))));
    }

    @Test
    public void testFromOrPredicate()
            throws Exception
    {
        Expression originalPredicate = or(
                and(greaterThan(A, longLiteral(1L)), unprocessableExpression1(A)),
                and(lessThan(A, longLiteral(5L)), unprocessableExpression2(A)));
        ExtractionResult result = fromPredicate(originalPredicate, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), originalPredicate);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.notNull(Long.class))));

        originalPredicate = or(
                and(equal(A, longLiteral(1L)), unprocessableExpression1(A)),
                and(equal(A, longLiteral(2L)), unprocessableExpression2(A)));
        result = fromPredicate(originalPredicate, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), originalPredicate);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.equal(1L), Range.equal(2L)), false))));

        // Same unprocessableExpression means that we can do more extraction
        // If both sides are operating on the same single symbol
        originalPredicate = or(
                and(equal(A, longLiteral(1L)), unprocessableExpression1(A)),
                and(equal(A, longLiteral(2L)), unprocessableExpression1(A)));
        result = fromPredicate(originalPredicate, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), unprocessableExpression1(A));
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.equal(1L), Range.equal(2L)), false))));

        // And not if they have different symbols
        originalPredicate = or(
                and(equal(A, longLiteral(1L)), unprocessableExpression1(A)),
                and(equal(B, doubleLiteral(2.0)), unprocessableExpression1(A)));
        result = fromPredicate(originalPredicate, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), originalPredicate);
        Assert.assertTrue(result.getTupleDomain().isAll());

        // We can make another optimization if one side is the super set of the other side
        originalPredicate = or(
                and(greaterThan(A, longLiteral(1L)), greaterThan(B, doubleLiteral(1.0)), unprocessableExpression1(A)),
                and(greaterThan(A, longLiteral(2L)), greaterThan(B, doubleLiteral(2.0)), unprocessableExpression1(A)));
        result = fromPredicate(originalPredicate, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), unprocessableExpression1(A));
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(
                ACH, Domain.create(SortedRangeSet.of(Range.greaterThan(1L)), false),
                BCH, Domain.create(SortedRangeSet.of(Range.greaterThan(1.0)), false))));

        // We can't make those inferences if the unprocessableExpressions are non-deterministic
        originalPredicate = or(
                and(equal(A, longLiteral(1L)), randPredicate(A)),
                and(equal(A, longLiteral(2L)), randPredicate(A)));
        result = fromPredicate(originalPredicate, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), originalPredicate);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.equal(1L), Range.equal(2L)), false))));

        // Test complements
        originalPredicate = not(or(
                and(greaterThan(A, longLiteral(1L)), unprocessableExpression1(A)),
                and(lessThan(A, longLiteral(5L)), unprocessableExpression2(A))));
        result = fromPredicate(originalPredicate, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), and(
                not(and(greaterThan(A, longLiteral(1L)), unprocessableExpression1(A))),
                not(and(lessThan(A, longLiteral(5L)), unprocessableExpression2(A)))));
        Assert.assertTrue(result.getTupleDomain().isAll());

        originalPredicate = not(or(
                not(and(greaterThan(A, longLiteral(1L)), unprocessableExpression1(A))),
                not(and(lessThan(A, longLiteral(5L)), unprocessableExpression2(A)))));
        result = fromPredicate(originalPredicate, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), and(unprocessableExpression1(A), unprocessableExpression2(A)));
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.range(1L, false, 5L, false)), false))));
    }

    @Test
    public void testFromNotPredicate()
            throws Exception
    {
        Expression originalPredicate = not(and(equal(A, longLiteral(1L)), unprocessableExpression1(A)));
        ExtractionResult result = fromPredicate(originalPredicate, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), originalPredicate);
        Assert.assertTrue(result.getTupleDomain().isAll());

        originalPredicate = not(unprocessableExpression1(A));
        result = fromPredicate(originalPredicate, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), originalPredicate);
        Assert.assertTrue(result.getTupleDomain().isAll());

        originalPredicate = not(TRUE_LITERAL);
        result = fromPredicate(originalPredicate, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertTrue(result.getTupleDomain().isNone());

        originalPredicate = not(equal(A, longLiteral(1L)));
        result = fromPredicate(originalPredicate, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThan(1L), Range.greaterThan(1L)), false))));
    }

    @Test
    public void testFromUnprocessableComparison()
            throws Exception
    {
        // If it is not a simple comparison, we should not try to process it
        Expression predicate = comparison(GREATER_THAN, unprocessableExpression1(A), unprocessableExpression2(A));
        ExtractionResult result = fromPredicate(predicate, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), predicate);
        Assert.assertTrue(result.getTupleDomain().isAll());

        // Complement
        predicate = not(comparison(GREATER_THAN, unprocessableExpression1(A), unprocessableExpression2(A)));
        result = fromPredicate(predicate, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), predicate);
        Assert.assertTrue(result.getTupleDomain().isAll());
    }

    @Test
    public void testFromBasicComparisons()
            throws Exception
    {
        // Test out the extraction of all basic comparisons
        Expression originalExpression = greaterThan(A, longLiteral(2L));
        ExtractionResult result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.greaterThan(2L)), false))));

        originalExpression = greaterThanOrEqual(A, longLiteral(2L));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.greaterThanOrEqual(2L)), false))));

        originalExpression = lessThan(A, longLiteral(2L));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThan(2L)), false))));

        originalExpression = lessThanOrEqual(A, longLiteral(2L));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThanOrEqual(2L)), false))));

        originalExpression = equal(A, longLiteral(2L));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.equal(2L)), false))));

        originalExpression = notEqual(A, longLiteral(2L));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThan(2L), Range.greaterThan(2L)), false))));

        originalExpression = isDistinctFrom(A, longLiteral(2L));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThan(2L), Range.greaterThan(2L)), true))));

        // Test complement
        originalExpression = not(greaterThan(A, longLiteral(2L)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThanOrEqual(2L)), false))));

        originalExpression = not(greaterThanOrEqual(A, longLiteral(2L)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThan(2L)), false))));

        originalExpression = not(lessThan(A, longLiteral(2L)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.greaterThanOrEqual(2L)), false))));

        originalExpression = not(lessThanOrEqual(A, longLiteral(2L)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.greaterThan(2L)), false))));

        originalExpression = not(equal(A, longLiteral(2L)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThan(2L), Range.greaterThan(2L)), false))));

        originalExpression = not(notEqual(A, longLiteral(2L)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.equal(2L)), false))));

        originalExpression = not(isDistinctFrom(A, longLiteral(2L)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.equal(2L)), false))));
    }

    @Test
    public void testFromFlippedBasicComparisons()
            throws Exception
    {
        // Test out the extraction of all basic comparisons where the reference literal ordering is flipped
        ComparisonExpression originalExpression = comparison(GREATER_THAN, longLiteral(2L), reference(A));
        ExtractionResult result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThan(2L)), false))));

        originalExpression = comparison(GREATER_THAN_OR_EQUAL, longLiteral(2L), reference(A));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThanOrEqual(2L)), false))));

        originalExpression = comparison(LESS_THAN, longLiteral(2L), reference(A));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.greaterThan(2L)), false))));

        originalExpression = comparison(LESS_THAN_OR_EQUAL, longLiteral(2L), reference(A));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.greaterThanOrEqual(2L)), false))));

        originalExpression = comparison(EQUAL, longLiteral(2L), reference(A));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.equal(2L)), false))));

        originalExpression = comparison(NOT_EQUAL, longLiteral(2L), reference(A));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThan(2L), Range.greaterThan(2L)), false))));

        originalExpression = comparison(IS_DISTINCT_FROM, longLiteral(2L), reference(A));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThan(2L), Range.greaterThan(2L)), true))));

        originalExpression = comparison(IS_DISTINCT_FROM, nullLiteral(), reference(A));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.notNull(Long.class))));
    }

    @Test
    public void testFromBasicComparisonsWithNulls()
            throws Exception
    {
        // Test out the extraction of all basic comparisons with null literals
        Expression originalExpression = greaterThan(A, nullLiteral());
        ExtractionResult result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertTrue(result.getTupleDomain().isNone());

        originalExpression = greaterThanOrEqual(A, nullLiteral());
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertTrue(result.getTupleDomain().isNone());

        originalExpression = lessThan(A, nullLiteral());
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertTrue(result.getTupleDomain().isNone());

        originalExpression = lessThanOrEqual(A, nullLiteral());
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertTrue(result.getTupleDomain().isNone());

        originalExpression = equal(A, nullLiteral());
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertTrue(result.getTupleDomain().isNone());

        originalExpression = notEqual(A, nullLiteral());
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertTrue(result.getTupleDomain().isNone());

        originalExpression = isDistinctFrom(A, nullLiteral());
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.notNull(Long.class))));

        // Test complements
        originalExpression = not(greaterThan(A, nullLiteral()));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(greaterThanOrEqual(A, nullLiteral()));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(lessThan(A, nullLiteral()));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(lessThanOrEqual(A, nullLiteral()));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(equal(A, nullLiteral()));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(notEqual(A, nullLiteral()));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(isDistinctFrom(A, nullLiteral()));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.onlyNull(Long.class))));
    }

    @Test
    public void testFromComparisonsWithImplictCoercions()
            throws Exception
    {
        // B is a double column. Check that it can be compared against longs
        Expression originalExpression = greaterThan(B, longLiteral(2L));
        ExtractionResult result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(BCH, Domain.create(SortedRangeSet.of(Range.greaterThan(2.0)), false))));

        // C is a string column. Check that it can be compared.
        originalExpression = greaterThan(C, stringLiteral("test"));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(CCH, Domain.create(SortedRangeSet.of(Range.greaterThan("test")), false))));

        // A is a long column. Check that it can be compared against doubles
        originalExpression = greaterThan(A, doubleLiteral(2.0));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.greaterThan(2L)), false))));

        originalExpression = greaterThan(A, doubleLiteral(2.1));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.greaterThan(2L)), false))));

        originalExpression = greaterThanOrEqual(A, doubleLiteral(2.0));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.greaterThanOrEqual(2L)), false))));

        originalExpression = greaterThanOrEqual(A, doubleLiteral(2.1));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.greaterThanOrEqual(3L)), false))));

        originalExpression = lessThan(A, doubleLiteral(2.0));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThan(2L)), false))));

        originalExpression = lessThan(A, doubleLiteral(2.1));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThan(3L)), false))));

        originalExpression = lessThanOrEqual(A, doubleLiteral(2.0));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThanOrEqual(2L)), false))));

        originalExpression = lessThanOrEqual(A, doubleLiteral(2.1));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThanOrEqual(2L)), false))));

        originalExpression = equal(A, doubleLiteral(2.0));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.equal(2L)), false))));

        originalExpression = equal(A, doubleLiteral(2.1));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.none(Long.class))));

        originalExpression = notEqual(A, doubleLiteral(2.0));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThan(2L), Range.greaterThan(2L)), false))));

        originalExpression = notEqual(A, doubleLiteral(2.1));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.notNull(Long.class))));

        originalExpression = isDistinctFrom(A, doubleLiteral(2.0));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThan(2L), Range.greaterThan(2L)), true))));

        originalExpression = isDistinctFrom(A, doubleLiteral(2.1));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertTrue(result.getTupleDomain().isAll());

        // Test complements

        // B is a double column. Check that it can be compared against longs
        originalExpression = not(greaterThan(B, longLiteral(2L)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(BCH, Domain.create(SortedRangeSet.of(Range.lessThanOrEqual(2.0)), false))));

        // C is a string column. Check that it can be compared.
        originalExpression = not(greaterThan(C, stringLiteral("test")));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(CCH, Domain.create(SortedRangeSet.of(Range.lessThanOrEqual("test")), false))));

        // A is a long column. Check that it can be compared against doubles
        originalExpression = not(greaterThan(A, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThanOrEqual(2L)), false))));

        originalExpression = not(greaterThan(A, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThanOrEqual(2L)), false))));

        originalExpression = not(greaterThanOrEqual(A, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThan(2L)), false))));

        originalExpression = not(greaterThanOrEqual(A, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThan(3L)), false))));

        originalExpression = not(lessThan(A, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.greaterThanOrEqual(2L)), false))));

        originalExpression = not(lessThan(A, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.greaterThanOrEqual(3L)), false))));

        originalExpression = not(lessThanOrEqual(A, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.greaterThan(2L)), false))));

        originalExpression = not(lessThanOrEqual(A, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.greaterThan(2L)), false))));

        originalExpression = not(equal(A, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThan(2L), Range.greaterThan(2L)), false))));

        originalExpression = not(equal(A, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.notNull(Long.class))));

        originalExpression = not(notEqual(A, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.equal(2L)), false))));

        originalExpression = not(notEqual(A, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.none(Long.class))));

        originalExpression = not(isDistinctFrom(A, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.equal(2L)), false))));

        originalExpression = not(isDistinctFrom(A, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertTrue(result.getTupleDomain().isNone());
    }

    @Test
    public void testFromUnprocessableInPredicate()
            throws Exception
    {
        Expression originalExpression = new InPredicate(unprocessableExpression1(A), new InListExpression(ImmutableList.<Expression>of(TRUE_LITERAL)));
        ExtractionResult result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), originalExpression);
        Assert.assertTrue(result.getTupleDomain().isAll());

        originalExpression = new InPredicate(reference(D), new InListExpression(ImmutableList.<Expression>of(unprocessableExpression1(D))));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), equal(D, unprocessableExpression1(D)));
        Assert.assertTrue(result.getTupleDomain().isAll());

        originalExpression = new InPredicate(reference(D), new InListExpression(ImmutableList.<Expression>of(TRUE_LITERAL, unprocessableExpression1(D))));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), or(equal(D, TRUE_LITERAL), equal(D, unprocessableExpression1(D))));
        Assert.assertTrue(result.getTupleDomain().isAll());

        // Test complement
        originalExpression = not(new InPredicate(reference(D), new InListExpression(ImmutableList.<Expression>of(unprocessableExpression1(D)))));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), not(equal(D, unprocessableExpression1(D))));
        Assert.assertTrue(result.getTupleDomain().isAll());
    }

    @Test
    public void testFromInPredicate()
            throws Exception
    {
        Expression originalExpression = in(A, ImmutableList.of(1L));
        ExtractionResult result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.singleValue(1L))));

        originalExpression = in(A, ImmutableList.of(1L, 2L));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.equal(1L), Range.equal(2L)), false))));

        originalExpression = not(in(A, ImmutableList.of(1L, 2L)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThan(1L), Range.range(1L, false, 2L, false), Range.greaterThan(2L)), false))));

        originalExpression = in(A, Arrays.asList(1L, 2L, (Expression) null));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.equal(1L), Range.equal(2L)), false))));

        originalExpression = not(in(A, Arrays.asList(1L, 2L, (Expression) null)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertTrue(result.getTupleDomain().isNone());

        originalExpression = in(A, Arrays.asList((Expression) null));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(in(A, Arrays.asList((Expression) null)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertTrue(result.getTupleDomain().isNone());
    }

    @Test
    public void testFromBetweenPredicate()
            throws Exception
    {
        Expression originalExpression = between(A, longLiteral(1L), longLiteral(2L));
        ExtractionResult result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.range(1L, true, 2L, true)), false))));

        originalExpression = between(A, longLiteral(1L), doubleLiteral(2.1));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.range(1L, true, 2L, true)), false))));

        originalExpression = between(A, longLiteral(1L), nullLiteral());
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertTrue(result.getTupleDomain().isNone());

        // Test complements
        originalExpression = not(between(A, longLiteral(1L), longLiteral(2L)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThan(1L), Range.greaterThan(2L)), false))));

        originalExpression = not(between(A, longLiteral(1L), doubleLiteral(2.1)));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThan(1L), Range.greaterThan(2L)), false))));

        originalExpression = not(between(A, longLiteral(1L), nullLiteral()));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.create(SortedRangeSet.of(Range.lessThan(1L)), false))));
    }

    @Test
    public void testFromIsNullPredicate()
            throws Exception
    {
        Expression originalExpression = isNull(A);
        ExtractionResult result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.onlyNull(Long.class))));

        originalExpression = not(isNull(A));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.notNull(Long.class))));
    }

    @Test
    public void testFromIsNotNullPredicate()
            throws Exception
    {
        Expression originalExpression = isNotNull(A);
        ExtractionResult result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.notNull(Long.class))));

        originalExpression = not(isNotNull(A));
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ACH, Domain.onlyNull(Long.class))));
    }

    @Test
    public void testFromBooleanLiteralPredicate()
            throws Exception
    {
        Expression originalExpression = TRUE_LITERAL;
        ExtractionResult result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertTrue(result.getTupleDomain().isAll());

        originalExpression = not(TRUE_LITERAL);
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertTrue(result.getTupleDomain().isNone());

        originalExpression = FALSE_LITERAL;
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(FALSE_LITERAL);
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertTrue(result.getTupleDomain().isAll());
    }

    @Test
    public void testFromNullLiteralPredicate()
            throws Exception
    {
        Expression originalExpression = nullLiteral();
        ExtractionResult result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(nullLiteral());
        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
        Assert.assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Assert.assertTrue(result.getTupleDomain().isNone());
    }

    private static Expression unprocessableExpression1(Symbol symbol)
    {
        return comparison(GREATER_THAN, reference(symbol), reference(symbol));
    }

    private static Expression unprocessableExpression2(Symbol symbol)
    {
        return comparison(LESS_THAN, reference(symbol), reference(symbol));
    }

    private static Expression randPredicate(Symbol symbol)
    {
        return comparison(GREATER_THAN, reference(symbol), new FunctionCall(new QualifiedName("rand"), ImmutableList.<Expression>of()));
    }

    private static QualifiedNameReference reference(Symbol symbol)
    {
        return new QualifiedNameReference(symbol.toQualifiedName());
    }

    private static NotExpression not(Expression expression)
    {
        return new NotExpression(expression);
    }

    private static ComparisonExpression comparison(ComparisonExpression.Type type, Expression expression1, Expression expression2)
    {
        return new ComparisonExpression(type, expression1, expression2);
    }

    private static ComparisonExpression equal(Symbol symbol, Expression expression)
    {
        return comparison(EQUAL, reference(symbol), expression);
    }

    private static ComparisonExpression notEqual(Symbol symbol, Expression expression)
    {
        return comparison(NOT_EQUAL, reference(symbol), expression);
    }

    private static ComparisonExpression greaterThan(Symbol symbol, Expression expression)
    {
        return comparison(GREATER_THAN, reference(symbol), expression);
    }

    private static ComparisonExpression greaterThanOrEqual(Symbol symbol, Expression expression)
    {
        return comparison(GREATER_THAN_OR_EQUAL, reference(symbol), expression);
    }

    private static ComparisonExpression lessThan(Symbol symbol, Expression expression)
    {
        return comparison(LESS_THAN, reference(symbol), expression);
    }

    private static ComparisonExpression lessThanOrEqual(Symbol symbol, Expression expression)
    {
        return comparison(LESS_THAN_OR_EQUAL, reference(symbol), expression);
    }

    private static ComparisonExpression isDistinctFrom(Symbol symbol, Expression expression)
    {
        return comparison(IS_DISTINCT_FROM, reference(symbol), expression);
    }

    private static IsNotNullPredicate isNotNull(Symbol symbol)
    {
        return new IsNotNullPredicate(reference(symbol));
    }

    private static IsNullPredicate isNull(Symbol symbol)
    {
        return new IsNullPredicate(reference(symbol));
    }

    private static InPredicate in(Symbol symbol, List<?> values)
    {
        return new InPredicate(reference(symbol), new InListExpression(LiteralInterpreter.toExpressions(values)));
    }

    private static BetweenPredicate between(Symbol symbol, Expression min, Expression max)
    {
        return new BetweenPredicate(reference(symbol), min, max);
    }

    private static LongLiteral longLiteral(long value)
    {
        return new LongLiteral(Long.toString(value));
    }

    private static DoubleLiteral doubleLiteral(double value)
    {
        return new DoubleLiteral(Double.toString(value));
    }

    private static StringLiteral stringLiteral(String value)
    {
        return new StringLiteral(value);
    }

    private static NullLiteral nullLiteral()
    {
        return new NullLiteral();
    }
}
