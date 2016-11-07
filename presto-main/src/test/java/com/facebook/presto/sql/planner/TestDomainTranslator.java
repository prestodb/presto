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
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.DomainTranslator.ExtractionResult;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.FunctionRegistry.getMagicLiteralFunctionSignature;
import static com.facebook.presto.spi.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.ExpressionUtils.or;
import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.GREATER_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.IS_DISTINCT_FROM;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.LESS_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.NOT_EQUAL;
import static com.facebook.presto.type.ColorType.COLOR;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Collections.nCopies;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDomainTranslator
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();

    private static final Symbol A = new Symbol("a");
    private static final Symbol B = new Symbol("b");
    private static final Symbol C = new Symbol("c");
    private static final Symbol D = new Symbol("d");
    private static final Symbol E = new Symbol("e");
    private static final Symbol F = new Symbol("f");
    private static final Symbol G = new Symbol("g");
    private static final Symbol H = new Symbol("h");
    private static final Symbol I = new Symbol("i");
    private static final Symbol J = new Symbol("j");
    private static final Symbol K = new Symbol("k");
    private static final Symbol L = new Symbol("l");
    private static final Symbol M = new Symbol("m");
    private static final Symbol N = new Symbol("n");
    private static final Symbol O = new Symbol("o");
    private static final Symbol P = new Symbol("p");

    private static final Map<Symbol, Type> TYPES = ImmutableMap.<Symbol, Type>builder()
            .put(A, BIGINT)
            .put(B, DOUBLE)
            .put(C, VARCHAR)
            .put(D, BOOLEAN)
            .put(E, BIGINT)
            .put(F, DOUBLE)
            .put(G, VARCHAR)
            .put(H, TIMESTAMP)
            .put(I, DATE)
            .put(J, COLOR) // Equatable, but not orderable
            .put(K, HYPER_LOG_LOG) // Not Equatable or orderable
            .put(L, VARBINARY)
            .put(M, createDecimalType(10, 5))
            .put(N, createDecimalType(4, 2))
            .put(O, INTEGER)
            .put(P, createCharType(10))
            .build();

    private static final long TIMESTAMP_VALUE = new DateTime(2013, 3, 30, 1, 5, 0, 0, DateTimeZone.UTC).getMillis();
    private static final long DATE_VALUE = new DateTime(2001, 1, 22, 0, 0, 0, 0, DateTimeZone.UTC).getMillis();
    private static final long COLOR_VALUE_1 = 1;
    private static final long COLOR_VALUE_2 = 2;

    @Test
    public void testNoneRoundTrip()
            throws Exception
    {
        TupleDomain<Symbol> tupleDomain = TupleDomain.none();
        ExtractionResult result = fromPredicate(toPredicate(tupleDomain));
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), tupleDomain);
    }

    @Test
    public void testAllRoundTrip()
            throws Exception
    {
        TupleDomain<Symbol> tupleDomain = TupleDomain.all();
        ExtractionResult result = fromPredicate(toPredicate(tupleDomain));
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), tupleDomain);
    }

    @Test
    public void testRoundTrip()
            throws Exception
    {
        TupleDomain<Symbol> tupleDomain = withColumnDomains(ImmutableMap.<Symbol, Domain>builder()
                .put(A, Domain.singleValue(BIGINT, 1L))
                .put(B, Domain.onlyNull(DOUBLE))
                .put(C, Domain.notNull(VARCHAR))
                .put(D, Domain.singleValue(BOOLEAN, true))
                .put(E, Domain.singleValue(BIGINT, 2L))
                .put(F, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(DOUBLE, 1.1), Range.equal(DOUBLE, 2.0), Range.range(DOUBLE, 3.0, false, 3.5, true)), true))
                .put(G, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(VARCHAR, utf8Slice("2013-01-01")), Range.greaterThan(VARCHAR, utf8Slice("2013-10-01"))), false))
                .put(H, Domain.singleValue(TIMESTAMP, TIMESTAMP_VALUE))
                .put(I, Domain.singleValue(DATE, DATE_VALUE))
                .put(J, Domain.singleValue(COLOR, COLOR_VALUE_1))
                .put(K, Domain.notNull(HYPER_LOG_LOG))
                .build());

        ExtractionResult result = fromPredicate(toPredicate(tupleDomain));
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), tupleDomain);
    }

    @Test
    public void testInOptimization()
            throws Exception
    {
        Domain testDomain = Domain.create(
                ValueSet.all(BIGINT)
                        .subtract(ValueSet.ofRanges(
                                Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L))), false);

        TupleDomain<Symbol> tupleDomain = withColumnDomains(ImmutableMap.<Symbol, Domain>builder().put(A, testDomain).build());
        assertEquals(toPredicate(tupleDomain), not(in(A, ImmutableList.of(1L, 2L, 3L))));

        testDomain = Domain.create(
                ValueSet.ofRanges(
                        Range.lessThan(BIGINT, 4L)).intersect(
                        ValueSet.all(BIGINT)
                                .subtract(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L)))), false);

        tupleDomain = withColumnDomains(ImmutableMap.<Symbol, Domain>builder().put(A, testDomain).build());
        assertEquals(toPredicate(tupleDomain), and(lessThan(A, bigintLiteral(4L)), not(in(A, ImmutableList.of(1L, 2L, 3L)))));

        testDomain = Domain.create(ValueSet.ofRanges(
                Range.range(BIGINT, 1L, true, 3L, true),
                Range.range(BIGINT, 5L, true, 7L, true),
                Range.range(BIGINT, 9L, true, 11L, true)),
                false);

        tupleDomain = withColumnDomains(ImmutableMap.<Symbol, Domain>builder().put(A, testDomain).build());
        assertEquals(toPredicate(tupleDomain),
                or(between(A, bigintLiteral(1L), bigintLiteral(3L)), (between(A, bigintLiteral(5L), bigintLiteral(7L))), (between(A, bigintLiteral(9L), bigintLiteral(11L)))));

        testDomain = Domain.create(
                ValueSet.ofRanges(
                        Range.lessThan(BIGINT, 4L))
                        .intersect(ValueSet.all(BIGINT)
                                .subtract(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L))))
                        .union(ValueSet.ofRanges(Range.range(BIGINT, 7L, true, 9L, true))), false);

        tupleDomain = withColumnDomains(ImmutableMap.<Symbol, Domain>builder().put(A, testDomain).build());
        assertEquals(toPredicate(tupleDomain), or(and(lessThan(A, bigintLiteral(4L)), not(in(A, ImmutableList.of(1L, 2L, 3L)))), between(A, bigintLiteral(7L), bigintLiteral(9L))));

        testDomain = Domain.create(
                ValueSet.ofRanges(Range.lessThan(BIGINT, 4L))
                        .intersect(ValueSet.all(BIGINT)
                                .subtract(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L))))
                        .union(ValueSet.ofRanges(Range.range(BIGINT, 7L, false, 9L, false), Range.range(BIGINT, 11L, false, 13L, false))), false);

        tupleDomain = withColumnDomains(ImmutableMap.<Symbol, Domain>builder().put(A, testDomain).build());
        assertEquals(toPredicate(tupleDomain), or(
                and(lessThan(A, bigintLiteral(4L)), not(in(A, ImmutableList.of(1L, 2L, 3L)))),
                and(greaterThan(A, bigintLiteral(7L)), lessThan(A, bigintLiteral(9L))),
                and(greaterThan(A, bigintLiteral(11L)), lessThan(A, bigintLiteral(13L)))));
    }

    @Test
    public void testToPredicateNone()
            throws Exception
    {
        TupleDomain<Symbol> tupleDomain = withColumnDomains(ImmutableMap.<Symbol, Domain>builder()
                .put(A, Domain.singleValue(BIGINT, 1L))
                .put(B, Domain.onlyNull(DOUBLE))
                .put(C, Domain.notNull(VARCHAR))
                .put(D, Domain.none(BOOLEAN))
                .build());

        assertEquals(toPredicate(tupleDomain), FALSE_LITERAL);
    }

    @Test
    public void testToPredicateAllIgnored()
            throws Exception
    {
        TupleDomain<Symbol> tupleDomain = withColumnDomains(ImmutableMap.<Symbol, Domain>builder()
                .put(A, Domain.singleValue(BIGINT, 1L))
                .put(B, Domain.onlyNull(DOUBLE))
                .put(C, Domain.notNull(VARCHAR))
                .put(D, Domain.all(BOOLEAN))
                .build());

        ExtractionResult result = fromPredicate(toPredicate(tupleDomain));
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<Symbol, Domain>builder()
                .put(A, Domain.singleValue(BIGINT, 1L))
                .put(B, Domain.onlyNull(DOUBLE))
                .put(C, Domain.notNull(VARCHAR))
                .build()));
    }

    @Test
    public void testToPredicate()
            throws Exception
    {
        TupleDomain<Symbol> tupleDomain;

        tupleDomain = withColumnDomains(ImmutableMap.of(A, Domain.notNull(BIGINT)));
        assertEquals(toPredicate(tupleDomain), isNotNull(A));

        tupleDomain = withColumnDomains(ImmutableMap.of(A, Domain.onlyNull(BIGINT)));
        assertEquals(toPredicate(tupleDomain), isNull(A));

        tupleDomain = withColumnDomains(ImmutableMap.of(A, Domain.none(BIGINT)));
        assertEquals(toPredicate(tupleDomain), FALSE_LITERAL);

        tupleDomain = withColumnDomains(ImmutableMap.of(A, Domain.all(BIGINT)));
        assertEquals(toPredicate(tupleDomain), TRUE_LITERAL);

        tupleDomain = withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 1L)), false)));
        assertEquals(toPredicate(tupleDomain), greaterThan(A, bigintLiteral(1L)));

        tupleDomain = withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 1L)), false)));
        assertEquals(toPredicate(tupleDomain), greaterThanOrEqual(A, bigintLiteral(1L)));

        tupleDomain = withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L)), false)));
        assertEquals(toPredicate(tupleDomain), lessThan(A, bigintLiteral(1L)));

        tupleDomain = withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 0L, false, 1L, true)), false)));
        assertEquals(toPredicate(tupleDomain), and(greaterThan(A, bigintLiteral(0L)), lessThanOrEqual(A, bigintLiteral(1L))));

        tupleDomain = withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 1L)), false)));
        assertEquals(toPredicate(tupleDomain), lessThanOrEqual(A, bigintLiteral(1L)));

        tupleDomain = withColumnDomains(ImmutableMap.of(A, Domain.singleValue(BIGINT, 1L)));
        assertEquals(toPredicate(tupleDomain), equal(A, bigintLiteral(1L)));

        tupleDomain = withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false)));
        assertEquals(toPredicate(tupleDomain), in(A, ImmutableList.of(1L, 2L)));

        tupleDomain = withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L)), true)));
        assertEquals(toPredicate(tupleDomain), or(lessThan(A, bigintLiteral(1L)), isNull(A)));

        tupleDomain = withColumnDomains(ImmutableMap.of(J, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1), true)));
        assertEquals(toPredicate(tupleDomain), or(equal(J, colorLiteral(COLOR_VALUE_1)), isNull(J)));

        tupleDomain = withColumnDomains(ImmutableMap.of(J, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1).complement(), true)));
        assertEquals(toPredicate(tupleDomain), or(not(equal(J, colorLiteral(COLOR_VALUE_1))), isNull(J)));

        tupleDomain = withColumnDomains(ImmutableMap.of(K, Domain.onlyNull(HYPER_LOG_LOG)));
        assertEquals(toPredicate(tupleDomain), isNull(K));

        tupleDomain = withColumnDomains(ImmutableMap.of(K, Domain.notNull(HYPER_LOG_LOG)));
        assertEquals(toPredicate(tupleDomain), isNotNull(K));
    }

    @Test
    public void testFromUnknownPredicate()
            throws Exception
    {
        ExtractionResult result = fromPredicate(unprocessableExpression1(A));
        assertTrue(result.getTupleDomain().isAll());
        assertEquals(result.getRemainingExpression(), unprocessableExpression1(A));

        // Test the complement
        result = fromPredicate(not(unprocessableExpression1(A)));
        assertTrue(result.getTupleDomain().isAll());
        assertEquals(result.getRemainingExpression(), not(unprocessableExpression1(A)));
    }

    @Test
    public void testFromAndPredicate()
            throws Exception
    {
        Expression originalPredicate = and(
                and(greaterThan(A, bigintLiteral(1L)), unprocessableExpression1(A)),
                and(lessThan(A, bigintLiteral(5L)), unprocessableExpression2(A)));
        ExtractionResult result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), and(unprocessableExpression1(A), unprocessableExpression2(A)));
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, false, 5L, false)), false))));

        // Test complements
        originalPredicate = not(and(
                and(greaterThan(A, bigintLiteral(1L)), unprocessableExpression1(A)),
                and(lessThan(A, bigintLiteral(5L)), unprocessableExpression2(A))));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertTrue(result.getTupleDomain().isAll());

        originalPredicate = not(and(
                not(and(greaterThan(A, bigintLiteral(1L)), unprocessableExpression1(A))),
                not(and(lessThan(A, bigintLiteral(5L)), unprocessableExpression2(A)))));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.notNull(BIGINT))));
    }

    @Test
    public void testFromOrPredicate()
            throws Exception
    {
        Expression originalPredicate = or(
                and(greaterThan(A, bigintLiteral(1L)), unprocessableExpression1(A)),
                and(lessThan(A, bigintLiteral(5L)), unprocessableExpression2(A)));
        ExtractionResult result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.notNull(BIGINT))));

        originalPredicate = or(
                and(equal(A, bigintLiteral(1L)), unprocessableExpression1(A)),
                and(equal(A, bigintLiteral(2L)), unprocessableExpression2(A)));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false))));

        // Same unprocessableExpression means that we can do more extraction
        // If both sides are operating on the same single symbol
        originalPredicate = or(
                and(equal(A, bigintLiteral(1L)), unprocessableExpression1(A)),
                and(equal(A, bigintLiteral(2L)), unprocessableExpression1(A)));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), unprocessableExpression1(A));
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false))));

        // And not if they have different symbols
        originalPredicate = or(
                and(equal(A, bigintLiteral(1L)), unprocessableExpression1(A)),
                and(equal(B, doubleLiteral(2.0)), unprocessableExpression1(A)));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertTrue(result.getTupleDomain().isAll());

        // We can make another optimization if one side is the super set of the other side
        originalPredicate = or(
                and(greaterThan(A, bigintLiteral(1L)), greaterThan(B, doubleLiteral(1.0)), unprocessableExpression1(A)),
                and(greaterThan(A, bigintLiteral(2L)), greaterThan(B, doubleLiteral(2.0)), unprocessableExpression1(A)));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), unprocessableExpression1(A));
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(
                A, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 1L)), false),
                B, Domain.create(ValueSet.ofRanges(Range.greaterThan(DOUBLE, 1.0)), false))));

        // We can't make those inferences if the unprocessableExpressions are non-deterministic
        originalPredicate = or(
                and(equal(A, bigintLiteral(1L)), randPredicate(A)),
                and(equal(A, bigintLiteral(2L)), randPredicate(A)));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false))));

        // Test complements
        originalPredicate = not(or(
                and(greaterThan(A, bigintLiteral(1L)), unprocessableExpression1(A)),
                and(lessThan(A, bigintLiteral(5L)), unprocessableExpression2(A))));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), and(
                not(and(greaterThan(A, bigintLiteral(1L)), unprocessableExpression1(A))),
                not(and(lessThan(A, bigintLiteral(5L)), unprocessableExpression2(A)))));
        assertTrue(result.getTupleDomain().isAll());

        originalPredicate = not(or(
                not(and(greaterThan(A, bigintLiteral(1L)), unprocessableExpression1(A))),
                not(and(lessThan(A, bigintLiteral(5L)), unprocessableExpression2(A)))));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), and(unprocessableExpression1(A), unprocessableExpression2(A)));
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, false, 5L, false)), false))));
    }

    @Test
    public void testFromNotPredicate()
            throws Exception
    {
        Expression originalPredicate = not(and(equal(A, bigintLiteral(1L)), unprocessableExpression1(A)));
        ExtractionResult result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertTrue(result.getTupleDomain().isAll());

        originalPredicate = not(unprocessableExpression1(A));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertTrue(result.getTupleDomain().isAll());

        originalPredicate = not(TRUE_LITERAL);
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalPredicate = not(equal(A, bigintLiteral(1L)));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L), Range.greaterThan(BIGINT, 1L)), false))));
    }

    @Test
    public void testFromUnprocessableComparison()
            throws Exception
    {
        // If it is not a simple comparison, we should not try to process it
        Expression predicate = comparison(GREATER_THAN, unprocessableExpression1(A), unprocessableExpression2(A));
        ExtractionResult result = fromPredicate(predicate);
        assertEquals(result.getRemainingExpression(), predicate);
        assertTrue(result.getTupleDomain().isAll());

        // Complement
        predicate = not(comparison(GREATER_THAN, unprocessableExpression1(A), unprocessableExpression2(A)));
        result = fromPredicate(predicate);
        assertEquals(result.getRemainingExpression(), predicate);
        assertTrue(result.getTupleDomain().isAll());
    }

    @Test
    public void testFromDecimalComparison()
            throws Exception
    {
        Expression predicate = greaterThan(M, decimalLiteral("12.345"));
        ExtractionResult result = fromPredicate(predicate);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(M, Domain.create(ValueSet.ofRanges(Range.greaterThan(createDecimalType(10, 5), 1234500L)), false))));
    }

    @Test
    public void testFromBasicComparisons()
            throws Exception
    {
        // Test out the extraction of all basic comparisons
        Expression originalExpression = greaterThan(A, bigintLiteral(2L));
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = greaterThanOrEqual(A, bigintLiteral(2L));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = lessThan(A, bigintLiteral(2L));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L)), false))));

        originalExpression = lessThanOrEqual(A, bigintLiteral(2L));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = equal(A, bigintLiteral(2L));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))));

        originalExpression = notEqual(A, bigintLiteral(2L));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = isDistinctFrom(A, bigintLiteral(2L));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), true))));

        originalExpression = equal(J, colorLiteral(COLOR_VALUE_1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(J, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1), false))));

        originalExpression = in(J, ImmutableList.of(colorLiteral(COLOR_VALUE_1), colorLiteral(COLOR_VALUE_2)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(J, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1, COLOR_VALUE_2), false))));

        originalExpression = isDistinctFrom(J, colorLiteral(COLOR_VALUE_1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(J, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1).complement(), true))));

        // Test complement
        originalExpression = not(greaterThan(A, bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = not(greaterThanOrEqual(A, bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L)), false))));

        originalExpression = not(lessThan(A, bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = not(lessThanOrEqual(A, bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(equal(A, bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(notEqual(A, bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))));

        originalExpression = not(isDistinctFrom(A, bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))));

        originalExpression = not(equal(J, colorLiteral(COLOR_VALUE_1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(J, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1).complement(), false))));

        originalExpression = not(in(J, ImmutableList.of(colorLiteral(COLOR_VALUE_1), colorLiteral(COLOR_VALUE_2))));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(J, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1, COLOR_VALUE_2).complement(), false))));

        originalExpression = not(isDistinctFrom(J, colorLiteral(COLOR_VALUE_1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(J, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1), false))));
    }

    @Test
    public void testFromFlippedBasicComparisons()
            throws Exception
    {
        // Test out the extraction of all basic comparisons where the reference literal ordering is flipped
        ComparisonExpression originalExpression = comparison(GREATER_THAN, bigintLiteral(2L), A.toSymbolReference());
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L)), false))));

        originalExpression = comparison(GREATER_THAN_OR_EQUAL, bigintLiteral(2L), A.toSymbolReference());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = comparison(LESS_THAN, bigintLiteral(2L), A.toSymbolReference());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = comparison(LESS_THAN_OR_EQUAL, bigintLiteral(2L), A.toSymbolReference());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = comparison(EQUAL, bigintLiteral(2L), A.toSymbolReference());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))));

        originalExpression = comparison(EQUAL, colorLiteral(COLOR_VALUE_1), J.toSymbolReference());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(J, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1), false))));

        originalExpression = comparison(NOT_EQUAL, bigintLiteral(2L), A.toSymbolReference());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = comparison(NOT_EQUAL, colorLiteral(COLOR_VALUE_1), J.toSymbolReference());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(J, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1).complement(), false))));

        originalExpression = comparison(IS_DISTINCT_FROM, bigintLiteral(2L), A.toSymbolReference());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), true))));

        originalExpression = comparison(IS_DISTINCT_FROM, colorLiteral(COLOR_VALUE_1), J.toSymbolReference());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(J, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1).complement(), true))));

        originalExpression = comparison(IS_DISTINCT_FROM, nullLiteral(), A.toSymbolReference());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.notNull(BIGINT))));
    }

    @Test
    public void testFromBasicComparisonsWithNulls()
            throws Exception
    {
        // Test out the extraction of all basic comparisons with null literals
        Expression originalExpression = greaterThan(A, nullLiteral());
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = greaterThan(C, new Cast(nullLiteral(), StandardTypes.VARCHAR));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C, Domain.create(ValueSet.none(VARCHAR), false))));

        originalExpression = greaterThanOrEqual(A, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = lessThan(A, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = lessThanOrEqual(A, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = equal(A, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = equal(J, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = notEqual(A, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = notEqual(J, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = isDistinctFrom(A, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.notNull(BIGINT))));

        originalExpression = isDistinctFrom(J, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(J, Domain.notNull(COLOR))));

        // Test complements
        originalExpression = not(greaterThan(A, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(greaterThanOrEqual(A, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(lessThan(A, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(lessThanOrEqual(A, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(equal(A, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(equal(J, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(notEqual(A, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(notEqual(J, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(isDistinctFrom(A, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.onlyNull(BIGINT))));

        originalExpression = not(isDistinctFrom(J, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(J, Domain.onlyNull(COLOR))));
    }

    @Test
    public void testFromComparisonsWithImplictCoercions()
            throws Exception
    {
        // B is a double column. Check that it can be compared against longs
        Expression originalExpression = greaterThan(B, bigintLiteral(2L));
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(B, Domain.create(ValueSet.ofRanges(Range.greaterThan(DOUBLE, 2.0)), false))));

        // C is a string column. Check that it can be compared.
        originalExpression = greaterThan(C, stringLiteral("test"));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C, Domain.create(ValueSet.ofRanges(Range.greaterThan(VARCHAR, utf8Slice("test"))), false))));

        // A is a long column. Check that it can be compared against doubles
        originalExpression = greaterThan(A, doubleLiteral(2.0));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = greaterThan(A, doubleLiteral(2.1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = greaterThanOrEqual(A, doubleLiteral(2.0));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = greaterThanOrEqual(A, doubleLiteral(2.1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = lessThan(A, doubleLiteral(2.0));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L)), false))));

        originalExpression = lessThan(A, doubleLiteral(2.1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = lessThanOrEqual(A, doubleLiteral(2.0));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = lessThanOrEqual(A, doubleLiteral(2.1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = equal(A, doubleLiteral(2.0));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))));

        originalExpression = equal(A, doubleLiteral(2.1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.none(BIGINT))));

        originalExpression = notEqual(A, doubleLiteral(2.0));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = notEqual(A, doubleLiteral(2.1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.notNull(BIGINT))));

        originalExpression = isDistinctFrom(A, doubleLiteral(2.0));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), true))));

        originalExpression = isDistinctFrom(A, doubleLiteral(2.1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isAll());

        // Test complements

        // B is a double column. Check that it can be compared against longs
        originalExpression = not(greaterThan(B, bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(B, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(DOUBLE, 2.0)), false))));

        // C is a string column. Check that it can be compared.
        originalExpression = not(greaterThan(C, stringLiteral("test")));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(VARCHAR, utf8Slice("test"))), false))));

        // A is a long column. Check that it can be compared against doubles
        originalExpression = not(greaterThan(A, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = not(greaterThan(A, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = not(greaterThanOrEqual(A, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L)), false))));

        originalExpression = not(greaterThanOrEqual(A, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = not(lessThan(A, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = not(lessThan(A, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(lessThanOrEqual(A, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(lessThanOrEqual(A, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(equal(A, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(equal(A, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.notNull(BIGINT))));

        originalExpression = not(notEqual(A, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))));

        originalExpression = not(notEqual(A, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.none(BIGINT))));

        originalExpression = not(isDistinctFrom(A, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))));

        originalExpression = not(isDistinctFrom(A, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());
    }

    @Test
    public void testFromUnprocessableInPredicate()
            throws Exception
    {
        Expression originalExpression = new InPredicate(unprocessableExpression1(A), new InListExpression(ImmutableList.<Expression>of(TRUE_LITERAL)));
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), originalExpression);
        assertTrue(result.getTupleDomain().isAll());

        originalExpression = new InPredicate(D.toSymbolReference(), new InListExpression(ImmutableList.of(unprocessableExpression1(D))));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), equal(D, unprocessableExpression1(D)));
        assertTrue(result.getTupleDomain().isAll());

        originalExpression = new InPredicate(D.toSymbolReference(), new InListExpression(ImmutableList.of(TRUE_LITERAL, unprocessableExpression1(D))));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), or(equal(D, TRUE_LITERAL), equal(D, unprocessableExpression1(D))));
        assertTrue(result.getTupleDomain().isAll());

        // Test complement
        originalExpression = not(new InPredicate(D.toSymbolReference(), new InListExpression(ImmutableList.of(unprocessableExpression1(D)))));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), not(equal(D, unprocessableExpression1(D))));
        assertTrue(result.getTupleDomain().isAll());
    }

    @Test
    public void testFromInPredicate()
            throws Exception
    {
        Expression originalExpression = in(A, ImmutableList.of(1L));
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.singleValue(BIGINT, 1L))));

        originalExpression = in(J, ImmutableList.of(colorLiteral(COLOR_VALUE_1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(J, Domain.singleValue(COLOR, COLOR_VALUE_1))));

        originalExpression = in(A, ImmutableList.of(1L, 2L));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false))));

        originalExpression = in(J, ImmutableList.of(colorLiteral(COLOR_VALUE_1), colorLiteral(COLOR_VALUE_2)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(J, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1, COLOR_VALUE_2), false))));

        originalExpression = not(in(A, ImmutableList.of(1L, 2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L), Range.range(BIGINT, 1L, false, 2L, false), Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(in(J, ImmutableList.of(colorLiteral(COLOR_VALUE_1), colorLiteral(COLOR_VALUE_2))));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(J, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1, COLOR_VALUE_2).complement(), false))));

        // TODO update domain translator to properly handle cast
//        originalExpression = in(A, Arrays.asList(1L, 2L, (Expression) null));
//        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
//        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
//        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(ACH, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false))));
//
//        originalExpression = not(in(A, Arrays.asList(1L, 2L, (Expression) null)));
//        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
//        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
//        assertTrue(result.getTupleDomain().isNone());
//
//        originalExpression = in(A, Arrays.asList((Expression) null));
//        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
//        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
//        assertTrue(result.getTupleDomain().isNone());
//
//        originalExpression = not(in(A, Arrays.asList((Expression) null)));
//        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
//        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
//        assertTrue(result.getTupleDomain().isNone());
    }

    @Test
    public void testFromBetweenPredicate()
            throws Exception
    {
        Expression originalExpression = between(A, bigintLiteral(1L), bigintLiteral(2L));
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 2L, true)), false))));

        originalExpression = between(A, bigintLiteral(1L), doubleLiteral(2.1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 2L, true)), false))));

        originalExpression = between(A, bigintLiteral(1L), nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        // Test complements
        originalExpression = not(between(A, bigintLiteral(1L), bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L), Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(between(A, bigintLiteral(1L), doubleLiteral(2.1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L), Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(between(A, bigintLiteral(1L), nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L)), false))));
    }

    @Test
    public void testFromIsNullPredicate()
            throws Exception
    {
        Expression originalExpression = isNull(A);
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.onlyNull(BIGINT))));

        originalExpression = isNull(K);
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(K, Domain.onlyNull(HYPER_LOG_LOG))));

        originalExpression = not(isNull(A));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.notNull(BIGINT))));

        originalExpression = not(isNull(K));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(K, Domain.notNull(HYPER_LOG_LOG))));
    }

    @Test
    public void testFromIsNotNullPredicate()
            throws Exception
    {
        Expression originalExpression = isNotNull(A);
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.notNull(BIGINT))));

        originalExpression = isNotNull(K);
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(K, Domain.notNull(HYPER_LOG_LOG))));

        originalExpression = not(isNotNull(A));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(A, Domain.onlyNull(BIGINT))));

        originalExpression = not(isNotNull(K));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(K, Domain.onlyNull(HYPER_LOG_LOG))));
    }

    @Test
    public void testFromBooleanLiteralPredicate()
            throws Exception
    {
        Expression originalExpression = TRUE_LITERAL;
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isAll());

        originalExpression = not(TRUE_LITERAL);
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = FALSE_LITERAL;
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(FALSE_LITERAL);
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isAll());
    }

    @Test
    public void testFromNullLiteralPredicate()
            throws Exception
    {
        Expression originalExpression = nullLiteral();
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());
    }

    @Test
    public void testExpressionConstantFolding()
            throws Exception
    {
        Expression originalExpression = comparison(GREATER_THAN, L.toSymbolReference(), function("from_hex", stringLiteral("123456")));
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Slice value = Slices.wrappedBuffer(BaseEncoding.base16().decode("123456"));
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(L, Domain.create(ValueSet.ofRanges(Range.greaterThan(VARBINARY, value)), false))));

        Expression expression = toPredicate(result.getTupleDomain());
        assertEquals(expression, comparison(GREATER_THAN, L.toSymbolReference(), varbinaryLiteral(value)));
    }

    @Test
    public void testBigintComparedToDoubleExpression()
            throws Exception
    {
        // greater than or equal
        testSimpleComparison(greaterThanOrEqual(A, doubleLiteral(2.5)), A, Range.greaterThan(BIGINT, 2L));
        testSimpleComparison(greaterThanOrEqual(A, doubleLiteral(2.0)), A, Range.greaterThanOrEqual(BIGINT, 2L));
        testSimpleComparison(greaterThanOrEqual(A, doubleLiteral(-2.5)), A, Range.greaterThan(BIGINT, -3L));
        testSimpleComparison(greaterThanOrEqual(A, doubleLiteral(-2.0)), A, Range.greaterThanOrEqual(BIGINT, -2L));
        testSimpleComparison(greaterThanOrEqual(A, doubleLiteral(0x1p64)), A, Range.greaterThan(BIGINT, Long.MAX_VALUE));
        testSimpleComparison(greaterThanOrEqual(A, doubleLiteral(-0x1p64)), A, Range.greaterThanOrEqual(BIGINT, Long.MIN_VALUE));

        // greater than
        testSimpleComparison(greaterThan(A, doubleLiteral(2.5)), A, Range.greaterThan(BIGINT, 2L));
        testSimpleComparison(greaterThan(A, doubleLiteral(2.0)), A, Range.greaterThan(BIGINT, 2L));
        testSimpleComparison(greaterThan(A, doubleLiteral(-2.5)), A, Range.greaterThan(BIGINT, -3L));
        testSimpleComparison(greaterThan(A, doubleLiteral(-2.0)), A, Range.greaterThan(BIGINT, -2L));
        testSimpleComparison(greaterThan(A, doubleLiteral(0x1p64)), A, Range.greaterThan(BIGINT, Long.MAX_VALUE));
        testSimpleComparison(greaterThan(A, doubleLiteral(-0x1p64)), A, Range.greaterThanOrEqual(BIGINT, Long.MIN_VALUE));

        // less than or equal
        testSimpleComparison(lessThanOrEqual(A, doubleLiteral(2.5)), A, Range.lessThanOrEqual(BIGINT, 2L));
        testSimpleComparison(lessThanOrEqual(A, doubleLiteral(2.0)), A, Range.lessThanOrEqual(BIGINT, 2L));
        testSimpleComparison(lessThanOrEqual(A, doubleLiteral(-2.5)), A, Range.lessThanOrEqual(BIGINT, -3L));
        testSimpleComparison(lessThanOrEqual(A, doubleLiteral(-2.0)), A, Range.lessThanOrEqual(BIGINT, -2L));
        testSimpleComparison(lessThanOrEqual(A, doubleLiteral(0x1p64)), A, Range.lessThanOrEqual(BIGINT, Long.MAX_VALUE));
        testSimpleComparison(lessThanOrEqual(A, doubleLiteral(-0x1p64)), A, Range.lessThan(BIGINT, Long.MIN_VALUE));

        // less than
        testSimpleComparison(lessThan(A, doubleLiteral(2.5)), A, Range.lessThanOrEqual(BIGINT, 2L));
        testSimpleComparison(lessThan(A, doubleLiteral(2.0)), A, Range.lessThan(BIGINT, 2L));
        testSimpleComparison(lessThan(A, doubleLiteral(-2.5)), A, Range.lessThanOrEqual(BIGINT, -3L));
        testSimpleComparison(lessThan(A, doubleLiteral(-2.0)), A, Range.lessThan(BIGINT, -2L));
        testSimpleComparison(lessThan(A, doubleLiteral(0x1p64)), A, Range.lessThanOrEqual(BIGINT, Long.MAX_VALUE));
        testSimpleComparison(lessThan(A, doubleLiteral(-0x1p64)), A, Range.lessThan(BIGINT, Long.MIN_VALUE));

        // equal
        testSimpleComparison(equal(A, doubleLiteral(2.5)), A, Domain.none(BIGINT));
        testSimpleComparison(equal(A, doubleLiteral(2.0)), A, Range.equal(BIGINT, 2L));
        testSimpleComparison(equal(A, doubleLiteral(-2.5)), A, Domain.none(BIGINT));
        testSimpleComparison(equal(A, doubleLiteral(-2.0)), A, Range.equal(BIGINT, -2L));
        testSimpleComparison(equal(A, doubleLiteral(0x1p64)), A, Domain.none(BIGINT));
        testSimpleComparison(equal(A, doubleLiteral(-0x1p64)), A, Domain.none(BIGINT));

        // not equal
        testSimpleComparison(notEqual(A, doubleLiteral(2.5)), A, Domain.notNull(BIGINT));
        testSimpleComparison(notEqual(A, doubleLiteral(2.0)), A, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), false));
        testSimpleComparison(notEqual(A, doubleLiteral(-2.5)), A, Domain.notNull(BIGINT));
        testSimpleComparison(notEqual(A, doubleLiteral(-2.0)), A, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, -2L), Range.greaterThan(BIGINT, -2L)), false));
        testSimpleComparison(notEqual(A, doubleLiteral(0x1p64)), A, Domain.notNull(BIGINT));
        testSimpleComparison(notEqual(A, doubleLiteral(-0x1p64)), A, Domain.notNull(BIGINT));

        // is distinct from
        testSimpleComparison(isDistinctFrom(A, doubleLiteral(2.5)), A, Domain.all(BIGINT));
        testSimpleComparison(isDistinctFrom(A, doubleLiteral(2.0)), A, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), true));
        testSimpleComparison(isDistinctFrom(A, doubleLiteral(-2.5)), A, Domain.all(BIGINT));
        testSimpleComparison(isDistinctFrom(A, doubleLiteral(-2.0)), A, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, -2L), Range.greaterThan(BIGINT, -2L)), true));
        testSimpleComparison(isDistinctFrom(A, doubleLiteral(0x1p64)), A, Domain.all(BIGINT));
        testSimpleComparison(isDistinctFrom(A, doubleLiteral(-0x1p64)), A, Domain.all(BIGINT));
    }

    @Test
    public void testIntegerComparedToDoubleExpression()
            throws Exception
    {
        // greater than or equal
        testSimpleComparison(greaterThanOrEqual(O, doubleLiteral(2.5)), O, Range.greaterThan(INTEGER, 2L));
        testSimpleComparison(greaterThanOrEqual(O, doubleLiteral(2.0)), O, Range.greaterThanOrEqual(INTEGER, 2L));
        testSimpleComparison(greaterThanOrEqual(O, doubleLiteral(0x1p32)), O, Range.greaterThan(INTEGER, (long) Integer.MAX_VALUE));

        // greater than
        testSimpleComparison(greaterThan(O, doubleLiteral(2.5)), O, Range.greaterThan(INTEGER, 2L));
        testSimpleComparison(greaterThan(O, doubleLiteral(2.0)), O, Range.greaterThan(INTEGER, 2L));
        testSimpleComparison(greaterThan(O, doubleLiteral(0x1p32)), O, Range.greaterThan(INTEGER, (long) Integer.MAX_VALUE));

        // less than or equal
        testSimpleComparison(lessThanOrEqual(O, doubleLiteral(-2.5)), O, Range.lessThanOrEqual(INTEGER, -3L));
        testSimpleComparison(lessThanOrEqual(O, doubleLiteral(-2.0)), O, Range.lessThanOrEqual(INTEGER, -2L));
        testSimpleComparison(lessThanOrEqual(O, doubleLiteral(-0x1p32)), O, Range.lessThan(INTEGER, (long) Integer.MIN_VALUE));

        // less than
        testSimpleComparison(lessThan(O, doubleLiteral(-2.5)), O, Range.lessThanOrEqual(INTEGER, -3L));
        testSimpleComparison(lessThan(O, doubleLiteral(-2.0)), O, Range.lessThan(INTEGER, -2L));
        testSimpleComparison(lessThan(O, doubleLiteral(-0x1p32)), O, Range.lessThan(INTEGER, (long) Integer.MIN_VALUE));

        // equal
        testSimpleComparison(equal(O, doubleLiteral(2.5)), O, Domain.none(INTEGER));
        testSimpleComparison(equal(O, doubleLiteral(2.0)), O, Range.equal(INTEGER, 2L));

        // not equal
        testSimpleComparison(notEqual(O, doubleLiteral(2.5)), O, Domain.notNull(INTEGER));
        testSimpleComparison(notEqual(O, doubleLiteral(2.0)), O, Domain.create(ValueSet.ofRanges(Range.lessThan(INTEGER, 2L), Range.greaterThan(INTEGER, 2L)), false));

        // is distinct from
        testSimpleComparison(isDistinctFrom(O, doubleLiteral(2.5)), O, Domain.all(INTEGER));
        testSimpleComparison(isDistinctFrom(O, doubleLiteral(2.0)), O, Domain.create(ValueSet.ofRanges(Range.lessThan(INTEGER, 2L), Range.greaterThan(INTEGER, 2L)), true));
    }

    @Test
    public void testIntegerComparedToBigintExpression()
            throws Exception
    {
        // greater than or equal
        testSimpleComparison(greaterThanOrEqual(O, bigintLiteral(2L)), O, Range.greaterThanOrEqual(INTEGER, 2L));
        testSimpleComparison(greaterThanOrEqual(O, bigintLiteral(Integer.MAX_VALUE + 1L)), O, Range.greaterThan(INTEGER, (long) Integer.MAX_VALUE));

        // greater than
        testSimpleComparison(greaterThan(O, bigintLiteral(2L)), O, Range.greaterThan(INTEGER, 2L));
        testSimpleComparison(greaterThan(O, bigintLiteral(Integer.MAX_VALUE + 1L)), O, Range.greaterThan(INTEGER, (long) Integer.MAX_VALUE));

        // less than or equal
        testSimpleComparison(lessThanOrEqual(O, bigintLiteral(-2L)), O, Range.lessThanOrEqual(INTEGER, -2L));
        testSimpleComparison(lessThanOrEqual(O, bigintLiteral(Integer.MIN_VALUE - 1L)), O, Range.lessThan(INTEGER, (long) Integer.MIN_VALUE));

        // less than
        testSimpleComparison(lessThan(O, bigintLiteral(-2L)), O, Range.lessThan(INTEGER, -2L));
        testSimpleComparison(lessThan(O, bigintLiteral(Integer.MIN_VALUE - 1L)), O, Range.lessThan(INTEGER, (long) Integer.MIN_VALUE));

        // equal
        testSimpleComparison(equal(O, bigintLiteral(Integer.MIN_VALUE - 1L)), O, Domain.none(INTEGER));
        testSimpleComparison(equal(O, bigintLiteral(-2L)), O, Range.equal(INTEGER, -2L));

        // not equal
        testSimpleComparison(notEqual(O, bigintLiteral(Integer.MAX_VALUE + 1L)), O, Domain.notNull(INTEGER));
        testSimpleComparison(notEqual(O, bigintLiteral(2L)), O, Domain.create(ValueSet.ofRanges(Range.lessThan(INTEGER, 2L), Range.greaterThan(INTEGER, 2L)), false));

        // is distinct from
        testSimpleComparison(isDistinctFrom(O, bigintLiteral(Integer.MAX_VALUE + 1L)), O, Domain.all(INTEGER));
        testSimpleComparison(isDistinctFrom(O, bigintLiteral(2L)), O, Domain.create(ValueSet.ofRanges(Range.lessThan(INTEGER, 2L), Range.greaterThan(INTEGER, 2L)), true));
    }

    @Test
    public void testDecimalComparedToWiderDecimal()
            throws Exception
    {
        // greater than or equal
        testSimpleComparison(greaterThanOrEqual(N, decimalLiteral("44.555678")), N, Range.greaterThan(createDecimalType(4, 2), shortDecimal("44.55")));
        testSimpleComparison(greaterThanOrEqual(N, decimalLiteral("99.99")), N, Range.greaterThanOrEqual(createDecimalType(4, 2), shortDecimal("99.99")));
        testSimpleComparison(greaterThanOrEqual(N, decimalLiteral("9999.999")), N, Range.greaterThan(createDecimalType(4, 2), shortDecimal("99.99")));

        // greater than
        testSimpleComparison(greaterThan(N, decimalLiteral("44.555678")), N, Range.greaterThan(createDecimalType(4, 2), shortDecimal("44.55")));
        testSimpleComparison(greaterThan(N, decimalLiteral("44.55")), N, Range.greaterThan(createDecimalType(4, 2), shortDecimal("44.55")));
        testSimpleComparison(greaterThan(N, decimalLiteral("9999.999")), N, Range.greaterThan(createDecimalType(4, 2), shortDecimal("99.99")));

        // less than or equal
        testSimpleComparison(lessThanOrEqual(N, decimalLiteral("-44.555678")), N, Range.lessThanOrEqual(createDecimalType(4, 2), shortDecimal("-44.56")));
        testSimpleComparison(lessThanOrEqual(N, decimalLiteral("-99.99")), N, Range.lessThanOrEqual(createDecimalType(4, 2), shortDecimal("-99.99")));
        testSimpleComparison(lessThanOrEqual(N, decimalLiteral("-9999.999")), N, Range.lessThan(createDecimalType(4, 2), shortDecimal("-99.99")));

        // less than
        testSimpleComparison(lessThan(N, decimalLiteral("-44.555678")), N, Range.lessThanOrEqual(createDecimalType(4, 2), shortDecimal("-44.56")));
        testSimpleComparison(lessThan(N, decimalLiteral("-99.99")), N, Range.lessThan(createDecimalType(4, 2), shortDecimal("-99.99")));
        testSimpleComparison(lessThan(N, decimalLiteral("-9999.999")), N, Range.lessThan(createDecimalType(4, 2), shortDecimal("-99.99")));

        // equal
        testSimpleComparison(equal(N, decimalLiteral("-44.555678")), N, Domain.none(createDecimalType(4, 2)));
        testSimpleComparison(equal(N, decimalLiteral("99.99")), N, Range.equal(createDecimalType(4, 2), shortDecimal("99.99")));

        // not equal
        testSimpleComparison(notEqual(N, decimalLiteral("-44.555678")), N, Domain.notNull(createDecimalType(4, 2)));
        testSimpleComparison(notEqual(N, decimalLiteral("99.99")), N, Domain.create(ValueSet.ofRanges(
                Range.lessThan(createDecimalType(4, 2), shortDecimal("99.99")), Range.greaterThan(createDecimalType(4, 2), shortDecimal("99.99"))), false));

        // is distinct from
        testSimpleComparison(isDistinctFrom(N, decimalLiteral("-44.555678")), N, Domain.all(createDecimalType(4, 2)));
        testSimpleComparison(isDistinctFrom(N, decimalLiteral("99.99")), N, Domain.create(ValueSet.ofRanges(
                Range.lessThan(createDecimalType(4, 2), shortDecimal("99.99")), Range.greaterThan(createDecimalType(4, 2), shortDecimal("99.99"))), true));
    }

    @Test
    public void testVarcharComparedToCharExpression()
            throws Exception
    {
        // greater than or equal
        testSimpleComparison(greaterThanOrEqual(P, stringLiteral("123456789")), P, Range.greaterThan(createCharType(10), Slices.utf8Slice("12345678")));
        testSimpleComparison(greaterThanOrEqual(P, stringLiteral("1234567890")), P, Range.greaterThanOrEqual(createCharType(10), Slices.utf8Slice("1234567890")));
        testSimpleComparison(greaterThanOrEqual(P, stringLiteral("12345678901")), P, Range.greaterThan(createCharType(10), Slices.utf8Slice("1234567890")));

        // greater than
        testSimpleComparison(greaterThan(P, stringLiteral("123456789")), P, Range.greaterThan(createCharType(10), Slices.utf8Slice("12345678")));
        testSimpleComparison(greaterThan(P, stringLiteral("1234567890")), P, Range.greaterThan(createCharType(10), Slices.utf8Slice("1234567890")));
        testSimpleComparison(greaterThan(P, stringLiteral("12345678901")), P, Range.greaterThan(createCharType(10), Slices.utf8Slice("1234567890")));

        // less than or equal
        testSimpleComparison(lessThanOrEqual(P, stringLiteral("123456789")), P, Range.lessThanOrEqual(createCharType(10), Slices.utf8Slice("12345678")));
        testSimpleComparison(lessThanOrEqual(P, stringLiteral("1234567890")), P, Range.lessThanOrEqual(createCharType(10), Slices.utf8Slice("1234567890")));
        testSimpleComparison(lessThanOrEqual(P, stringLiteral("12345678901")), P, Range.lessThanOrEqual(createCharType(10), Slices.utf8Slice("1234567890")));

        // less than
        testSimpleComparison(lessThan(P, stringLiteral("123456789")), P, Range.lessThanOrEqual(createCharType(10), Slices.utf8Slice("12345678")));
        testSimpleComparison(lessThan(P, stringLiteral("1234567890")), P, Range.lessThan(createCharType(10), Slices.utf8Slice("1234567890")));
        testSimpleComparison(lessThan(P, stringLiteral("12345678901")), P, Range.lessThanOrEqual(createCharType(10), Slices.utf8Slice("1234567890")));

        // equal
        testSimpleComparison(equal(P, stringLiteral("123456789")), P, Domain.none(createCharType(10)));
        testSimpleComparison(equal(P, stringLiteral("1234567890")), P, Range.equal(createCharType(10), Slices.utf8Slice("1234567890")));
        testSimpleComparison(equal(P, stringLiteral("12345678901")), P, Domain.none(createCharType(10)));

        // not equal
        testSimpleComparison(notEqual(P, stringLiteral("123456789")), P, Domain.notNull(createCharType(10)));
        testSimpleComparison(notEqual(P, stringLiteral("1234567890")), P, Domain.create(ValueSet.ofRanges(
                Range.lessThan(createCharType(10), Slices.utf8Slice("1234567890")), Range.greaterThan(createCharType(10), Slices.utf8Slice("1234567890"))), false));
        testSimpleComparison(notEqual(P, stringLiteral("12345678901")), P, Domain.notNull(createCharType(10)));

        // is distinct from
        testSimpleComparison(isDistinctFrom(P, stringLiteral("123456789")), P, Domain.all(createCharType(10)));
        testSimpleComparison(isDistinctFrom(P, stringLiteral("1234567890")), P, Domain.create(ValueSet.ofRanges(
                Range.lessThan(createCharType(10), Slices.utf8Slice("1234567890")), Range.greaterThan(createCharType(10), Slices.utf8Slice("1234567890"))), true));
        testSimpleComparison(isDistinctFrom(P, stringLiteral("12345678901")), P, Domain.all(createCharType(10)));
    }

    private static ExtractionResult fromPredicate(Expression originalPredicate)
    {
        return DomainTranslator.fromPredicate(METADATA, TEST_SESSION, originalPredicate, TYPES);
    }

    private static Expression toPredicate(TupleDomain<Symbol> tupleDomain)
    {
        return DomainTranslator.toPredicate(tupleDomain);
    }

    private static Expression unprocessableExpression1(Symbol symbol)
    {
        return comparison(GREATER_THAN, symbol.toSymbolReference(), symbol.toSymbolReference());
    }

    private static Expression unprocessableExpression2(Symbol symbol)
    {
        return comparison(LESS_THAN, symbol.toSymbolReference(), symbol.toSymbolReference());
    }

    private static Expression randPredicate(Symbol symbol)
    {
        return comparison(GREATER_THAN, symbol.toSymbolReference(), new FunctionCall(QualifiedName.of("rand"), ImmutableList.<Expression>of()));
    }

    private static NotExpression not(Expression expression)
    {
        return new NotExpression(expression);
    }

    private static ComparisonExpression comparison(ComparisonExpressionType type, Expression expression1, Expression expression2)
    {
        return new ComparisonExpression(type, expression1, expression2);
    }

    private static ComparisonExpression equal(Symbol symbol, Expression expression)
    {
        return comparison(EQUAL, symbol.toSymbolReference(), expression);
    }

    private static ComparisonExpression notEqual(Symbol symbol, Expression expression)
    {
        return comparison(NOT_EQUAL, symbol.toSymbolReference(), expression);
    }

    private static ComparisonExpression greaterThan(Symbol symbol, Expression expression)
    {
        return comparison(GREATER_THAN, symbol.toSymbolReference(), expression);
    }

    private static ComparisonExpression greaterThanOrEqual(Symbol symbol, Expression expression)
    {
        return comparison(GREATER_THAN_OR_EQUAL, symbol.toSymbolReference(), expression);
    }

    private static ComparisonExpression lessThan(Symbol symbol, Expression expression)
    {
        return comparison(LESS_THAN, symbol.toSymbolReference(), expression);
    }

    private static ComparisonExpression lessThanOrEqual(Symbol symbol, Expression expression)
    {
        return comparison(LESS_THAN_OR_EQUAL, symbol.toSymbolReference(), expression);
    }

    private static ComparisonExpression isDistinctFrom(Symbol symbol, Expression expression)
    {
        return comparison(IS_DISTINCT_FROM, symbol.toSymbolReference(), expression);
    }

    private static Expression isNotNull(Symbol symbol)
    {
        return new NotExpression(new IsNullPredicate(symbol.toSymbolReference()));
    }

    private static IsNullPredicate isNull(Symbol symbol)
    {
        return new IsNullPredicate(symbol.toSymbolReference());
    }

    private static InPredicate in(Symbol symbol, List<?> values)
    {
        List<Type> types = nCopies(values.size(), TYPES.get(symbol));
        List<Expression> expressions = LiteralInterpreter.toExpressions(values, types);
        return new InPredicate(symbol.toSymbolReference(), new InListExpression(expressions));
    }

    private static BetweenPredicate between(Symbol symbol, Expression min, Expression max)
    {
        return new BetweenPredicate(symbol.toSymbolReference(), min, max);
    }

    private static Literal bigintLiteral(long value)
    {
        if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
            return new GenericLiteral("BIGINT", Long.toString(value));
        }
        return new LongLiteral(Long.toString(value));
    }

    private static DoubleLiteral doubleLiteral(double value)
    {
        return new DoubleLiteral(Double.toString(value));
    }

    private static DecimalLiteral decimalLiteral(String value)
    {
        return new DecimalLiteral(value);
    }

    private static StringLiteral stringLiteral(String value)
    {
        return new StringLiteral(value);
    }

    private static NullLiteral nullLiteral()
    {
        return new NullLiteral();
    }

    private static FunctionCall colorLiteral(long value)
    {
        return new FunctionCall(QualifiedName.of(getMagicLiteralFunctionSignature(COLOR).getName()), ImmutableList.<Expression>of(bigintLiteral(value)));
    }

    private static Expression varbinaryLiteral(Slice value)
    {
        return LiteralInterpreter.toExpression(value, VARBINARY);
    }

    private static FunctionCall function(String functionName, Expression... args)
    {
        return new FunctionCall(QualifiedName.of(functionName), ImmutableList.copyOf(args));
    }

    private static Long shortDecimal(String value)
    {
        return new BigDecimal(value).unscaledValue().longValueExact();
    }

    private static void testSimpleComparison(Expression expression, Symbol symbol, Range expectedDomainRange)
    {
        testSimpleComparison(expression, symbol, Domain.create(ValueSet.ofRanges(expectedDomainRange), false));
    }

    private static void testSimpleComparison(Expression expression, Symbol symbol, Domain domain)
    {
        ExtractionResult result = fromPredicate(expression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(symbol, domain)));
    }
}
