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
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.DomainTranslator.ExtractionResult;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
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
import java.util.function.Function;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.FunctionRegistry.getMagicLiteralFunctionSignature;
import static com.facebook.presto.spi.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.Decimals.encodeScaledValue;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.FloatType.FLOAT;
import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.ExpressionUtils.or;
import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.GREATER_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.IS_DISTINCT_FROM;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.LESS_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.NOT_EQUAL;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.facebook.presto.type.ColorType.COLOR;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestDomainTranslator
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();

    private static final Symbol A = new Symbol("c_bigint");
    private static final Symbol B = new Symbol("c_double");
    private static final Symbol C = new Symbol("c_varchar");
    private static final Symbol D = new Symbol("c_boolean");
    private static final Symbol E = new Symbol("c_bigint_1");
    private static final Symbol F = new Symbol("c_double_1");
    private static final Symbol G = new Symbol("c_varchar_1");
    private static final Symbol H = new Symbol("c_timestamp");
    private static final Symbol I = new Symbol("c_date");
    private static final Symbol J = new Symbol("c_color");
    private static final Symbol K = new Symbol("c_hyper_log_log");
    private static final Symbol L = new Symbol("c_varbinary");
    private static final Symbol M = new Symbol("c_decimal_26_5");
    private static final Symbol N = new Symbol("c_decimal_23_4");
    private static final Symbol O = new Symbol("c_integer");
    private static final Symbol P = new Symbol("c_decimal_21_3");
    private static final Symbol R = new Symbol("c_decimal_12_2");
    private static final Symbol S = new Symbol("c_decimal_6_1");
    private static final Symbol T = new Symbol("c_decimal_3_0");
    private static final Symbol U = new Symbol("c_decimal_2_0");
    private static final Symbol V = new Symbol("c_smallint");
    private static final Symbol W = new Symbol("c_tinyint");

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
            .put(M, createDecimalType(26, 5))
            .put(N, createDecimalType(23, 4))
            .put(O, INTEGER)
            .put(P, createDecimalType(21, 3))
            .put(R, createDecimalType(12, 2))
            .put(S, createDecimalType(6, 1))
            .put(T, createDecimalType(3, 0))
            .put(U, createDecimalType(2, 0))
            .put(V, SMALLINT)
            .put(W, TINYINT)
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
    public void testNumericTypeTranslation()
            throws Exception
    {
        List<NumericValues> translationChain = ImmutableList.of(
                // DOUBLE
                new NumericValues(B, (value) -> doubleLiteral((double) value), -1.0 * Double.MAX_VALUE, -22.0, -44.555678, 23.0, 44.555678, Double.MAX_VALUE),
                // DECIMAL(26, 5)
                new NumericValues(M, (value) -> decimalLiteral(Decimals.toString((Slice) value, 5)), longDecimal("-999999999999999999999.99999"), longDecimal("-22.00000"), longDecimal("-44.55568"), longDecimal("23.00000"), longDecimal("44.55567"), longDecimal("999999999999999999999.99999")),
                // DECIMAL(23, 4)
                new NumericValues(N, (value) -> decimalLiteral(Decimals.toString((Slice) value, 4)), longDecimal("-9999999999999999999.9999"), longDecimal("-22.0000"), longDecimal("-44.5557"), longDecimal("23.0000"), longDecimal("44.5556"), longDecimal("9999999999999999999.9999")),
                // BIGINT
                new NumericValues(A, (value) -> bigintLiteral((long) value), Long.MIN_VALUE, -22L, -45L, 23L, 44L, Long.MAX_VALUE),
                // DECIMAL(21, 3)
                new NumericValues(P, (value) -> decimalLiteral(Decimals.toString((Slice) value, 3)),
                        longDecimal("-999999999999999999.999"), longDecimal("-22.000"), longDecimal("-44.556"), longDecimal("23.000"), longDecimal("44.555"), longDecimal("999999999999999999.999")),
                // DECIMAL(12, 2)
                new NumericValues(R, (value) -> decimalLiteral(Decimals.toString((Long) value, 2)),
                        shortDecimal("-9999999999.99"), shortDecimal("-22.00"), shortDecimal("-44.56"), shortDecimal("23.00"), shortDecimal("44.55"), shortDecimal("9999999999.99")),
                // INTEGER
                new NumericValues(O, (value) -> integerLiteral((long) value), (long) Integer.MIN_VALUE, -22L, -45L, 23L, 44L, (long) Integer.MAX_VALUE),
                // DECIMAL(6, 1)
                new NumericValues(S, (value) -> decimalLiteral(Decimals.toString((Long) value, 1)),
                        shortDecimal("-99999.9"), shortDecimal("-22.0"), shortDecimal("-44.6"), shortDecimal("23.0"), shortDecimal("44.5"), shortDecimal("99999.9")),
                // SMALLINT
                new NumericValues(V, (value) -> smallintLiteral((long) value), (long) Short.MIN_VALUE, -22L, -45L, 23L, 44L, (long) Short.MAX_VALUE),
                // DECIMAL(3, 0)
                new NumericValues(T, (value) -> decimalLiteral(Decimals.toString((Long) value, 0)),
                        shortDecimal("-999"), shortDecimal("-22"), shortDecimal("-45"), shortDecimal("23"), shortDecimal("44"), shortDecimal("999")),
                // TINYINT
                new NumericValues(W, (value) -> tinyintLiteral((long) value), (long) Byte.MIN_VALUE, -22L, -45L, 23L, 44L, (long) Byte.MAX_VALUE),
                // DECIMAL(2, 0)
                new NumericValues(U, (value) -> decimalLiteral(Decimals.toString((Long) value, 0)),
                        shortDecimal("-99"), shortDecimal("-22"), shortDecimal("-45"), shortDecimal("23"), shortDecimal("44"), shortDecimal("99"))
        );

        for (int literalIndex = 0; literalIndex < translationChain.size(); literalIndex++) {
            for (int columnIndex = literalIndex + 1; columnIndex < translationChain.size(); columnIndex++) {
                NumericValues literal = translationChain.get(literalIndex);
                NumericValues column = translationChain.get(columnIndex);
                testNumericTypeTranslation(column, literal);
            }
        }
    }

    private void testNumericTypeTranslation(NumericValues columnValues, NumericValues literalValues)
    {
        Symbol columnSymbol = columnValues.getColumn();
        Type columnType = columnValues.getType();
        Function<Object, Expression> literalConstructor = literalValues.getLiteralConstructor();

        // greater than or equal
        testSimpleComparison(greaterThanOrEqual(columnSymbol, literalConstructor.apply(literalValues.getPositive())), columnSymbol, Range.greaterThanOrEqual(columnType, columnValues.getPositive()));
        testSimpleComparison(greaterThanOrEqual(columnSymbol, literalConstructor.apply(literalValues.getNegative())), columnSymbol, Range.greaterThanOrEqual(columnType, columnValues.getNegative()));
        testSimpleComparison(greaterThanOrEqual(columnSymbol, literalConstructor.apply(literalValues.getMax())), columnSymbol, Range.greaterThan(columnType, columnValues.getMax()));
        testSimpleComparison(greaterThanOrEqual(columnSymbol, literalConstructor.apply(literalValues.getMin())), columnSymbol, Range.greaterThanOrEqual(columnType, columnValues.getMin()));
        if (literalValues.isFractional()) {
            testSimpleComparison(greaterThanOrEqual(columnSymbol, literalConstructor.apply(literalValues.getFractionalPositive())), columnSymbol, Range.greaterThan(columnType, columnValues.getFractionalPositive()));
            testSimpleComparison(greaterThanOrEqual(columnSymbol, literalConstructor.apply(literalValues.getFractionalNegative())), columnSymbol, Range.greaterThan(columnType, columnValues.getFractionalNegative()));
        }

        // greater than
        testSimpleComparison(greaterThan(columnSymbol, literalConstructor.apply(literalValues.getPositive())), columnSymbol, Range.greaterThan(columnType, columnValues.getPositive()));
        testSimpleComparison(greaterThan(columnSymbol, literalConstructor.apply(literalValues.getNegative())), columnSymbol, Range.greaterThan(columnType, columnValues.getNegative()));
        testSimpleComparison(greaterThan(columnSymbol, literalConstructor.apply(literalValues.getMax())), columnSymbol, Range.greaterThan(columnType, columnValues.getMax()));
        testSimpleComparison(greaterThan(columnSymbol, literalConstructor.apply(literalValues.getMin())), columnSymbol, Range.greaterThanOrEqual(columnType, columnValues.getMin()));
        if (literalValues.isFractional()) {
            testSimpleComparison(greaterThan(columnSymbol, literalConstructor.apply(literalValues.getFractionalPositive())), columnSymbol, Range.greaterThan(columnType, columnValues.getFractionalPositive()));
            testSimpleComparison(greaterThan(columnSymbol, literalConstructor.apply(literalValues.getFractionalNegative())), columnSymbol, Range.greaterThan(columnType, columnValues.getFractionalNegative()));
        }

        // less than or equal
        testSimpleComparison(lessThanOrEqual(columnSymbol, literalConstructor.apply(literalValues.getPositive())), columnSymbol, Range.lessThanOrEqual(columnType, columnValues.getPositive()));
        testSimpleComparison(lessThanOrEqual(columnSymbol, literalConstructor.apply(literalValues.getNegative())), columnSymbol, Range.lessThanOrEqual(columnType, columnValues.getNegative()));
        testSimpleComparison(lessThanOrEqual(columnSymbol, literalConstructor.apply(literalValues.getMax())), columnSymbol, Range.lessThanOrEqual(columnType, columnValues.getMax()));
        testSimpleComparison(lessThanOrEqual(columnSymbol, literalConstructor.apply(literalValues.getMin())), columnSymbol, Range.lessThan(columnType, columnValues.getMin()));
        if (literalValues.isFractional()) {
            testSimpleComparison(lessThanOrEqual(columnSymbol, literalConstructor.apply(literalValues.getFractionalPositive())), columnSymbol, Range.lessThanOrEqual(columnType, columnValues.getFractionalPositive()));
            testSimpleComparison(lessThanOrEqual(columnSymbol, literalConstructor.apply(literalValues.getFractionalNegative())), columnSymbol, Range.lessThanOrEqual(columnType, columnValues.getFractionalNegative()));
        }

        // less than
        testSimpleComparison(lessThan(columnSymbol, literalConstructor.apply(literalValues.getPositive())), columnSymbol, Range.lessThan(columnType, columnValues.getPositive()));
        testSimpleComparison(lessThan(columnSymbol, literalConstructor.apply(literalValues.getNegative())), columnSymbol, Range.lessThan(columnType, columnValues.getNegative()));
        testSimpleComparison(lessThan(columnSymbol, literalConstructor.apply(literalValues.getMax())), columnSymbol, Range.lessThanOrEqual(columnType, columnValues.getMax()));
        testSimpleComparison(lessThan(columnSymbol, literalConstructor.apply(literalValues.getMin())), columnSymbol, Range.lessThan(columnType, columnValues.getMin()));
        if (literalValues.isFractional()) {
            testSimpleComparison(lessThan(columnSymbol, literalConstructor.apply(literalValues.getFractionalPositive())), columnSymbol, Range.lessThanOrEqual(columnType, columnValues.getFractionalPositive()));
            testSimpleComparison(lessThan(columnSymbol, literalConstructor.apply(literalValues.getFractionalNegative())), columnSymbol, Range.lessThanOrEqual(columnType, columnValues.getFractionalNegative()));
        }

        // equal
        testSimpleComparison(equal(columnSymbol, literalConstructor.apply(literalValues.getPositive())), columnSymbol, Range.equal(columnType, columnValues.getPositive()));
        testSimpleComparison(equal(columnSymbol, literalConstructor.apply(literalValues.getNegative())), columnSymbol, Range.equal(columnType, columnValues.getNegative()));
        testSimpleComparison(equal(columnSymbol, literalConstructor.apply(literalValues.getMax())), columnSymbol, Domain.none(columnType));
        testSimpleComparison(equal(columnSymbol, literalConstructor.apply(literalValues.getMin())), columnSymbol, Domain.none(columnType));
        if (literalValues.isFractional()) {
            testSimpleComparison(equal(columnSymbol, literalConstructor.apply(literalValues.getFractionalPositive())), columnSymbol, Domain.none(columnType));
            testSimpleComparison(equal(columnSymbol, literalConstructor.apply(literalValues.getFractionalNegative())), columnSymbol, Domain.none(columnType));
        }

        // not equal
        testSimpleComparison(notEqual(columnSymbol, literalConstructor.apply(literalValues.getPositive())), columnSymbol, Domain.create(ValueSet.ofRanges(Range.lessThan(columnType, columnValues.getPositive()), Range.greaterThan(columnType, columnValues.getPositive())), false));
        testSimpleComparison(notEqual(columnSymbol, literalConstructor.apply(literalValues.getNegative())), columnSymbol, Domain.create(ValueSet.ofRanges(Range.lessThan(columnType, columnValues.getNegative()), Range.greaterThan(columnType, columnValues.getNegative())), false));
        testSimpleComparison(notEqual(columnSymbol, literalConstructor.apply(literalValues.getMax())), columnSymbol, Domain.notNull(columnType));
        testSimpleComparison(notEqual(columnSymbol, literalConstructor.apply(literalValues.getMin())), columnSymbol, Domain.notNull(columnType));
        if (literalValues.isFractional()) {
            testSimpleComparison(notEqual(columnSymbol, literalConstructor.apply(literalValues.getFractionalPositive())), columnSymbol, Domain.notNull(columnType));
            testSimpleComparison(notEqual(columnSymbol, literalConstructor.apply(literalValues.getFractionalNegative())), columnSymbol, Domain.notNull(columnType));
        }

        // is distinct from
        testSimpleComparison(isDistinctFrom(columnSymbol, literalConstructor.apply(literalValues.getPositive())), columnSymbol, Domain.create(ValueSet.ofRanges(Range.lessThan(columnType, columnValues.getPositive()), Range.greaterThan(columnType, columnValues.getPositive())), true));
        testSimpleComparison(isDistinctFrom(columnSymbol, literalConstructor.apply(literalValues.getNegative())), columnSymbol, Domain.create(ValueSet.ofRanges(Range.lessThan(columnType, columnValues.getNegative()), Range.greaterThan(columnType, columnValues.getNegative())), true));
        testSimpleComparison(isDistinctFrom(columnSymbol, literalConstructor.apply(literalValues.getMax())), columnSymbol, Domain.all(columnType));
        testSimpleComparison(isDistinctFrom(columnSymbol, literalConstructor.apply(literalValues.getMin())), columnSymbol, Domain.all(columnType));
        if (literalValues.isFractional()) {
            testSimpleComparison(isDistinctFrom(columnSymbol, literalConstructor.apply(literalValues.getFractionalPositive())), columnSymbol, Domain.all(columnType));
            testSimpleComparison(isDistinctFrom(columnSymbol, literalConstructor.apply(literalValues.getFractionalNegative())), columnSymbol, Domain.all(columnType));
        }
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

    private static ComparisonExpression comparison(ComparisonExpression.Type type, Expression expression1, Expression expression2)
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

    private static Literal integerLiteral(long value)
    {
        checkArgument(value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE);
        return new GenericLiteral("INTEGER", Long.toString(value));
    }

    private static Literal smallintLiteral(long value)
    {
        checkArgument(value >= Short.MIN_VALUE && value <= Short.MAX_VALUE);
        return new GenericLiteral("SMALLINT", Long.toString(value));
    }

    private static Literal tinyintLiteral(long value)
    {
        checkArgument(value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE);
        return new GenericLiteral("TINYINT", Long.toString(value));
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

    private static Slice longDecimal(String value)
    {
        return encodeScaledValue(new BigDecimal(value));
    }

    private static void testSimpleComparison(Expression expression, Symbol symbol, Range expectedDomainRange)
    {
        testSimpleComparison(expression, symbol, Domain.create(ValueSet.ofRanges(expectedDomainRange), false));
    }

    private static void testSimpleComparison(Expression expression, Symbol symbol, Domain domain)
    {
        ExtractionResult result = fromPredicate(expression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        TupleDomain<Symbol> actual = result.getTupleDomain();
        TupleDomain<Symbol> expected = withColumnDomains(ImmutableMap.of(symbol, domain));
        if (!actual.equals(expected)) {
            fail(format("for comparison [%s] expected %s but found %s", expression.toString(), expected.toString(SESSION), actual.toString(SESSION)));
        }
    }

    private static class NumericValues
    {
        private final Type type;
        private final Symbol column;
        private final Function<Object, Expression> literalConstructor;
        private final Object min;
        private final Object negative;
        private final Object fractionalNegative;
        private final Object positive;
        private final Object fractionalPositive;
        private final Object max;

        public NumericValues(Symbol column, Function<Object, Expression> literalConstructor, Object min, Object negative, Object fractionalNegative, Object positive, Object fractionalPositive, Object max)
        {
            this.type = TYPES.get(column);
            this.column = column;
            this.literalConstructor = literalConstructor;
            this.min = min;
            this.negative = negative;
            this.fractionalNegative = fractionalNegative;
            this.positive = positive;
            this.fractionalPositive = fractionalPositive;
            this.max = max;
        }

        public Type getType()
        {
            return type;
        }

        public Symbol getColumn()
        {
            return column;
        }

        public Function<Object, Expression> getLiteralConstructor()
        {
            return literalConstructor;
        }

        public Object getMin()
        {
            return min;
        }

        public Object getNegative()
        {
            return negative;
        }

        public Object getFractionalNegative()
        {
            return fractionalNegative;
        }

        public Object getPositive()
        {
            return positive;
        }

        public Object getFractionalPositive()
        {
            return fractionalPositive;
        }

        public Object getMax()
        {
            return max;
        }

        public boolean isFractional()
        {
            return type == DOUBLE || type == FLOAT || (type instanceof DecimalType && ((DecimalType) type).getScale() > 0);
        }
    }
}
