package com.facebook.presto.sql.relational;

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

import com.facebook.presto.metadata.CastType;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.optimizations.RowExpressionCanonicalizer;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.testing.assertions.Assert;
import com.facebook.presto.type.JoniRegexpType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.function.OperatorType.ADD;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.DEREFERENCE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.Decimals.encodeScaledValue;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.RowType.field;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.LiteralEncoder.toRowExpression;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static com.facebook.presto.sql.relational.Expressions.variable;
import static com.facebook.presto.sql.relational.LogicalRowExpressions.FALSE;
import static com.facebook.presto.sql.relational.LogicalRowExpressions.TRUE;
import static com.facebook.presto.sql.relational.LogicalRowExpressions.and;
import static com.facebook.presto.sql.relational.LogicalRowExpressions.or;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.facebook.presto.type.ColorType.COLOR;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.floatToIntBits;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestDomainExtractor
{
    private static final RowExpression C_BIGINT = variable("x1", BIGINT);
    private static final RowExpression C_DOUBLE = variable("x2", DOUBLE);
    private static final RowExpression C_VARCHAR = variable("x3", VARCHAR);
    private static final RowExpression C_BOOLEAN = variable("x4", BOOLEAN);
    private static final RowExpression C_BIGINT_1 = variable("x5", BIGINT);
    private static final RowExpression C_DOUBLE_1 = variable("x6", DOUBLE);
    private static final RowExpression C_VARCHAR_1 = variable("x7", VARCHAR);
    private static final RowExpression C_TIMESTAMP = variable("x8", TIMESTAMP);
    private static final RowExpression C_DATE = variable("x9", DATE);
    private static final RowExpression C_COLOR = variable("x10", COLOR);
    private static final RowExpression C_HYPER_LOG_LOG = variable("x11", HYPER_LOG_LOG);
    private static final RowExpression C_VARBINARY = variable("x12", VARBINARY);
    private static final RowExpression C_DECIMAL_26_5 = variable("x13", createDecimalType(26, 5));
    private static final RowExpression C_DECIMAL_23_4 = variable("x14", createDecimalType(23, 4));
    private static final RowExpression C_INTEGER = variable("x15", INTEGER);
    private static final RowExpression C_CHAR = variable("x16", createCharType(10));
    private static final RowExpression C_DECIMAL_21_3 = variable("x17", createDecimalType(21, 3));
    private static final RowExpression C_DECIMAL_12_2 = variable("x18", createDecimalType(12, 2));
    private static final RowExpression C_DECIMAL_6_1 = variable("x19", createDecimalType(6, 1));
    private static final RowExpression C_DECIMAL_3_0 = variable("x20", createDecimalType(3, 0));
    private static final RowExpression C_DECIMAL_2_0 = variable("x21", createDecimalType(2, 0));
    private static final RowExpression C_SMALLINT = variable("x22", SMALLINT);
    private static final RowExpression C_TINYINT = variable("x23", TINYINT);
    private static final RowExpression C_REAL = variable("x24", REAL);
    private static final RowExpression C_COLOR_1 = variable("x25", COLOR);
    private static final RowExpression C_ROW = variable("x26", RowType.from(ImmutableList.of(field("field1", BIGINT), field("field2", DOUBLE))));

    private static final long TIMESTAMP_VALUE = new DateTime(2013, 3, 30, 1, 5, 0, 0, DateTimeZone.UTC).getMillis();
    private static final long DATE_VALUE = TimeUnit.MILLISECONDS.toDays(new DateTime(2001, 1, 22, 0, 0, 0, 0, DateTimeZone.UTC).getMillis());
    private static final long COLOR_VALUE_1 = 1;
    private static final long COLOR_VALUE_2 = 2;

    private Metadata metadata;
    private RowExpressionDomainTranslator domainTranslator;
    private RowExpressionCanonicalizer canonicalizer;

    private RowExpression opBigint;
    private RowExpression opDouble;
    private RowExpression opVarchar;

    @BeforeClass
    public void setup()
    {
        metadata = createTestMetadataManager();
        domainTranslator = new RowExpressionDomainTranslator(metadata.getFunctionManager());
        opBigint = add(C_BIGINT, constant(1, BIGINT));
        opDouble = dereference(DOUBLE, C_ROW, constant(1, INTEGER));
        opVarchar = regex_extract(C_VARCHAR, constant(Slices.utf8Slice("test"), JoniRegexpType.JONI_REGEXP));
        canonicalizer = new RowExpressionCanonicalizer(metadata.getFunctionManager());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        metadata = null;
        domainTranslator = null;
    }

    @Test
    public void testNoneRoundTrip()
    {
        TupleDomain<RowExpression> tupleDomain = TupleDomain.none();
        DomainExtractor.ExtractionResult result = fromPredicate(toPredicate(tupleDomain));
        assertEquals(result.getRemainingExpression(), TRUE);
        assertEquals(result.getTupleDomain(), tupleDomain);
    }

    @Test
    public void testAllRoundTrip()
    {
        TupleDomain<RowExpression> tupleDomain = TupleDomain.all();
        DomainExtractor.ExtractionResult result = fromPredicate(toPredicate(tupleDomain));
        assertEquals(result.getRemainingExpression(), TRUE);
        assertEquals(result.getTupleDomain(), tupleDomain);
    }

    @Test
    public void testRoundTrip()
    {
        TupleDomain<RowExpression> tupleDomain = withColumnDomains(ImmutableMap.<RowExpression, Domain>builder()
                .put(opBigint, Domain.singleValue(BIGINT, 1L))
                .put(opDouble, Domain.onlyNull(DOUBLE))
                .put(opVarchar, Domain.notNull(VARCHAR))
                .put(C_BOOLEAN, Domain.singleValue(BOOLEAN, true))
                .put(C_BIGINT_1, Domain.singleValue(BIGINT, 2L))
                .put(C_DOUBLE_1, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(DOUBLE, 1.1), Range.equal(DOUBLE, 2.0), Range.range(DOUBLE, 3.0, false, 3.5, true)), true))
                .put(C_VARCHAR_1, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(VARCHAR, utf8Slice("2013-01-01")), Range.greaterThan(VARCHAR, utf8Slice("2013-10-01"))), false))
                .put(C_TIMESTAMP, Domain.singleValue(TIMESTAMP, TIMESTAMP_VALUE))
                .put(C_DATE, Domain.singleValue(DATE, DATE_VALUE))
                .put(C_COLOR, Domain.singleValue(COLOR, COLOR_VALUE_1))
                .put(C_HYPER_LOG_LOG, Domain.notNull(HYPER_LOG_LOG))
                .build());

        assertPredicateTranslates(toPredicate(tupleDomain), tupleDomain);
    }

    @Test
    public void testInOptimization()
    {
        Domain testDomain = Domain.create(
                ValueSet.all(BIGINT)
                        .subtract(ValueSet.ofRanges(
                                Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L))), false);

        TupleDomain<RowExpression> tupleDomain = withColumnDomains(ImmutableMap.<RowExpression, Domain>builder().put(opBigint, testDomain).build());
        assertEquals(toPredicate(tupleDomain), not(bigintIn(opBigint, ImmutableList.of(1L, 2L, 3L))));

        testDomain = Domain.create(
                ValueSet.ofRanges(
                        Range.lessThan(BIGINT, 4L)).intersect(
                        ValueSet.all(BIGINT)
                                .subtract(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L)))), false);

        tupleDomain = withColumnDomains(ImmutableMap.<RowExpression, Domain>builder().put(opBigint, testDomain).build());
        assertEquals(toPredicate(tupleDomain), and(lessThan(opBigint, bigintLiteral(4L)), not(bigintIn(opBigint, ImmutableList.of(1L, 2L, 3L)))));

        testDomain = Domain.create(ValueSet.ofRanges(
                Range.range(BIGINT, 1L, true, 3L, true),
                Range.range(BIGINT, 5L, true, 7L, true),
                Range.range(BIGINT, 9L, true, 11L, true)),
                false);

        tupleDomain = withColumnDomains(ImmutableMap.<RowExpression, Domain>builder().put(opBigint, testDomain).build());
        assertEquals(toPredicate(tupleDomain),
                or(between(opBigint, bigintLiteral(1L), bigintLiteral(3L)), (between(opBigint, bigintLiteral(5L), bigintLiteral(7L))), (between(opBigint, bigintLiteral(9L), bigintLiteral(11L)))));

        testDomain = Domain.create(
                ValueSet.ofRanges(
                        Range.lessThan(BIGINT, 4L))
                        .intersect(ValueSet.all(BIGINT)
                                .subtract(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L))))
                        .union(ValueSet.ofRanges(Range.range(BIGINT, 7L, true, 9L, true))), false);

        tupleDomain = withColumnDomains(ImmutableMap.<RowExpression, Domain>builder().put(opBigint, testDomain).build());
        assertEquals(toPredicate(tupleDomain), or(and(lessThan(opBigint, bigintLiteral(4L)), not(bigintIn(opBigint, ImmutableList.of(1L, 2L, 3L)))), between(opBigint, bigintLiteral(7L), bigintLiteral(9L))));

        testDomain = Domain.create(
                ValueSet.ofRanges(Range.lessThan(BIGINT, 4L))
                        .intersect(ValueSet.all(BIGINT)
                                .subtract(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L))))
                        .union(ValueSet.ofRanges(Range.range(BIGINT, 7L, false, 9L, false), Range.range(BIGINT, 11L, false, 13L, false))), false);

        tupleDomain = withColumnDomains(ImmutableMap.<RowExpression, Domain>builder().put(opBigint, testDomain).build());
        assertEquals(toPredicate(tupleDomain), or(
                and(lessThan(opBigint, bigintLiteral(4L)), not(bigintIn(opBigint, ImmutableList.of(1L, 2L, 3L)))),
                and(greaterThan(opBigint, bigintLiteral(7L)), lessThan(opBigint, bigintLiteral(9L))),
                and(greaterThan(opBigint, bigintLiteral(11L)), lessThan(opBigint, bigintLiteral(13L)))));
    }

    @Test
    public void testToPredicateNone()
    {
        TupleDomain<RowExpression> tupleDomain = withColumnDomains(ImmutableMap.<RowExpression, Domain>builder()
                .put(opBigint, Domain.singleValue(BIGINT, 1L))
                .put(opDouble, Domain.onlyNull(DOUBLE))
                .put(opVarchar, Domain.notNull(VARCHAR))
                .put(C_BOOLEAN, Domain.none(BOOLEAN))
                .build());

        assertEquals(toPredicate(tupleDomain), FALSE);
    }

    @Test
    public void testToPredicateAllIgnored()
    {
        TupleDomain<RowExpression> tupleDomain = withColumnDomains(ImmutableMap.<RowExpression, Domain>builder()
                .put(opBigint, Domain.singleValue(BIGINT, 1L))
                .put(C_DOUBLE, Domain.onlyNull(DOUBLE))
                .put(opVarchar, Domain.notNull(VARCHAR))
                .put(C_BOOLEAN, Domain.all(BOOLEAN))
                .build());

        DomainExtractor.ExtractionResult result = fromPredicate(toPredicate(tupleDomain));
        assertEquals(result.getRemainingExpression(), TRUE);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<RowExpression, Domain>builder()
                .put(opBigint, Domain.singleValue(BIGINT, 1L))
                .put(C_DOUBLE, Domain.onlyNull(DOUBLE))
                .put(opVarchar, Domain.notNull(VARCHAR))
                .build()));
    }

    @Test
    public void testToPredicate()
    {
        TupleDomain<RowExpression> tupleDomain;

        tupleDomain = withColumnDomains(ImmutableMap.of(opBigint, Domain.notNull(BIGINT)));
        assertEquals(toPredicate(tupleDomain), isNotNull(opBigint));

        tupleDomain = withColumnDomains(ImmutableMap.of(opBigint, Domain.onlyNull(BIGINT)));
        assertEquals(toPredicate(tupleDomain), isNull(opBigint));

        tupleDomain = withColumnDomains(ImmutableMap.of(opBigint, Domain.none(BIGINT)));
        assertEquals(toPredicate(tupleDomain), FALSE);

        tupleDomain = withColumnDomains(ImmutableMap.of(opBigint, Domain.all(BIGINT)));
        assertEquals(toPredicate(tupleDomain), TRUE);

        tupleDomain = withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 1L)), false)));
        assertEquals(toPredicate(tupleDomain), greaterThan(opBigint, bigintLiteral(1L)));

        tupleDomain = withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 1L)), false)));
        assertEquals(toPredicate(tupleDomain), greaterThanOrEqual(opBigint, bigintLiteral(1L)));

        tupleDomain = withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L)), false)));
        assertEquals(toPredicate(tupleDomain), lessThan(opBigint, bigintLiteral(1L)));

        tupleDomain = withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 0L, false, 1L, true)), false)));
        assertEquals(toPredicate(tupleDomain), and(greaterThan(opBigint, bigintLiteral(0L)), lessThanOrEqual(opBigint, bigintLiteral(1L))));

        tupleDomain = withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 1L)), false)));
        assertEquals(toPredicate(tupleDomain), lessThanOrEqual(opBigint, bigintLiteral(1L)));

        tupleDomain = withColumnDomains(ImmutableMap.of(opBigint, Domain.singleValue(BIGINT, 1L)));
        assertEquals(toPredicate(tupleDomain), equal(opBigint, bigintLiteral(1L)));

        tupleDomain = withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false)));
        assertEquals(toPredicate(tupleDomain), bigintIn(opBigint, ImmutableList.of(1L, 2L)));

        tupleDomain = withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L)), true)));
        assertEquals(toPredicate(tupleDomain), or(lessThan(opBigint, bigintLiteral(1L)), isNull(opBigint)));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1), true)));
        assertEquals(toPredicate(tupleDomain), or(equal(C_COLOR, colorLiteral(COLOR_VALUE_1)), isNull(C_COLOR)));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1).complement(), true)));
        assertEquals(toPredicate(tupleDomain), or(not(equal(C_COLOR, colorLiteral(COLOR_VALUE_1))), isNull(C_COLOR)));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_HYPER_LOG_LOG, Domain.onlyNull(HYPER_LOG_LOG)));
        assertEquals(toPredicate(tupleDomain), isNull(C_HYPER_LOG_LOG));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_HYPER_LOG_LOG, Domain.notNull(HYPER_LOG_LOG)));
        assertEquals(toPredicate(tupleDomain), isNotNull(C_HYPER_LOG_LOG));
    }

    @Test
    public void testFromUnknownPredicate()
    {
        assertUnsupportedPredicate(unprocessableExpression1(opBigint));
        assertUnsupportedPredicate(not(unprocessableExpression1(opBigint)));
    }

    @Test
    public void testFromAndPredicate()
    {
        RowExpression originalPredicate = and(
                and(greaterThan(opBigint, bigintLiteral(1L)), unprocessableExpression1(opBigint)),
                and(lessThan(opBigint, bigintLiteral(5L)), unprocessableExpression2(opBigint)));
        DomainExtractor.ExtractionResult result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), and(unprocessableExpression1(opBigint), unprocessableExpression2(opBigint)));
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, false, 5L, false)), false))));

        // Test complements
        assertUnsupportedPredicate(not(and(
                and(greaterThan(opBigint, bigintLiteral(1L)), unprocessableExpression1(opBigint)),
                and(lessThan(opBigint, bigintLiteral(5L)), unprocessableExpression2(opBigint)))));

        originalPredicate = not(and(
                not(and(greaterThan(opBigint, bigintLiteral(1L)), unprocessableExpression1(opBigint))),
                not(and(lessThan(opBigint, bigintLiteral(5L)), unprocessableExpression2(opBigint)))));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(opBigint, Domain.notNull(BIGINT))));
    }

    @Test
    public void testFromOrPredicate()
    {
        RowExpression originalPredicate = or(
                and(greaterThan(opBigint, bigintLiteral(1L)), unprocessableExpression1(opBigint)),
                and(lessThan(opBigint, bigintLiteral(5L)), unprocessableExpression2(opBigint)));
        DomainExtractor.ExtractionResult result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(opBigint, Domain.notNull(BIGINT))));

        originalPredicate = or(
                and(equal(opBigint, bigintLiteral(1L)), unprocessableExpression1(opBigint)),
                and(equal(opBigint, bigintLiteral(2L)), unprocessableExpression2(opBigint)));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false))));

        // Same unprocessableExpression means that we can do more extraction
        // If both sides are operating on the same single symbol
        originalPredicate = or(
                and(equal(opBigint, bigintLiteral(1L)), unprocessableExpression1(opBigint)),
                and(equal(opBigint, bigintLiteral(2L)), unprocessableExpression1(opBigint)));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), unprocessableExpression1(opBigint));
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false))));

        // And not if they have different symbols
        assertUnsupportedPredicate(or(
                and(equal(opBigint, bigintLiteral(1L)), unprocessableExpression1(opBigint)),
                and(equal(opDouble, doubleLiteral(2.0)), unprocessableExpression1(opBigint))));

        // We can make another optimization if one side is the super set of the other side
        originalPredicate = or(
                and(greaterThan(opBigint, bigintLiteral(1L)), greaterThan(opDouble, doubleLiteral(1.0)), unprocessableExpression1(opBigint)),
                and(greaterThan(opBigint, bigintLiteral(2L)), greaterThan(opDouble, doubleLiteral(2.0)), unprocessableExpression1(opBigint)));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), unprocessableExpression1(opBigint));
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(
                opBigint, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 1L)), false),
                opDouble, Domain.create(ValueSet.ofRanges(Range.greaterThan(DOUBLE, 1.0)), false))));

        // We can't make those inferences if the unprocessableExpressions are non-deterministic
        originalPredicate = or(
                and(equal(opBigint, bigintLiteral(1L)), randPredicate(opBigint)),
                and(equal(opBigint, bigintLiteral(2L)), randPredicate(opBigint)));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false))));

        // Test complements
        originalPredicate = not(or(
                and(greaterThan(opBigint, bigintLiteral(1L)), unprocessableExpression1(opBigint)),
                and(lessThan(opBigint, bigintLiteral(5L)), unprocessableExpression2(opBigint))));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), and(
                not(and(greaterThan(opBigint, bigintLiteral(1L)), unprocessableExpression1(opBigint))),
                not(and(lessThan(opBigint, bigintLiteral(5L)), unprocessableExpression2(opBigint)))));
        assertTrue(result.getTupleDomain().isAll());

        originalPredicate = not(or(
                not(and(greaterThan(opBigint, bigintLiteral(1L)), unprocessableExpression1(opBigint))),
                not(and(lessThan(opBigint, bigintLiteral(5L)), unprocessableExpression2(opBigint)))));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), and(unprocessableExpression1(opBigint), unprocessableExpression2(opBigint)));
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, false, 5L, false)), false))));
    }

    @Test
    public void testFromNotPredicate()
    {
        assertUnsupportedPredicate(not(and(equal(opBigint, bigintLiteral(1L)), unprocessableExpression1(opBigint))));
        assertUnsupportedPredicate(not(unprocessableExpression1(opBigint)));

        assertPredicateIsAlwaysFalse(not(TRUE));

        assertPredicateTranslates(
                not(equal(opBigint, bigintLiteral(1L))),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L), Range.greaterThan(BIGINT, 1L)), false))));
    }

    @Test
    public void testFromUnprocessableComparison()
    {
        assertUnsupportedPredicate(greaterThan(unprocessableExpression1(opBigint), unprocessableExpression2(opBigint)));
        assertUnsupportedPredicate(not(greaterThan(unprocessableExpression1(opBigint), unprocessableExpression2(opBigint))));
    }

    @Test
    public void testFromBasicComparisons()
    {
        // Test out the extraction of all basic comparisons
        assertPredicateTranslates(
                greaterThan(opBigint, bigintLiteral(2L)),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        assertPredicateTranslates(
                greaterThanOrEqual(opBigint, bigintLiteral(2L)),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 2L)), false))));

        assertPredicateTranslates(
                lessThan(opBigint, bigintLiteral(2L)),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L)), false))));

        assertPredicateTranslates(
                lessThanOrEqual(opBigint, bigintLiteral(2L)),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        assertPredicateTranslates(
                equal(opBigint, bigintLiteral(2L)),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))));

        assertPredicateTranslates(
                notEqual(opBigint, bigintLiteral(2L)),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), false))));

        assertPredicateTranslates(
                isDistinctFrom(opBigint, bigintLiteral(2L)),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), true))));

        assertPredicateTranslates(
                equal(C_COLOR, colorLiteral(COLOR_VALUE_1)),
                withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1), false))));

        assertPredicateTranslates(
                in(C_COLOR, ImmutableList.of(colorLiteral(COLOR_VALUE_1), colorLiteral(COLOR_VALUE_2))),
                withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1, COLOR_VALUE_2), false))));

        assertPredicateTranslates(
                not(in(C_COLOR, ImmutableList.of(colorLiteral(COLOR_VALUE_1), colorLiteral(COLOR_VALUE_2)))),
                withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1, COLOR_VALUE_2).complement(), false))));

        assertPredicateTranslates(
                in(C_COLOR, ImmutableList.of(colorLiteral(COLOR_VALUE_1), colorLiteral(COLOR_VALUE_2), constantNull(COLOR))),
                withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1, COLOR_VALUE_2), true))));

        assertPredicateTranslates(
                not(in(C_COLOR, ImmutableList.of(colorLiteral(COLOR_VALUE_1), colorLiteral(COLOR_VALUE_2), constantNull(COLOR)))),
                withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1, COLOR_VALUE_2).complement(), true))));

        assertPredicateTranslates(
                isDistinctFrom(C_COLOR, colorLiteral(COLOR_VALUE_1)),
                withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1).complement(), true))));

        // Test complement
        assertPredicateTranslates(
                not(greaterThan(opBigint, bigintLiteral(2L))),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        assertPredicateTranslates(
                not(greaterThanOrEqual(opBigint, bigintLiteral(2L))),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L)), false))));

        assertPredicateTranslates(
                not(lessThan(opBigint, bigintLiteral(2L))),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 2L)), false))));

        assertPredicateTranslates(
                not(lessThanOrEqual(opBigint, bigintLiteral(2L))),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        assertPredicateTranslates(
                not(equal(opBigint, bigintLiteral(2L))),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), false))));

        assertPredicateTranslates(
                not(notEqual(opBigint, bigintLiteral(2L))),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))));

        assertPredicateTranslates(
                not(isDistinctFrom(opBigint, bigintLiteral(2L))),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))));

        assertPredicateTranslates(
                not(equal(C_COLOR, colorLiteral(COLOR_VALUE_1))),
                withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1).complement(), false))));

        assertPredicateTranslates(
                not(in(C_COLOR, ImmutableList.of(colorLiteral(COLOR_VALUE_1), colorLiteral(COLOR_VALUE_2)))),
                withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1, COLOR_VALUE_2).complement(), false))));

        assertPredicateTranslates(
                not(isDistinctFrom(C_COLOR, colorLiteral(COLOR_VALUE_1))),
                withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1), false))));
    }

    @Test
    public void testFromFlippedBasicComparisons()
    {
        // Test out the extraction of all basic comparisons where the reference literal ordering is flipped
        assertPredicateTranslates(
                greaterThan(bigintLiteral(2L), opBigint),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L)), false))));

        assertPredicateTranslates(
                greaterThanOrEqual(bigintLiteral(2L), opBigint),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        assertPredicateTranslates(
                lessThan(bigintLiteral(2L), opBigint),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        assertPredicateTranslates(
                lessThanOrEqual(bigintLiteral(2L), opBigint),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 2L)), false))));

        assertPredicateTranslates(equal(bigintLiteral(2L), opBigint),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))));

        assertPredicateTranslates(equal(colorLiteral(COLOR_VALUE_1), C_COLOR),
                withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1), false))));

        assertPredicateTranslates(notEqual(bigintLiteral(2L), opBigint),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), false))));

        assertPredicateTranslates(
                notEqual(colorLiteral(COLOR_VALUE_1), C_COLOR),
                withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1).complement(), false))));

        assertPredicateTranslates(isDistinctFrom(bigintLiteral(2L), opBigint),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), true))));

        assertPredicateTranslates(
                isDistinctFrom(colorLiteral(COLOR_VALUE_1), C_COLOR),
                withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1).complement(), true))));

        assertPredicateTranslates(
                isDistinctFrom(nullLiteral(BIGINT), opBigint),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.notNull(BIGINT))));
    }

    @Test
    public void testFromBasicComparisonsWithNulls()
    {
        // Test out the extraction of all basic comparisons with null literals
        assertPredicateIsAlwaysFalse(greaterThan(opBigint, nullLiteral(BIGINT)));

        assertPredicateTranslates(
                greaterThan(opVarchar, nullLiteral(VARCHAR)),
                withColumnDomains(ImmutableMap.of(opVarchar, Domain.create(ValueSet.none(VARCHAR), false))));

        assertPredicateIsAlwaysFalse(greaterThanOrEqual(opBigint, nullLiteral(BIGINT)));
        assertPredicateIsAlwaysFalse(lessThan(opBigint, nullLiteral(BIGINT)));
        assertPredicateIsAlwaysFalse(lessThanOrEqual(opBigint, nullLiteral(BIGINT)));
        assertPredicateIsAlwaysFalse(equal(opBigint, nullLiteral(BIGINT)));
        assertPredicateIsAlwaysFalse(equal(C_COLOR, nullLiteral(COLOR)));
        assertPredicateIsAlwaysFalse(notEqual(opBigint, nullLiteral(BIGINT)));
        assertPredicateIsAlwaysFalse(notEqual(C_COLOR, nullLiteral(COLOR)));

        assertPredicateTranslates(
                isDistinctFrom(opBigint, nullLiteral(BIGINT)),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.notNull(BIGINT))));

        assertPredicateTranslates(
                isDistinctFrom(C_COLOR, nullLiteral(COLOR)),
                withColumnDomains(ImmutableMap.of(C_COLOR, Domain.notNull(COLOR))));

        // Test complements
        assertPredicateIsAlwaysFalse(not(greaterThan(opBigint, nullLiteral(BIGINT))));
        assertPredicateIsAlwaysFalse(not(greaterThanOrEqual(opBigint, nullLiteral(BIGINT))));
        assertPredicateIsAlwaysFalse(not(lessThan(opBigint, nullLiteral(BIGINT))));
        assertPredicateIsAlwaysFalse(not(lessThanOrEqual(opBigint, nullLiteral(BIGINT))));
        assertPredicateIsAlwaysFalse(not(equal(opBigint, nullLiteral(BIGINT))));
        assertPredicateIsAlwaysFalse(not(equal(C_COLOR, nullLiteral(COLOR))));
        assertPredicateIsAlwaysFalse(not(notEqual(opBigint, nullLiteral(BIGINT))));
        assertPredicateIsAlwaysFalse(not(notEqual(C_COLOR, nullLiteral(COLOR))));

        assertPredicateTranslates(
                not(isDistinctFrom(opBigint, nullLiteral(BIGINT))),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.onlyNull(BIGINT))));

        assertPredicateTranslates(
                not(isDistinctFrom(C_COLOR, nullLiteral(COLOR))),
                withColumnDomains(ImmutableMap.of(C_COLOR, Domain.onlyNull(COLOR))));
    }

    @Test
    void testNonImplictCastOnSymbolSide()
    {
        // we expect TupleDomain.all here().
        // see comment in ExpressionDomainTranslator.Visitor.visitComparisonExpression()
        assertUnsupportedPredicate(equal(
                cast(C_TIMESTAMP, DATE),
                toRowExpression(DATE_VALUE, DATE)));
        assertUnsupportedPredicate(equal(
                cast(C_DECIMAL_12_2, BIGINT),
                bigintLiteral(135L)));
    }

    @Test
    void testNoSaturatedFloorCastFromUnsupportedApproximateDomain()
    {
        assertUnsupportedPredicate(equal(
                cast(C_DECIMAL_12_2, DOUBLE),
                doubleLiteral(12345.56)));

        assertUnsupportedPredicate(equal(
                cast(opBigint, DOUBLE),
                doubleLiteral(12345.56)));

        assertUnsupportedPredicate(equal(
                cast(opBigint, REAL),
                realLiteral(12345.56f)));

        assertUnsupportedPredicate(equal(
                cast(C_INTEGER, REAL),
                realLiteral(12345.56f)));
    }

    @Test
    public void testFromComparisonsWithCoercions()
    {
        // B is a double column. Check that it can be compared against longs
        assertPredicateTranslates(
                greaterThan(opDouble, cast(bigintLiteral(2L), DOUBLE)),
                withColumnDomains(ImmutableMap.of(opDouble, Domain.create(ValueSet.ofRanges(Range.greaterThan(DOUBLE, 2.0)), false))));

        // C is a string column. Check that it can be compared.
        assertPredicateTranslates(
                greaterThan(opVarchar, stringLiteral("test")),
                withColumnDomains(ImmutableMap.of(opVarchar, Domain.create(ValueSet.ofRanges(Range.greaterThan(VARCHAR, utf8Slice("test"))), false))));

        // A is a integer column. Check that it can be compared against doubles
        assertPredicateTranslates(
                greaterThan(cast(C_INTEGER, DOUBLE), doubleLiteral(2.0)),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 2L)), false))));

        assertPredicateTranslates(
                greaterThan(cast(C_INTEGER, DOUBLE), doubleLiteral(2.1)),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 2L)), false))));

        assertPredicateTranslates(
                greaterThanOrEqual(cast(C_INTEGER, DOUBLE), doubleLiteral(2.0)),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(INTEGER, 2L)), false))));

        assertPredicateTranslates(
                greaterThanOrEqual(cast(C_INTEGER, DOUBLE), doubleLiteral(2.1)),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 2L)), false))));

        assertPredicateTranslates(
                lessThan(cast(C_INTEGER, DOUBLE), doubleLiteral(2.0)),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.create(ValueSet.ofRanges(Range.lessThan(INTEGER, 2L)), false))));

        assertPredicateTranslates(
                lessThan(cast(C_INTEGER, DOUBLE), doubleLiteral(2.1)),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(INTEGER, 2L)), false))));

        assertPredicateTranslates(
                lessThanOrEqual(cast(C_INTEGER, DOUBLE), doubleLiteral(2.0)),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(INTEGER, 2L)), false))));

        assertPredicateTranslates(
                lessThanOrEqual(cast(C_INTEGER, DOUBLE), doubleLiteral(2.1)),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(INTEGER, 2L)), false))));

        assertPredicateTranslates(
                equal(cast(C_INTEGER, DOUBLE), doubleLiteral(2.0)),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.create(ValueSet.ofRanges(Range.equal(INTEGER, 2L)), false))));

        assertPredicateTranslates(
                equal(cast(C_INTEGER, DOUBLE), doubleLiteral(2.1)),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.none(INTEGER))));

        assertPredicateTranslates(
                notEqual(cast(C_INTEGER, DOUBLE), doubleLiteral(2.0)),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.create(ValueSet.ofRanges(Range.lessThan(INTEGER, 2L), Range.greaterThan(INTEGER, 2L)), false))));

        assertPredicateTranslates(
                notEqual(cast(C_INTEGER, DOUBLE), doubleLiteral(2.1)),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.notNull(INTEGER))));

        assertPredicateTranslates(
                isDistinctFrom(cast(C_INTEGER, DOUBLE), doubleLiteral(2.0)),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.create(ValueSet.ofRanges(Range.lessThan(INTEGER, 2L), Range.greaterThan(INTEGER, 2L)), true))));

        assertPredicateIsAlwaysTrue(isDistinctFrom(cast(C_INTEGER, DOUBLE), doubleLiteral(2.1)));

        // Test complements

        // B is a double column. Check that it can be compared against longs
        assertPredicateTranslates(
                not(greaterThan(opDouble, cast(bigintLiteral(2L), DOUBLE))),
                withColumnDomains(ImmutableMap.of(opDouble, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(DOUBLE, 2.0)), false))));

        // C is a string column. Check that it can be compared.
        assertPredicateTranslates(
                not(greaterThan(opVarchar, stringLiteral("test"))),
                withColumnDomains(ImmutableMap.of(opVarchar, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(VARCHAR, utf8Slice("test"))), false))));

        // A is a integer column. Check that it can be compared against doubles
        assertPredicateTranslates(
                not(greaterThan(cast(C_INTEGER, DOUBLE), doubleLiteral(2.0))),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(INTEGER, 2L)), false))));

        assertPredicateTranslates(
                not(greaterThan(cast(C_INTEGER, DOUBLE), doubleLiteral(2.1))),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(INTEGER, 2L)), false))));

        assertPredicateTranslates(
                not(greaterThanOrEqual(cast(C_INTEGER, DOUBLE), doubleLiteral(2.0))),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.create(ValueSet.ofRanges(Range.lessThan(INTEGER, 2L)), false))));

        assertPredicateTranslates(
                not(greaterThanOrEqual(cast(C_INTEGER, DOUBLE), doubleLiteral(2.1))),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(INTEGER, 2L)), false))));

        assertPredicateTranslates(
                not(lessThan(cast(C_INTEGER, DOUBLE), doubleLiteral(2.0))),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(INTEGER, 2L)), false))));

        assertPredicateTranslates(
                not(lessThan(cast(C_INTEGER, DOUBLE), doubleLiteral(2.1))),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 2L)), false))));

        assertPredicateTranslates(
                not(lessThanOrEqual(cast(C_INTEGER, DOUBLE), doubleLiteral(2.0))),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 2L)), false))));

        assertPredicateTranslates(
                not(lessThanOrEqual(cast(C_INTEGER, DOUBLE), doubleLiteral(2.1))),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 2L)), false))));

        assertPredicateTranslates(
                not(equal(cast(C_INTEGER, DOUBLE), doubleLiteral(2.0))),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.create(ValueSet.ofRanges(Range.lessThan(INTEGER, 2L), Range.greaterThan(INTEGER, 2L)), false))));

        assertPredicateTranslates(
                not(equal(cast(C_INTEGER, DOUBLE), doubleLiteral(2.1))),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.notNull(INTEGER))));

        assertPredicateTranslates(
                not(notEqual(cast(C_INTEGER, DOUBLE), doubleLiteral(2.0))),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.create(ValueSet.ofRanges(Range.equal(INTEGER, 2L)), false))));

        assertPredicateTranslates(
                not(notEqual(cast(C_INTEGER, DOUBLE), doubleLiteral(2.1))),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.none(INTEGER))));

        assertPredicateTranslates(
                not(isDistinctFrom(cast(C_INTEGER, DOUBLE), doubleLiteral(2.0))),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.create(ValueSet.ofRanges(Range.equal(INTEGER, 2L)), false))));

        assertPredicateIsAlwaysFalse(not(isDistinctFrom(cast(C_INTEGER, DOUBLE), doubleLiteral(2.1))));
    }

    @Test
    public void testFromUnprocessableInPredicate()
    {
        // following is supported since it is always true
        // assertUnsupportedPredicate(in(unprocessableExpression1(opBigint), ImmutableList.of(TRUE)));
        assertUnsupportedPredicate(in(C_BOOLEAN, ImmutableList.of(unprocessableExpression1(C_BOOLEAN))));
        assertUnsupportedPredicate(
                in(C_BOOLEAN, ImmutableList.of(TRUE, unprocessableExpression1(C_BOOLEAN))));
        assertUnsupportedPredicate(not(in(C_BOOLEAN, ImmutableList.of(unprocessableExpression1(C_BOOLEAN)))));
    }

    @Test
    public void testFromInPredicate()
    {
        assertPredicateTranslates(
                bigintIn(opBigint, ImmutableList.of(1L)),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.singleValue(BIGINT, 1L))));

        assertPredicateTranslates(
                in(C_COLOR, ImmutableList.of(colorLiteral(COLOR_VALUE_1))),
                withColumnDomains(ImmutableMap.of(C_COLOR, Domain.singleValue(COLOR, COLOR_VALUE_1))));

        assertPredicateTranslates(
                bigintIn(opBigint, ImmutableList.of(1L, 2L)),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false))));

        assertPredicateTranslates(
                in(C_COLOR, ImmutableList.of(colorLiteral(COLOR_VALUE_1), colorLiteral(COLOR_VALUE_2))),
                withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1, COLOR_VALUE_2), false))));

        assertPredicateTranslates(
                not(bigintIn(opBigint, ImmutableList.of(1L, 2L))),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L), Range.range(BIGINT, 1L, false, 2L, false), Range.greaterThan(BIGINT, 2L)), false))));

        assertPredicateTranslates(
                not(in(C_COLOR, ImmutableList.of(colorLiteral(COLOR_VALUE_1), colorLiteral(COLOR_VALUE_2)))),
                withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1, COLOR_VALUE_2).complement(), false))));

        assertPredicateTranslates(
                not(in(C_COLOR, ImmutableList.of(colorLiteral(COLOR_VALUE_1), C_COLOR_1, colorLiteral(COLOR_VALUE_2)))),
                withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1, COLOR_VALUE_2).complement(), false))),
                not(equal(C_COLOR, C_COLOR_1)));

        assertPredicateTranslates(
                not(in(C_COLOR, ImmutableList.of(colorLiteral(COLOR_VALUE_1), C_COLOR_1, colorLiteral(COLOR_VALUE_2)))),
                withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1, COLOR_VALUE_2).complement(), false))),
                not(equal(C_COLOR, C_COLOR_1)));
    }

    @Test
    public void testFromBetweenPredicate()
    {
        assertPredicateTranslates(
                between(opBigint, bigintLiteral(1L), bigintLiteral(2L)),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 2L, true)), false))));

        assertPredicateTranslates(
                between(cast(C_INTEGER, DOUBLE), cast(bigintLiteral(1L), DOUBLE), doubleLiteral(2.1)),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.create(ValueSet.ofRanges(Range.range(INTEGER, 1L, true, 2L, true)), false))));

        assertPredicateIsAlwaysFalse(between(opBigint, bigintLiteral(1L), nullLiteral(BIGINT)));

        // Test complements
        assertPredicateTranslates(
                not(between(opBigint, bigintLiteral(1L), bigintLiteral(2L))),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L), Range.greaterThan(BIGINT, 2L)), false))));

        assertPredicateTranslates(
                not(between(cast(C_INTEGER, DOUBLE), cast(bigintLiteral(1L), DOUBLE), doubleLiteral(2.1))),
                withColumnDomains(ImmutableMap.of(C_INTEGER, Domain.create(ValueSet.ofRanges(Range.lessThan(INTEGER, 1L), Range.greaterThan(INTEGER, 2L)), false))));

        assertPredicateTranslates(
                not(between(opBigint, bigintLiteral(1L), nullLiteral(BIGINT))),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L)), false))));
    }

    @Test
    public void testFromIsNullPredicate()
    {
        assertPredicateTranslates(
                isNull(opBigint),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.onlyNull(BIGINT))));

        assertPredicateTranslates(
                isNull(C_HYPER_LOG_LOG),
                withColumnDomains(ImmutableMap.of(C_HYPER_LOG_LOG, Domain.onlyNull(HYPER_LOG_LOG))));

        assertPredicateTranslates(
                not(isNull(opBigint)),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.notNull(BIGINT))));

        assertPredicateTranslates(
                not(isNull(C_HYPER_LOG_LOG)),
                withColumnDomains(ImmutableMap.of(C_HYPER_LOG_LOG, Domain.notNull(HYPER_LOG_LOG))));
    }

    @Test
    public void testFromIsNotNullPredicate()
    {
        assertPredicateTranslates(
                isNotNull(opBigint),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.notNull(BIGINT))));

        assertPredicateTranslates(
                isNotNull(C_HYPER_LOG_LOG),
                withColumnDomains(ImmutableMap.of(C_HYPER_LOG_LOG, Domain.notNull(HYPER_LOG_LOG))));

        assertPredicateTranslates(
                not(isNotNull(opBigint)),
                withColumnDomains(ImmutableMap.of(opBigint, Domain.onlyNull(BIGINT))));

        assertPredicateTranslates(
                not(isNotNull(C_HYPER_LOG_LOG)),
                withColumnDomains(ImmutableMap.of(C_HYPER_LOG_LOG, Domain.onlyNull(HYPER_LOG_LOG))));
    }

    @Test
    public void testFromBooleanLiteralPredicate()
    {
        assertPredicateIsAlwaysTrue(TRUE);
        assertPredicateIsAlwaysFalse(not(TRUE));
        assertPredicateIsAlwaysFalse(FALSE);
        assertPredicateIsAlwaysTrue(not(FALSE));
    }

    @Test
    public void testFromNullLiteralPredicate()
    {
        assertPredicateIsAlwaysFalse(nullLiteral(UNKNOWN));
        assertPredicateIsAlwaysFalse(not(nullLiteral(UNKNOWN)));
    }

    @Test
    public void testExpressionConstantFolding()
    {
        FunctionHandle hex = metadata.getFunctionManager().lookupFunction(QualifiedName.of("from_hex"), fromTypes(VARCHAR));
        RowExpression originalExpression = greaterThan(C_VARBINARY, call("from_hex", hex, VARBINARY, stringLiteral("123456")));
        DomainExtractor.ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE);
        Slice value = Slices.wrappedBuffer(BaseEncoding.base16().decode("123456"));
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_VARBINARY, Domain.create(ValueSet.ofRanges(Range.greaterThan(VARBINARY, value)), false))));

        RowExpression expression = toPredicate(result.getTupleDomain());
        assertEquals(expression, greaterThan(C_VARBINARY, varbinaryLiteral(value)));
    }

    @Test
    public void testConjunctExpression()
    {
        RowExpression expression = and(
                greaterThan(opDouble, doubleLiteral(0)),
                greaterThan(opBigint, bigintLiteral(0)));
        assertPredicateTranslates(
                expression,
                withColumnDomains(ImmutableMap.of(
                        opBigint, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 0L)), false),
                        opDouble, Domain.create(ValueSet.ofRanges(Range.greaterThan(DOUBLE, .0)), false))));

        assertEquals(
                toPredicate(fromPredicate(expression).getTupleDomain()),
                and(
                        greaterThan(opBigint, bigintLiteral(0)),
                        greaterThan(opDouble, doubleLiteral(0))));
    }

    @Test
    void testMultipleCoercionsOnSymbolSide()
    {
        assertPredicateTranslates(
                greaterThan(cast(cast(C_SMALLINT, REAL), DOUBLE), doubleLiteral(3.7)),
                withColumnDomains(ImmutableMap.of(C_SMALLINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(SMALLINT, 3L)), false))));
    }

    @Test
    public void testNumericTypeTranslation()
    {
        testNumericTypeTranslationChain(
                new NumericValues<>(C_DECIMAL_26_5, longDecimal("-999999999999999999999.99999"), longDecimal("-22.00000"), longDecimal("-44.55569"), longDecimal("23.00000"), longDecimal("44.55567"), longDecimal("999999999999999999999.99999")),
                new NumericValues<>(C_DECIMAL_23_4, longDecimal("-9999999999999999999.9999"), longDecimal("-22.0000"), longDecimal("-44.5557"), longDecimal("23.0000"), longDecimal("44.5556"), longDecimal("9999999999999999999.9999")),
                new NumericValues<>(opBigint, Long.MIN_VALUE, -22L, -45L, 23L, 44L, Long.MAX_VALUE),
                new NumericValues<>(C_DECIMAL_21_3, longDecimal("-999999999999999999.999"), longDecimal("-22.000"), longDecimal("-44.556"), longDecimal("23.000"), longDecimal("44.555"), longDecimal("999999999999999999.999")),
                new NumericValues<>(C_DECIMAL_12_2, shortDecimal("-9999999999.99"), shortDecimal("-22.00"), shortDecimal("-44.56"), shortDecimal("23.00"), shortDecimal("44.55"), shortDecimal("9999999999.99")),
                new NumericValues<>(C_INTEGER, (long) Integer.MIN_VALUE, -22L, -45L, 23L, 44L, (long) Integer.MAX_VALUE),
                new NumericValues<>(C_DECIMAL_6_1, shortDecimal("-99999.9"), shortDecimal("-22.0"), shortDecimal("-44.6"), shortDecimal("23.0"), shortDecimal("44.5"), shortDecimal("99999.9")),
                new NumericValues<>(C_SMALLINT, (long) Short.MIN_VALUE, -22L, -45L, 23L, 44L, (long) Short.MAX_VALUE),
                new NumericValues<>(C_DECIMAL_3_0, shortDecimal("-999"), shortDecimal("-22"), shortDecimal("-45"), shortDecimal("23"), shortDecimal("44"), shortDecimal("999")),
                new NumericValues<>(C_TINYINT, (long) Byte.MIN_VALUE, -22L, -45L, 23L, 44L, (long) Byte.MAX_VALUE),
                new NumericValues<>(C_DECIMAL_2_0, shortDecimal("-99"), shortDecimal("-22"), shortDecimal("-45"), shortDecimal("23"), shortDecimal("44"), shortDecimal("99")));

        testNumericTypeTranslationChain(
                new NumericValues<>(opDouble, -1.0 * Double.MAX_VALUE, -22.0, -44.5556836, 23.0, 44.5556789, Double.MAX_VALUE),
                new NumericValues<>(C_REAL, realValue(-1.0f * Float.MAX_VALUE), realValue(-22.0f), realValue(-44.555687f), realValue(23.0f), realValue(44.555676f), realValue(Float.MAX_VALUE)));
    }

    private void testNumericTypeTranslationChain(NumericValues... translationChain)
    {
        for (int literalIndex = 0; literalIndex < translationChain.length; literalIndex++) {
            for (int columnIndex = literalIndex + 1; columnIndex < translationChain.length; columnIndex++) {
                NumericValues literal = translationChain[literalIndex];
                NumericValues column = translationChain[columnIndex];
                testNumericTypeTranslation(column, literal);
            }
        }
    }

    private void testNumericTypeTranslation(NumericValues columnValues, NumericValues literalValues)
    {
        Type columnType = columnValues.getInput().getType();
        Type literalType = literalValues.getInput().getType();
        Type superType = metadata.getTypeManager().getCommonSuperType(columnType, literalType).orElseThrow(() -> new IllegalArgumentException("incompatible types in test (" + columnType + ", " + literalType + ")"));

        RowExpression max = toRowExpression(literalValues.getMax(), literalType);
        RowExpression min = toRowExpression(literalValues.getMin(), literalType);
        RowExpression integerPositive = toRowExpression(literalValues.getIntegerPositive(), literalType);
        RowExpression integerNegative = toRowExpression(literalValues.getIntegerNegative(), literalType);
        RowExpression fractionalPositive = toRowExpression(literalValues.getFractionalPositive(), literalType);
        RowExpression fractionalNegative = toRowExpression(literalValues.getFractionalNegative(), literalType);

        if (!literalType.equals(superType)) {
            max = cast(max, superType);
            min = cast(min, superType);
            integerPositive = cast(integerPositive, superType);
            integerNegative = cast(integerNegative, superType);
            fractionalPositive = cast(fractionalPositive, superType);
            fractionalNegative = cast(fractionalNegative, superType);
        }

        RowExpression columnSymbol = columnValues.getInput();
        RowExpression columnExpression = columnSymbol;

        if (!columnType.equals(superType)) {
            columnExpression = cast(columnExpression, superType);
        }

        // greater than or equal
        testSimpleComparison(greaterThanOrEqual(columnExpression, integerPositive), columnSymbol, Range.greaterThanOrEqual(columnType, columnValues.getIntegerPositive()));
        testSimpleComparison(greaterThanOrEqual(columnExpression, integerNegative), columnSymbol, Range.greaterThanOrEqual(columnType, columnValues.getIntegerNegative()));
        testSimpleComparison(greaterThanOrEqual(columnExpression, max), columnSymbol, Range.greaterThan(columnType, columnValues.getMax()));
        testSimpleComparison(greaterThanOrEqual(columnExpression, min), columnSymbol, Range.greaterThanOrEqual(columnType, columnValues.getMin()));
        if (literalValues.isFractional()) {
            testSimpleComparison(greaterThanOrEqual(columnExpression, fractionalPositive), columnSymbol, Range.greaterThan(columnType, columnValues.getFractionalPositive()));
            testSimpleComparison(greaterThanOrEqual(columnExpression, fractionalNegative), columnSymbol, Range.greaterThan(columnType, columnValues.getFractionalNegative()));
        }

        // greater than
        testSimpleComparison(greaterThan(columnExpression, integerPositive), columnSymbol, Range.greaterThan(columnType, columnValues.getIntegerPositive()));
        testSimpleComparison(greaterThan(columnExpression, integerNegative), columnSymbol, Range.greaterThan(columnType, columnValues.getIntegerNegative()));
        testSimpleComparison(greaterThan(columnExpression, max), columnSymbol, Range.greaterThan(columnType, columnValues.getMax()));
        testSimpleComparison(greaterThan(columnExpression, min), columnSymbol, Range.greaterThanOrEqual(columnType, columnValues.getMin()));
        if (literalValues.isFractional()) {
            testSimpleComparison(greaterThan(columnExpression, fractionalPositive), columnSymbol, Range.greaterThan(columnType, columnValues.getFractionalPositive()));
            testSimpleComparison(greaterThan(columnExpression, fractionalNegative), columnSymbol, Range.greaterThan(columnType, columnValues.getFractionalNegative()));
        }

        // less than or equal
        testSimpleComparison(lessThanOrEqual(columnExpression, integerPositive), columnSymbol, Range.lessThanOrEqual(columnType, columnValues.getIntegerPositive()));
        testSimpleComparison(lessThanOrEqual(columnExpression, integerNegative), columnSymbol, Range.lessThanOrEqual(columnType, columnValues.getIntegerNegative()));
        testSimpleComparison(lessThanOrEqual(columnExpression, max), columnSymbol, Range.lessThanOrEqual(columnType, columnValues.getMax()));
        testSimpleComparison(lessThanOrEqual(columnExpression, min), columnSymbol, Range.lessThan(columnType, columnValues.getMin()));
        if (literalValues.isFractional()) {
            testSimpleComparison(lessThanOrEqual(columnExpression, fractionalPositive), columnSymbol, Range.lessThanOrEqual(columnType, columnValues.getFractionalPositive()));
            testSimpleComparison(lessThanOrEqual(columnExpression, fractionalNegative), columnSymbol, Range.lessThanOrEqual(columnType, columnValues.getFractionalNegative()));
        }

        // less than
        testSimpleComparison(lessThan(columnExpression, integerPositive), columnSymbol, Range.lessThan(columnType, columnValues.getIntegerPositive()));
        testSimpleComparison(lessThan(columnExpression, integerNegative), columnSymbol, Range.lessThan(columnType, columnValues.getIntegerNegative()));
        testSimpleComparison(lessThan(columnExpression, max), columnSymbol, Range.lessThanOrEqual(columnType, columnValues.getMax()));
        testSimpleComparison(lessThan(columnExpression, min), columnSymbol, Range.lessThan(columnType, columnValues.getMin()));
        if (literalValues.isFractional()) {
            testSimpleComparison(lessThan(columnExpression, fractionalPositive), columnSymbol, Range.lessThanOrEqual(columnType, columnValues.getFractionalPositive()));
            testSimpleComparison(lessThan(columnExpression, fractionalNegative), columnSymbol, Range.lessThanOrEqual(columnType, columnValues.getFractionalNegative()));
        }

        // equal
        testSimpleComparison(equal(columnExpression, integerPositive), columnSymbol, Range.equal(columnType, columnValues.getIntegerPositive()));
        testSimpleComparison(equal(columnExpression, integerNegative), columnSymbol, Range.equal(columnType, columnValues.getIntegerNegative()));
        testSimpleComparison(equal(columnExpression, max), columnSymbol, Domain.none(columnType));
        testSimpleComparison(equal(columnExpression, min), columnSymbol, Domain.none(columnType));
        if (literalValues.isFractional()) {
            testSimpleComparison(equal(columnExpression, fractionalPositive), columnSymbol, Domain.none(columnType));
            testSimpleComparison(equal(columnExpression, fractionalNegative), columnSymbol, Domain.none(columnType));
        }

        // not equal
        testSimpleComparison(notEqual(columnExpression, integerPositive), columnSymbol, Domain.create(ValueSet.ofRanges(Range.lessThan(columnType, columnValues.getIntegerPositive()), Range.greaterThan(columnType, columnValues.getIntegerPositive())), false));
        testSimpleComparison(notEqual(columnExpression, integerNegative), columnSymbol, Domain.create(ValueSet.ofRanges(Range.lessThan(columnType, columnValues.getIntegerNegative()), Range.greaterThan(columnType, columnValues.getIntegerNegative())), false));
        testSimpleComparison(notEqual(columnExpression, max), columnSymbol, Domain.notNull(columnType));
        testSimpleComparison(notEqual(columnExpression, min), columnSymbol, Domain.notNull(columnType));
        if (literalValues.isFractional()) {
            testSimpleComparison(notEqual(columnExpression, fractionalPositive), columnSymbol, Domain.notNull(columnType));
            testSimpleComparison(notEqual(columnExpression, fractionalNegative), columnSymbol, Domain.notNull(columnType));
        }

        // is distinct from
        testSimpleComparison(isDistinctFrom(columnExpression, integerPositive), columnSymbol, Domain.create(ValueSet.ofRanges(Range.lessThan(columnType, columnValues.getIntegerPositive()), Range.greaterThan(columnType, columnValues.getIntegerPositive())), true));
        testSimpleComparison(isDistinctFrom(columnExpression, integerNegative), columnSymbol, Domain.create(ValueSet.ofRanges(Range.lessThan(columnType, columnValues.getIntegerNegative()), Range.greaterThan(columnType, columnValues.getIntegerNegative())), true));
        testSimpleComparison(isDistinctFrom(columnExpression, max), columnSymbol, Domain.all(columnType));
        testSimpleComparison(isDistinctFrom(columnExpression, min), columnSymbol, Domain.all(columnType));
        if (literalValues.isFractional()) {
            testSimpleComparison(isDistinctFrom(columnExpression, fractionalPositive), columnSymbol, Domain.all(columnType));
            testSimpleComparison(isDistinctFrom(columnExpression, fractionalNegative), columnSymbol, Domain.all(columnType));
        }
    }

    @Test
    public void testLegacyCharComparedToVarcharExpression()
    {
        metadata = createTestMetadataManager(new FeaturesConfig().setLegacyCharToVarcharCoercion(true));

        String maxCodePoint = new String(Character.toChars(Character.MAX_CODE_POINT));

        // greater than or equal
        testSimpleComparison(greaterThanOrEqual(cast(C_CHAR, VARCHAR), stringLiteral("123456789")), C_CHAR, Range.greaterThan(createCharType(10), utf8Slice("123456788" + maxCodePoint)));
        testSimpleComparison(greaterThanOrEqual(cast(C_CHAR, VARCHAR), stringLiteral("1234567890")), C_CHAR, Range.greaterThanOrEqual(createCharType(10), Slices.utf8Slice("1234567890")));
        testSimpleComparison(greaterThanOrEqual(cast(C_CHAR, VARCHAR), stringLiteral("12345678901")), C_CHAR, Range.greaterThan(createCharType(10), Slices.utf8Slice("1234567890")));

        // greater than
        testSimpleComparison(greaterThan(cast(C_CHAR, VARCHAR), stringLiteral("123456789")), C_CHAR, Range.greaterThan(createCharType(10), utf8Slice("123456788" + maxCodePoint)));
        testSimpleComparison(greaterThan(cast(C_CHAR, VARCHAR), stringLiteral("1234567890")), C_CHAR, Range.greaterThan(createCharType(10), Slices.utf8Slice("1234567890")));
        testSimpleComparison(greaterThan(cast(C_CHAR, VARCHAR), stringLiteral("12345678901")), C_CHAR, Range.greaterThan(createCharType(10), Slices.utf8Slice("1234567890")));

        // less than or equal
        testSimpleComparison(lessThanOrEqual(cast(C_CHAR, VARCHAR), stringLiteral("123456789")), C_CHAR, Range.lessThanOrEqual(createCharType(10), utf8Slice("123456788" + maxCodePoint)));
        testSimpleComparison(lessThanOrEqual(cast(C_CHAR, VARCHAR), stringLiteral("1234567890")), C_CHAR, Range.lessThanOrEqual(createCharType(10), Slices.utf8Slice("1234567890")));
        testSimpleComparison(lessThanOrEqual(cast(C_CHAR, VARCHAR), stringLiteral("12345678901")), C_CHAR, Range.lessThanOrEqual(createCharType(10), Slices.utf8Slice("1234567890")));

        // less than
        testSimpleComparison(lessThan(cast(C_CHAR, VARCHAR), stringLiteral("123456789")), C_CHAR, Range.lessThanOrEqual(createCharType(10), utf8Slice("123456788" + maxCodePoint)));
        testSimpleComparison(lessThan(cast(C_CHAR, VARCHAR), stringLiteral("1234567890")), C_CHAR, Range.lessThan(createCharType(10), Slices.utf8Slice("1234567890")));
        testSimpleComparison(lessThan(cast(C_CHAR, VARCHAR), stringLiteral("12345678901")), C_CHAR, Range.lessThanOrEqual(createCharType(10), Slices.utf8Slice("1234567890")));

        // equal
        testSimpleComparison(equal(cast(C_CHAR, VARCHAR), stringLiteral("123456789")), C_CHAR, Domain.none(createCharType(10)));
        testSimpleComparison(equal(cast(C_CHAR, VARCHAR), stringLiteral("1234567890")), C_CHAR, Range.equal(createCharType(10), Slices.utf8Slice("1234567890")));
        testSimpleComparison(equal(cast(C_CHAR, VARCHAR), stringLiteral("12345678901")), C_CHAR, Domain.none(createCharType(10)));

        // not equal
        testSimpleComparison(notEqual(cast(C_CHAR, VARCHAR), stringLiteral("123456789")), C_CHAR, Domain.notNull(createCharType(10)));
        testSimpleComparison(notEqual(cast(C_CHAR, VARCHAR), stringLiteral("1234567890")), C_CHAR, Domain.create(ValueSet.ofRanges(
                Range.lessThan(createCharType(10), Slices.utf8Slice("1234567890")), Range.greaterThan(createCharType(10), Slices.utf8Slice("1234567890"))), false));
        testSimpleComparison(notEqual(cast(C_CHAR, VARCHAR), stringLiteral("12345678901")), C_CHAR, Domain.notNull(createCharType(10)));

        // is distinct from
        testSimpleComparison(isDistinctFrom(cast(C_CHAR, VARCHAR), stringLiteral("123456789")), C_CHAR, Domain.all(createCharType(10)));
        testSimpleComparison(isDistinctFrom(cast(C_CHAR, VARCHAR), stringLiteral("1234567890")), C_CHAR, Domain.create(ValueSet.ofRanges(
                Range.lessThan(createCharType(10), Slices.utf8Slice("1234567890")), Range.greaterThan(createCharType(10), Slices.utf8Slice("1234567890"))), true));
        testSimpleComparison(isDistinctFrom(cast(C_CHAR, VARCHAR), stringLiteral("12345678901")), C_CHAR, Domain.all(createCharType(10)));
    }

    @Test
    public void testCharComparedToVarcharExpression()
    {
        Type charType = createCharType(10);
        // varchar literal is coerced to column (char) type
        testSimpleComparison(equal(C_CHAR, cast(stringLiteral("abc"), charType)), C_CHAR, Range.equal(charType, Slices.utf8Slice("abc")));

        // both sides got coerced to char(11)
        charType = createCharType(11);
        assertUnsupportedPredicate(equal(cast(C_CHAR, charType), cast(stringLiteral("abc12345678"), charType)));
    }

    private void assertPredicateTranslates(RowExpression expression, TupleDomain<RowExpression> tupleDomain)
    {
        DomainExtractor.ExtractionResult result = fromPredicate(expression);
        assertEquals(result.getRemainingExpression(), TRUE);
        assertEquals(result.getTupleDomain(), tupleDomain);
    }

    private void assertPredicateTranslates(RowExpression expression, TupleDomain<RowExpression> tupleDomain, RowExpression remaining)
    {
        DomainExtractor.ExtractionResult result = fromPredicate(expression);
        assertEquals(result.getRemainingExpression(), remaining);
        assertEquals(result.getTupleDomain(), tupleDomain);
    }

    private void assertPredicateIsAlwaysTrue(RowExpression expression)
    {
        assertPredicateTranslates(expression, TupleDomain.all());
    }

    private void assertPredicateIsAlwaysFalse(RowExpression expression)
    {
        DomainExtractor.ExtractionResult result = fromPredicate(expression);
        assertEquals(result.getRemainingExpression(), TRUE);
        assertTrue(result.getTupleDomain().isNone());
    }

    private RowExpression toPredicate(TupleDomain<? extends RowExpression> tupleDomain)
    {
        return domainTranslator.toPredicate(tupleDomain);
    }

    private DomainExtractor.ExtractionResult fromPredicate(RowExpression originalPredicate)
    {
        return DomainExtractor.fromPredicate(metadata, TEST_SESSION, originalPredicate);
    }

    private RowExpression nullLiteral(Type type)
    {
        return constantNull(type);
    }

    private RowExpression stringLiteral(String value)
    {
        return constant(Slices.utf8Slice(value), VARCHAR);
    }

    private RowExpression bigintLiteral(long value)
    {
        return constant(value, BIGINT);
    }

    private RowExpression doubleLiteral(double value)
    {
        return constant(value, DOUBLE);
    }

    private RowExpression varbinaryLiteral(Slice value)
    {
        return constant(value, VARBINARY);
    }

    private long realValue(float value)
    {
        return (long) floatToIntBits(value);
    }

    private RowExpression realLiteral(float value)
    {
        return constant(realValue(value), REAL);
    }

    private RowExpression colorLiteral(long value)
    {
        return constant(value, COLOR);
    }

    private static Long shortDecimal(String value)
    {
        return new BigDecimal(value).unscaledValue().longValueExact();
    }

    private static Slice longDecimal(String value)
    {
        return encodeScaledValue(new BigDecimal(value));
    }

    private static RowExpression isNull(RowExpression expression)
    {
        return new SpecialFormExpression(IS_NULL, BOOLEAN, expression);
    }

    private RowExpression cast(RowExpression expression, Type toType)
    {
        FunctionHandle cast = metadata.getFunctionManager().lookupCast(CastType.CAST, expression.getType().getTypeSignature(), toType.getTypeSignature());
        return call(CastType.CAST.name(), cast, toType, expression);
    }

    private RowExpression not(RowExpression expression)
    {
        return call("not", new StandardFunctionResolution(metadata.getFunctionManager()).notFunction(), expression.getType(), expression);
    }

    private RowExpression in(RowExpression value, List<RowExpression> inList)
    {
        return new SpecialFormExpression(IN, BOOLEAN, ImmutableList.<RowExpression>builder().add(value).addAll(inList).build());
    }

    private RowExpression add(RowExpression left, RowExpression right)
    {
        return call(
                ADD.name(),
                new StandardFunctionResolution(metadata.getFunctionManager()).arithmeticFunction(ArithmeticBinaryExpression.Operator.ADD, left.getType(), right.getType()),
                left.getType(),
                ImmutableList.of(left, right));
    }

    private RowExpression dereference(Type returnType, RowExpression row, RowExpression fieldReference)
    {
        return specialForm(
                DEREFERENCE,
                returnType,
                ImmutableList.of(row, fieldReference));
    }

    private RowExpression regex_extract(RowExpression cVarchar, ConstantExpression pattern)
    {
        return new CallExpression(
                "regexp_extract",
                metadata.getFunctionManager().lookupFunction(QualifiedName.of("regexp_extract"), fromTypes(VARCHAR, JoniRegexpType.JONI_REGEXP)),
                VARCHAR,
                ImmutableList.of(cVarchar, pattern));
    }

    private RowExpression isNotNull(RowExpression expression)
    {
        return not(isNull(expression));
    }

    private RowExpression bigintIn(RowExpression value, List<Long> inList)
    {
        List<RowExpression> arguments = inList.stream().map(argument -> constant(argument, BIGINT)).collect(toImmutableList());
        return in(value, arguments);
    }

    private RowExpression binaryOperator(OperatorType operatorType, RowExpression left, RowExpression right)
    {
        return call(
                operatorType.name(),
                metadata.getFunctionManager().resolveOperator(operatorType, fromTypes(left.getType(), right.getType())),
                BOOLEAN,
                left,
                right);
    }

    private RowExpression between(RowExpression value, RowExpression min, RowExpression max)
    {
        return call(
                "between",
                metadata.getFunctionManager().resolveOperator(OperatorType.BETWEEN, fromTypes(value.getType(), min.getType(), max.getType())),
                BOOLEAN,
                value,
                min,
                max);
    }

    private RowExpression isDistinctFrom(RowExpression left, RowExpression right)
    {
        return binaryOperator(OperatorType.IS_DISTINCT_FROM, left, right);
    }

    private RowExpression greaterThan(RowExpression left, RowExpression right)
    {
        return binaryOperator(OperatorType.GREATER_THAN, left, right);
    }

    private RowExpression lessThan(RowExpression left, RowExpression right)
    {
        return binaryOperator(OperatorType.LESS_THAN, left, right);
    }

    private RowExpression greaterThanOrEqual(RowExpression left, RowExpression right)
    {
        return binaryOperator(GREATER_THAN_OR_EQUAL, left, right);
    }

    private RowExpression lessThanOrEqual(RowExpression left, RowExpression right)
    {
        return binaryOperator(LESS_THAN_OR_EQUAL, left, right);
    }

    private RowExpression equal(RowExpression left, RowExpression right)
    {
        return binaryOperator(OperatorType.EQUAL, left, right);
    }

    private RowExpression notEqual(RowExpression left, RowExpression right)
    {
        return binaryOperator(NOT_EQUAL, left, right);
    }

    private RowExpression unprocessableExpression1(RowExpression expression)
    {
        return greaterThan(expression, expression);
    }

    private RowExpression unprocessableExpression2(RowExpression expression)
    {
        return lessThan(expression, expression);
    }

    private RowExpression randPredicate(RowExpression expression)
    {
        RowExpression random = call("random", metadata.getFunctionManager().lookupFunction(QualifiedName.of("random"), fromTypes()), DOUBLE);
        return greaterThan(
                expression,
                call(GREATER_THAN.getOperator(), metadata.getFunctionManager().lookupCast(CastType.CAST, DOUBLE.getTypeSignature(), expression.getType().getTypeSignature()), expression.getType(), random));
    }

    private void assertUnsupportedPredicate(RowExpression expression)
    {
        DomainExtractor.ExtractionResult result = fromPredicate(expression);
        assertEquals(result.getRemainingExpression(), expression);
        assertEquals(result.getTupleDomain(), TupleDomain.all());
    }

    private void testSimpleComparison(RowExpression expression, RowExpression input, Range expectedDomainRange)
    {
        testSimpleComparison(expression, input, Domain.create(ValueSet.ofRanges(expectedDomainRange), false));
    }

    private void testSimpleComparison(RowExpression expression, RowExpression input, Domain domain)
    {
        DomainExtractor.ExtractionResult result = fromPredicate(expression);
        assertEquals(result.getRemainingExpression(), TRUE);
        TupleDomain<RowExpression> actual = result.getTupleDomain();
        TupleDomain<RowExpression> expected = withColumnDomains(ImmutableMap.of(input, domain));
        if (!actual.equals(expected)) {
            fail(format("for comparison [%s] expected %s but found %s", expression.toString(), expected.toString(SESSION), actual.toString(SESSION)));
        }
    }

    private void assertEquals(RowExpression actual, RowExpression expected)
    {
        Assert.assertEquals(canonicalizer.canonicalize(actual), canonicalizer.canonicalize(expected));
    }

    private void assertEquals(TupleDomain<RowExpression> actual, TupleDomain<RowExpression> expected)
    {
        Assert.assertEquals(actual, expected,
                format("\nExpected Domain :%s\nActual Domain   :%s",
                        expected.toString(TEST_SESSION.toConnectorSession()),
                        actual.toString(TEST_SESSION.toConnectorSession())));
    }

    private static class NumericValues<T>
    {
        private final RowExpression input;
        private final T min;
        private final T integerNegative;
        private final T fractionalNegative;
        private final T integerPositive;
        private final T fractionalPositive;
        private final T max;

        private NumericValues(RowExpression input, T min, T integerNegative, T fractionalNegative, T integerPositive, T fractionalPositive, T max)
        {
            this.input = requireNonNull(input, "input is null");
            this.min = requireNonNull(min, "min is null");
            this.integerNegative = requireNonNull(integerNegative, "integerNegative is null");
            this.fractionalNegative = requireNonNull(fractionalNegative, "fractionalNegative is null");
            this.integerPositive = requireNonNull(integerPositive, "integerPositive is null");
            this.fractionalPositive = requireNonNull(fractionalPositive, "fractionalPositive is null");
            this.max = requireNonNull(max, "max is null");
        }

        public RowExpression getInput()
        {
            return input;
        }

        public T getMin()
        {
            return min;
        }

        public T getIntegerNegative()
        {
            return integerNegative;
        }

        public T getFractionalNegative()
        {
            return fractionalNegative;
        }

        public T getIntegerPositive()
        {
            return integerPositive;
        }

        public T getFractionalPositive()
        {
            return fractionalPositive;
        }

        public T getMax()
        {
            return max;
        }

        public boolean isFractional()
        {
            return input.getType() == DOUBLE || input.getType() == REAL || (input.getType() instanceof DecimalType && ((DecimalType) input.getType()).getScale() > 0);
        }
    }
}
