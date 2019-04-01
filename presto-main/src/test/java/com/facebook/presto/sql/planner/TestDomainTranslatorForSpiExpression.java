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
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.SpiExpression;
import com.facebook.presto.spi.predicate.SpiUnaryExpression;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NotExpression;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.ExpressionUtils.or;
import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.NOT_EQUAL;
import static com.facebook.presto.type.ColorType.COLOR;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Collections.nCopies;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestDomainTranslatorForSpiExpression
{
    private Metadata metadata;
    private LiteralEncoder literalEncoder;
    private ExpressionDomainTranslator domainTranslator;

    @BeforeClass
    public void setup()
    {
        metadata = createTestMetadataManager();
        literalEncoder = new LiteralEncoder(metadata.getBlockEncodingSerde());
        domainTranslator = new ExpressionDomainTranslator(literalEncoder);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        metadata = null;
        literalEncoder = null;
        domainTranslator = null;
    }

    @Test
    public void testToPredicateNone()
    {
        SpiExpression spiExpression = new SpiUnaryExpression("C_BOOLEAN", Domain.none(BOOLEAN));
        assertEquals(toPredicateFromSpiExpression(spiExpression), FALSE_LITERAL);
    }

    @Test
    public void testToPredicateAll()
    {
        SpiExpression spiExpression = new SpiUnaryExpression("C_BOOLEAN", Domain.all(BOOLEAN));
        assertEquals(toPredicateFromSpiExpression(spiExpression), TRUE_LITERAL);
    }

    @Test
    public void testToPredicate()
    {
        SpiExpression spiExpression;

        spiExpression = new SpiUnaryExpression("C_BIGINT", Domain.notNull(BIGINT));
        assertEquals(toPredicateFromSpiExpression(spiExpression), isNotNull(new Identifier("C_BIGINT")));

        spiExpression = new SpiUnaryExpression("C_BIGINT", Domain.onlyNull(BIGINT));
        assertEquals(toPredicateFromSpiExpression(spiExpression), isNull(new Identifier("C_BIGINT")));

        spiExpression = new SpiUnaryExpression("C_BIGINT", Domain.none(BIGINT));
        assertEquals(toPredicateFromSpiExpression(spiExpression), FALSE_LITERAL);

        spiExpression = new SpiUnaryExpression("C_BIGINT", Domain.all(BIGINT));
        assertEquals(toPredicateFromSpiExpression(spiExpression), TRUE_LITERAL);

        spiExpression = new SpiUnaryExpression("C_BIGINT", Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 1L)), false));
        assertEquals(toPredicateFromSpiExpression(spiExpression), greaterThan(new Identifier("C_BIGINT"), bigintLiteral(1L)));

        spiExpression = new SpiUnaryExpression("C_BIGINT", Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 1L)), false));
        assertEquals(toPredicateFromSpiExpression(spiExpression), greaterThanOrEqual(new Identifier("C_BIGINT"), bigintLiteral(1L)));

        spiExpression = new SpiUnaryExpression("C_BIGINT", Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L)), false));
        assertEquals(toPredicateFromSpiExpression(spiExpression), lessThan(new Identifier("C_BIGINT"), bigintLiteral(1L)));

        spiExpression = new SpiUnaryExpression("C_BIGINT", Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 0L, false, 1L, true)), false));
        assertEquals(toPredicateFromSpiExpression(spiExpression), and(greaterThan(new Identifier("C_BIGINT"), bigintLiteral(0L)), lessThanOrEqual(new Identifier("C_BIGINT"), bigintLiteral(1L))));

        spiExpression = new SpiUnaryExpression("C_BIGINT", Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 1L)), false));
        assertEquals(toPredicateFromSpiExpression(spiExpression), lessThanOrEqual(new Identifier("C_BIGINT"), bigintLiteral(1L)));

        spiExpression = new SpiUnaryExpression("C_BIGINT", Domain.singleValue(BIGINT, 1L));
        assertEquals(toPredicateFromSpiExpression(spiExpression), equal(new Identifier("C_BIGINT"), bigintLiteral(1L)));

        spiExpression = new SpiUnaryExpression("C_BIGINT", Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false));
        assertEquals(toPredicateFromSpiExpression(spiExpression), in(new Identifier("C_BIGINT"), BIGINT, ImmutableList.of(1L, 2L)));

        spiExpression = new SpiUnaryExpression("C_BIGINT", Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L)), true));
        assertEquals(toPredicateFromSpiExpression(spiExpression), or(lessThan(new Identifier("C_BIGINT"), bigintLiteral(1L)), isNull(new Identifier("C_BIGINT"))));

        spiExpression = new SpiUnaryExpression("C_VARCHAR", Domain.create(ValueSet.ofRanges(Range.equal(VARCHAR, utf8Slice("abcd")), Range.equal(VARCHAR, utf8Slice("abcde"))), false));
        assertEquals(toPredicateFromSpiExpression(spiExpression), in(new Identifier("C_VARCHAR"), VARCHAR, ImmutableList.of(utf8Slice("abcd"), utf8Slice("abcde"))));
    }

    @Test
    public void testSupportedTypes()
    {
        SpiExpression spiExpression;

        spiExpression = new SpiUnaryExpression("C_BIGINT", Domain.none(BIGINT));
        assertEquals(toPredicateFromSpiExpression(spiExpression), FALSE_LITERAL);

        spiExpression = new SpiUnaryExpression("C_DOUBLE", Domain.none(DOUBLE));
        assertEquals(toPredicateFromSpiExpression(spiExpression), FALSE_LITERAL);

        spiExpression = new SpiUnaryExpression("C_VARCHAR", Domain.none(VARCHAR));
        assertEquals(toPredicateFromSpiExpression(spiExpression), FALSE_LITERAL);

        spiExpression = new SpiUnaryExpression("C_BOOLEAN", Domain.none(BOOLEAN));
        assertEquals(toPredicateFromSpiExpression(spiExpression), FALSE_LITERAL);

        spiExpression = new SpiUnaryExpression("C_TIMESTAMP", Domain.none(TIMESTAMP));
        assertEquals(toPredicateFromSpiExpression(spiExpression), FALSE_LITERAL);

        spiExpression = new SpiUnaryExpression("C_DATE", Domain.none(DATE));
        assertEquals(toPredicateFromSpiExpression(spiExpression), FALSE_LITERAL);

        spiExpression = new SpiUnaryExpression("C_INTEGER", Domain.none(INTEGER));
        assertEquals(toPredicateFromSpiExpression(spiExpression), FALSE_LITERAL);

        spiExpression = new SpiUnaryExpression("C_INTEGER", Domain.none(INTEGER));
        assertEquals(toPredicateFromSpiExpression(spiExpression), FALSE_LITERAL);
    }

    @Test
    public void testUnsupportedTypes()
    {
        SpiExpression spiExpression1 = new SpiUnaryExpression("C_COLOR", Domain.all(COLOR));
        assertThrows(() -> toPredicateFromSpiExpression(spiExpression1));

        SpiExpression spiExpression2 = new SpiUnaryExpression("C_HYPER_LOG_LOG", Domain.all(HYPER_LOG_LOG));
        assertThrows(() -> toPredicateFromSpiExpression(spiExpression2));

        SpiExpression spiExpression3 = new SpiUnaryExpression("C_VARBINARY", Domain.all(VARBINARY));
        assertThrows(() -> toPredicateFromSpiExpression(spiExpression3));

        SpiExpression spiExpression4 = new SpiUnaryExpression("C_DECIMAL", Domain.all(createDecimalType(21, 3)));
        assertThrows(() -> toPredicateFromSpiExpression(spiExpression4));

        SpiExpression spiExpression5 = new SpiUnaryExpression("C_SMALLINT", Domain.all(SMALLINT));
        assertThrows(() -> toPredicateFromSpiExpression(spiExpression5));

        SpiExpression spiExpression6 = new SpiUnaryExpression("C_TINYINT", Domain.all(TINYINT));
        assertThrows(() -> toPredicateFromSpiExpression(spiExpression6));

        SpiExpression spiExpression7 = new SpiUnaryExpression("C_REAL", Domain.all(REAL));
        assertThrows(() -> toPredicateFromSpiExpression(spiExpression7));
    }

    private Expression toPredicateFromSpiExpression(SpiExpression spiExpression)
    {
        return domainTranslator.toPredicateFromSpiExpression(spiExpression);
    }

    private static Expression isNotNull(Expression expression)
    {
        return new NotExpression(new IsNullPredicate(expression));
    }

    private static IsNullPredicate isNull(Expression expression)
    {
        return new IsNullPredicate(expression);
    }

    private InPredicate in(Expression expression, Type expressisonType, List<?> values)
    {
        List<Type> types = nCopies(values.size(), expressisonType);
        List<Expression> expressions = literalEncoder.toExpressions(values, types);
        return new InPredicate(expression, new InListExpression(expressions));
    }

    private static ComparisonExpression equal(Expression left, Expression right)
    {
        return comparison(EQUAL, left, right);
    }

    private static ComparisonExpression notEqual(Expression left, Expression right)
    {
        return comparison(NOT_EQUAL, left, right);
    }

    private static ComparisonExpression greaterThan(Expression left, Expression right)
    {
        return comparison(GREATER_THAN, left, right);
    }

    private static ComparisonExpression greaterThanOrEqual(Expression left, Expression right)
    {
        return comparison(GREATER_THAN_OR_EQUAL, left, right);
    }

    private static ComparisonExpression lessThan(Expression left, Expression expression)
    {
        return comparison(LESS_THAN, left, expression);
    }

    private static ComparisonExpression lessThanOrEqual(Expression left, Expression right)
    {
        return comparison(LESS_THAN_OR_EQUAL, left, right);
    }

    private static ComparisonExpression isDistinctFrom(Expression left, Expression right)
    {
        return comparison(IS_DISTINCT_FROM, left, right);
    }

    private static ComparisonExpression comparison(ComparisonExpression.Operator operator, Expression expression1, Expression expression2)
    {
        return new ComparisonExpression(operator, expression1, expression2);
    }

    private static Literal bigintLiteral(long value)
    {
        if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
            return new GenericLiteral("BIGINT", Long.toString(value));
        }
        return new LongLiteral(Long.toString(value));
    }
}
