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
package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.LiteralEncoder;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.testng.Assert.assertEquals;

public class TestScalarStatsCalculator
{
    private static final Map<String, Type> DEFAULT_SYMBOL_TYPES = ImmutableMap.of(
            "a", BIGINT,
            "x", BIGINT,
            "y", BIGINT,
            "all_null", BIGINT);

    private ScalarStatsCalculator calculator;
    private Session session;
    private final SqlParser sqlParser = new SqlParser();
    private TestingRowExpressionTranslator translator;

    @BeforeClass
    public void setUp()
    {
        calculator = new ScalarStatsCalculator(MetadataManager.createTestMetadataManager());
        session = testSessionBuilder().build();
        translator = new TestingRowExpressionTranslator(MetadataManager.createTestMetadataManager());
    }

    @Test
    public void testLiteral()
    {
        assertCalculate(new GenericLiteral("TINYINT", "7"))
                .distinctValuesCount(1.0)
                .lowValue(7)
                .highValue(7)
                .nullsFraction(0.0);

        assertCalculate(new GenericLiteral("SMALLINT", "8"))
                .distinctValuesCount(1.0)
                .lowValue(8)
                .highValue(8)
                .nullsFraction(0.0);

        assertCalculate(new GenericLiteral("INTEGER", "9"))
                .distinctValuesCount(1.0)
                .lowValue(9)
                .highValue(9)
                .nullsFraction(0.0);

        assertCalculate(new GenericLiteral("BIGINT", Long.toString(Long.MAX_VALUE)))
                .distinctValuesCount(1.0)
                .lowValue(Long.MAX_VALUE)
                .highValue(Long.MAX_VALUE)
                .nullsFraction(0.0);

        assertCalculate(new DoubleLiteral("7.5"))
                .distinctValuesCount(1.0)
                .lowValue(7.5)
                .highValue(7.5)
                .nullsFraction(0.0);

        assertCalculate(new DecimalLiteral("75.5"))
                .distinctValuesCount(1.0)
                .lowValue(75.5)
                .highValue(75.5)
                .nullsFraction(0.0);

        assertCalculate(new StringLiteral("blah"))
                .distinctValuesCount(1.0)
                .lowValueUnknown()
                .highValueUnknown()
                .nullsFraction(0.0);

        assertCalculate(new NullLiteral())
                .distinctValuesCount(0.0)
                .lowValueUnknown()
                .highValueUnknown()
                .nullsFraction(1.0);
    }

    @Test
    public void testFunctionCall()
    {
        assertCalculate(
                new FunctionCall(
                        QualifiedName.of("length"),
                        ImmutableList.of(new Cast(new NullLiteral(), "VARCHAR(10)"))))
                .distinctValuesCount(0.0)
                .lowValueUnknown()
                .highValueUnknown()
                .nullsFraction(1.0);

        assertCalculate(
                new FunctionCall(
                        QualifiedName.of("length"),
                        ImmutableList.of(new SymbolReference("x"))),
                PlanNodeStatsEstimate.unknown(),
                TypeProvider.viewOf(ImmutableMap.of("x", createVarcharType(2))))
                .distinctValuesCountUnknown()
                .lowValueUnknown()
                .highValueUnknown()
                .nullsFractionUnknown();
    }

    @Test
    public void testVarbinaryConstant()
    {
        MetadataManager metadata = createTestMetadataManager();
        LiteralEncoder literalEncoder = new LiteralEncoder(metadata.getBlockEncodingSerde());
        Expression expression = literalEncoder.toExpression(Slices.utf8Slice("ala ma kota"), VARBINARY);

        assertCalculate(expression)
                .distinctValuesCount(1.0)
                .lowValueUnknown()
                .highValueUnknown()
                .nullsFraction(0.0);
    }

    @Test
    public void testSymbolReference()
    {
        VariableStatsEstimate xStats = VariableStatsEstimate.builder()
                .setLowValue(-1)
                .setHighValue(10)
                .setDistinctValuesCount(4)
                .setNullsFraction(0.1)
                .setAverageRowSize(2.0)
                .build();
        PlanNodeStatsEstimate inputStatistics = PlanNodeStatsEstimate.builder()
                .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "x", BIGINT), xStats)
                .build();

        assertCalculate(expression("x"), inputStatistics).isEqualTo(xStats);
        assertCalculate(expression("y"), inputStatistics).isEqualTo(VariableStatsEstimate.unknown());
    }

    @Test
    public void testCastDoubleToBigint()
    {
        PlanNodeStatsEstimate inputStatistics = PlanNodeStatsEstimate.builder()
                .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "a", BIGINT), VariableStatsEstimate.builder()
                        .setNullsFraction(0.3)
                        .setLowValue(1.6)
                        .setHighValue(17.3)
                        .setDistinctValuesCount(10)
                        .setAverageRowSize(2.0)
                        .build())
                .build();

        assertCalculate(new Cast(new SymbolReference("a"), "bigint"), inputStatistics)
                .lowValue(2.0)
                .highValue(17.0)
                .distinctValuesCount(10)
                .nullsFraction(0.3)
                .dataSizeUnknown();
    }

    @Test
    public void testCastDoubleToShortRange()
    {
        PlanNodeStatsEstimate inputStatistics = PlanNodeStatsEstimate.builder()
                .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "a", BIGINT), VariableStatsEstimate.builder()
                        .setNullsFraction(0.3)
                        .setLowValue(1.6)
                        .setHighValue(3.3)
                        .setDistinctValuesCount(10)
                        .setAverageRowSize(2.0)
                        .build())
                .build();

        assertCalculate(new Cast(new SymbolReference("a"), "bigint"), inputStatistics)
                .lowValue(2.0)
                .highValue(3.0)
                .distinctValuesCount(2)
                .nullsFraction(0.3)
                .dataSizeUnknown();
    }

    @Test
    public void testCastDoubleToShortRangeUnknownDistinctValuesCount()
    {
        PlanNodeStatsEstimate inputStatistics = PlanNodeStatsEstimate.builder()
                .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "a", BIGINT), VariableStatsEstimate.builder()
                        .setNullsFraction(0.3)
                        .setLowValue(1.6)
                        .setHighValue(3.3)
                        .setAverageRowSize(2.0)
                        .build())
                .build();

        assertCalculate(new Cast(new SymbolReference("a"), "bigint"), inputStatistics)
                .lowValue(2.0)
                .highValue(3.0)
                .distinctValuesCountUnknown()
                .nullsFraction(0.3)
                .dataSizeUnknown();
    }

    @Test
    public void testCastBigintToDouble()
    {
        PlanNodeStatsEstimate inputStatistics = PlanNodeStatsEstimate.builder()
                .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "a", DOUBLE), VariableStatsEstimate.builder()
                        .setNullsFraction(0.3)
                        .setLowValue(2.0)
                        .setHighValue(10.0)
                        .setDistinctValuesCount(4)
                        .setAverageRowSize(2.0)
                        .build())
                .build();

        assertCalculate(new Cast(new SymbolReference("a"), "double"), inputStatistics, TypeProvider.viewOf(ImmutableMap.of("a", DOUBLE)))
                .lowValue(2.0)
                .highValue(10.0)
                .distinctValuesCount(4)
                .nullsFraction(0.3)
                .dataSizeUnknown();
    }

    @Test
    public void testCastUnknown()
    {
        assertCalculate(new Cast(new SymbolReference("a"), "bigint"), PlanNodeStatsEstimate.unknown())
                .lowValueUnknown()
                .highValueUnknown()
                .distinctValuesCountUnknown()
                .nullsFractionUnknown()
                .dataSizeUnknown();
    }

    private VariableStatsAssertion assertCalculate(Expression scalarExpression)
    {
        return assertCalculate(scalarExpression, PlanNodeStatsEstimate.unknown());
    }

    private VariableStatsAssertion assertCalculate(Expression scalarExpression, PlanNodeStatsEstimate inputStatistics)
    {
        return assertCalculate(scalarExpression, inputStatistics, TypeProvider.viewOf(DEFAULT_SYMBOL_TYPES));
    }

    private VariableStatsAssertion assertCalculate(Expression scalarExpression, PlanNodeStatsEstimate inputStatistics, TypeProvider types)
    {
        // assert both visitors yield the same result
        RowExpression scalarRowExpression = translator.translate(scalarExpression, types);
        VariableStatsEstimate expressionVariableStatsEstimate = calculator.calculate(scalarExpression, inputStatistics, session, types);
        VariableStatsEstimate rowExpressionVariableStatsEstimate = calculator.calculate(scalarRowExpression, inputStatistics, session);
        assertEquals(expressionVariableStatsEstimate, rowExpressionVariableStatsEstimate);
        return VariableStatsAssertion.assertThat(expressionVariableStatsEstimate);
    }

    @Test
    public void testNonDivideArithmeticBinaryExpression()
    {
        PlanNodeStatsEstimate relationStats = PlanNodeStatsEstimate.builder()
                .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "x", BIGINT), VariableStatsEstimate.builder()
                        .setLowValue(-1)
                        .setHighValue(10)
                        .setDistinctValuesCount(4)
                        .setNullsFraction(0.1)
                        .setAverageRowSize(2.0)
                        .build())
                .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "y", BIGINT), VariableStatsEstimate.builder()
                        .setLowValue(-2)
                        .setHighValue(5)
                        .setDistinctValuesCount(3)
                        .setNullsFraction(0.2)
                        .setAverageRowSize(2.0)
                        .build())
                .setOutputRowCount(10)
                .build();

        assertCalculate(expression("x + y"), relationStats)
                .distinctValuesCount(10.0)
                .lowValue(-3.0)
                .highValue(15.0)
                .nullsFraction(0.28)
                .averageRowSize(2.0);

        assertCalculate(expression("x - y"), relationStats)
                .distinctValuesCount(10.0)
                .lowValue(-6.0)
                .highValue(12.0)
                .nullsFraction(0.28)
                .averageRowSize(2.0);

        assertCalculate(expression("x * y"), relationStats)
                .distinctValuesCount(10.0)
                .lowValue(-20.0)
                .highValue(50.0)
                .nullsFraction(0.28)
                .averageRowSize(2.0);
    }

    @Test
    public void tesArithmeticUnaryExpression()
    {
        PlanNodeStatsEstimate relationStats = PlanNodeStatsEstimate.builder()
                .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "x", BIGINT), VariableStatsEstimate.builder()
                        .setLowValue(-1)
                        .setHighValue(10)
                        .setDistinctValuesCount(4)
                        .setNullsFraction(0.1)
                        .setAverageRowSize(2.0)
                        .build())
                .setOutputRowCount(10)
                .build();

        assertCalculate(expression("+x"), relationStats)
                .distinctValuesCount(4.0)
                .lowValue(-1.0)
                .highValue(10.0)
                .nullsFraction(0.1)
                .averageRowSize(2.0);

        assertCalculate(expression("-x"), relationStats)
                .distinctValuesCount(4.0)
                .lowValue(-10.0)
                .highValue(1.0)
                .nullsFraction(0.1)
                .averageRowSize(2.0);
    }

    @Test
    public void testArithmeticBinaryWithAllNullsSymbol()
    {
        VariableStatsEstimate allNullStats = VariableStatsEstimate.zero();
        PlanNodeStatsEstimate relationStats = PlanNodeStatsEstimate.builder()
                .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "x", BIGINT), VariableStatsEstimate.builder()
                        .setLowValue(-1)
                        .setHighValue(10)
                        .setDistinctValuesCount(4)
                        .setNullsFraction(0.1)
                        .setAverageRowSize(0)
                        .build())
                .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "all_null", BIGINT), allNullStats)
                .setOutputRowCount(10)
                .build();

        assertCalculate(expression("x + all_null"), relationStats)
                .isEqualTo(allNullStats);
        assertCalculate(expression("x - all_null"), relationStats)
                .isEqualTo(allNullStats);
        assertCalculate(expression("all_null - x"), relationStats)
                .isEqualTo(allNullStats);
        assertCalculate(expression("all_null * x"), relationStats)
                .isEqualTo(allNullStats);
        assertCalculate(expression("x % all_null"), relationStats)
                .isEqualTo(allNullStats);
        assertCalculate(expression("all_null % x"), relationStats)
                .isEqualTo(allNullStats);
        assertCalculate(expression("x / all_null"), relationStats)
                .isEqualTo(allNullStats);
        assertCalculate(expression("all_null / x"), relationStats)
                .isEqualTo(allNullStats);
    }

    @Test
    public void testDivideArithmeticBinaryExpression()
    {
        assertCalculate(expression("x / y"), xyStats(-11, -3, -5, -4)).lowValue(0.6).highValue(2.75);
        assertCalculate(expression("x / y"), xyStats(-11, -3, -5, 4)).lowValue(NEGATIVE_INFINITY).highValue(POSITIVE_INFINITY);
        assertCalculate(expression("x / y"), xyStats(-11, -3, 4, 5)).lowValue(-2.75).highValue(-0.6);

        assertCalculate(expression("x / y"), xyStats(-11, 0, -5, -4)).lowValue(0).highValue(2.75);
        assertCalculate(expression("x / y"), xyStats(-11, 0, -5, 4)).lowValue(NEGATIVE_INFINITY).highValue(POSITIVE_INFINITY);
        assertCalculate(expression("x / y"), xyStats(-11, 0, 4, 5)).lowValue(-2.75).highValue(0);

        assertCalculate(expression("x / y"), xyStats(-11, 3, -5, -4)).lowValue(-0.75).highValue(2.75);
        assertCalculate(expression("x / y"), xyStats(-11, 3, -5, 4)).lowValue(NEGATIVE_INFINITY).highValue(POSITIVE_INFINITY);
        assertCalculate(expression("x / y"), xyStats(-11, 3, 4, 5)).lowValue(-2.75).highValue(0.75);

        assertCalculate(expression("x / y"), xyStats(0, 3, -5, -4)).lowValue(-0.75).highValue(0);
        assertCalculate(expression("x / y"), xyStats(0, 3, -5, 4)).lowValue(NEGATIVE_INFINITY).highValue(POSITIVE_INFINITY);
        assertCalculate(expression("x / y"), xyStats(0, 3, 4, 5)).lowValue(0).highValue(0.75);

        assertCalculate(expression("x / y"), xyStats(3, 11, -5, -4)).lowValue(-2.75).highValue(-0.6);
        assertCalculate(expression("x / y"), xyStats(3, 11, -5, 4)).lowValue(NEGATIVE_INFINITY).highValue(POSITIVE_INFINITY);
        assertCalculate(expression("x / y"), xyStats(3, 11, 4, 5)).lowValue(0.6).highValue(2.75);
    }

    @Test
    public void testModulusArithmeticBinaryExpression()
    {
        // negative
        assertCalculate(expression("x % y"), xyStats(-1, 0, -6, -4)).lowValue(-1).highValue(0);
        assertCalculate(expression("x % y"), xyStats(-5, 0, -6, -4)).lowValue(-5).highValue(0);
        assertCalculate(expression("x % y"), xyStats(-8, 0, -6, -4)).lowValue(-6).highValue(0);
        assertCalculate(expression("x % y"), xyStats(-8, 0, -6, -4)).lowValue(-6).highValue(0);
        assertCalculate(expression("x % y"), xyStats(-8, 0, -6, 4)).lowValue(-6).highValue(0);
        assertCalculate(expression("x % y"), xyStats(-8, 0, -6, 6)).lowValue(-6).highValue(0);
        assertCalculate(expression("x % y"), xyStats(-8, 0, 4, 6)).lowValue(-6).highValue(0);
        assertCalculate(expression("x % y"), xyStats(-1, 0, 4, 6)).lowValue(-1).highValue(0);
        assertCalculate(expression("x % y"), xyStats(-5, 0, 4, 6)).lowValue(-5).highValue(0);
        assertCalculate(expression("x % y"), xyStats(-8, 0, 4, 6)).lowValue(-6).highValue(0);

        // positive
        assertCalculate(expression("x % y"), xyStats(0, 5, -6, -4)).lowValue(0).highValue(5);
        assertCalculate(expression("x % y"), xyStats(0, 8, -6, -4)).lowValue(0).highValue(6);
        assertCalculate(expression("x % y"), xyStats(0, 1, -6, 4)).lowValue(0).highValue(1);
        assertCalculate(expression("x % y"), xyStats(0, 5, -6, 4)).lowValue(0).highValue(5);
        assertCalculate(expression("x % y"), xyStats(0, 8, -6, 4)).lowValue(0).highValue(6);
        assertCalculate(expression("x % y"), xyStats(0, 1, 4, 6)).lowValue(0).highValue(1);
        assertCalculate(expression("x % y"), xyStats(0, 5, 4, 6)).lowValue(0).highValue(5);
        assertCalculate(expression("x % y"), xyStats(0, 8, 4, 6)).lowValue(0).highValue(6);

        // mix
        assertCalculate(expression("x % y"), xyStats(-1, 1, -6, -4)).lowValue(-1).highValue(1);
        assertCalculate(expression("x % y"), xyStats(-1, 5, -6, -4)).lowValue(-1).highValue(5);
        assertCalculate(expression("x % y"), xyStats(-5, 1, -6, -4)).lowValue(-5).highValue(1);
        assertCalculate(expression("x % y"), xyStats(-5, 5, -6, -4)).lowValue(-5).highValue(5);
        assertCalculate(expression("x % y"), xyStats(-5, 8, -6, -4)).lowValue(-5).highValue(6);
        assertCalculate(expression("x % y"), xyStats(-8, 5, -6, -4)).lowValue(-6).highValue(5);
        assertCalculate(expression("x % y"), xyStats(-8, 8, -6, -4)).lowValue(-6).highValue(6);
        assertCalculate(expression("x % y"), xyStats(-1, 1, -6, 4)).lowValue(-1).highValue(1);
        assertCalculate(expression("x % y"), xyStats(-1, 5, -6, 4)).lowValue(-1).highValue(5);
        assertCalculate(expression("x % y"), xyStats(-5, 1, -6, 4)).lowValue(-5).highValue(1);
        assertCalculate(expression("x % y"), xyStats(-5, 5, -6, 4)).lowValue(-5).highValue(5);
        assertCalculate(expression("x % y"), xyStats(-5, 8, -6, 4)).lowValue(-5).highValue(6);
        assertCalculate(expression("x % y"), xyStats(-8, 5, -6, 4)).lowValue(-6).highValue(5);
        assertCalculate(expression("x % y"), xyStats(-8, 8, -6, 4)).lowValue(-6).highValue(6);
        assertCalculate(expression("x % y"), xyStats(-1, 1, 4, 6)).lowValue(-1).highValue(1);
        assertCalculate(expression("x % y"), xyStats(-1, 5, 4, 6)).lowValue(-1).highValue(5);
        assertCalculate(expression("x % y"), xyStats(-5, 1, 4, 6)).lowValue(-5).highValue(1);
        assertCalculate(expression("x % y"), xyStats(-5, 5, 4, 6)).lowValue(-5).highValue(5);
        assertCalculate(expression("x % y"), xyStats(-5, 8, 4, 6)).lowValue(-5).highValue(6);
        assertCalculate(expression("x % y"), xyStats(-8, 5, 4, 6)).lowValue(-6).highValue(5);
        assertCalculate(expression("x % y"), xyStats(-8, 8, 4, 6)).lowValue(-6).highValue(6);
    }

    private PlanNodeStatsEstimate xyStats(double lowX, double highX, double lowY, double highY)
    {
        return PlanNodeStatsEstimate.builder()
                .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "x", BIGINT), VariableStatsEstimate.builder()
                        .setLowValue(lowX)
                        .setHighValue(highX)
                        .build())
                .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "y", BIGINT), VariableStatsEstimate.builder()
                        .setLowValue(lowY)
                        .setHighValue(highY)
                        .build())
                .build();
    }

    @Test
    public void testCoalesceExpression()
    {
        PlanNodeStatsEstimate relationStats = PlanNodeStatsEstimate.builder()
                .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "x", BIGINT), VariableStatsEstimate.builder()
                        .setLowValue(-1)
                        .setHighValue(10)
                        .setDistinctValuesCount(4)
                        .setNullsFraction(0.1)
                        .setAverageRowSize(2.0)
                        .build())
                .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "y", BIGINT), VariableStatsEstimate.builder()
                        .setLowValue(-2)
                        .setHighValue(5)
                        .setDistinctValuesCount(3)
                        .setNullsFraction(0.2)
                        .setAverageRowSize(2.0)
                        .build())
                .setOutputRowCount(10)
                .build();

        assertCalculate(expression("coalesce(x, y)"), relationStats)
                .distinctValuesCount(5)
                .lowValue(-2)
                .highValue(10)
                .nullsFraction(0.02)
                .averageRowSize(2.0);

        assertCalculate(expression("coalesce(y, x)"), relationStats)
                .distinctValuesCount(5)
                .lowValue(-2)
                .highValue(10)
                .nullsFraction(0.02)
                .averageRowSize(2.0);
    }

    private Expression expression(String sqlExpression)
    {
        return rewriteIdentifiersToSymbolReferences(sqlParser.createExpression(sqlExpression));
    }
}
