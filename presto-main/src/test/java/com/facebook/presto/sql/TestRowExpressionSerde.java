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
package com.facebook.presto.sql;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.stats.cardinality.HyperLogLog;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncoding;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.block.IntArrayBlock;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.slice.Slice;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.function.OperatorType.SUBSCRIPT;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.ROW_CONSTRUCTOR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.lang.Float.floatToIntBits;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestRowExpressionSerde
{
    private final Metadata metadata = MetadataManager.createTestMetadataManager();
    private JsonCodec<RowExpression> codec;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        codec = getJsonCodec();
    }

    @Test
    public void testSimpleLiteral()
    {
        assertLiteral("TRUE", constant(true, BOOLEAN));
        assertLiteral("FALSE", constant(false, BOOLEAN));
        assertLiteral("CAST(NULL AS BOOLEAN)", constant(null, BOOLEAN));

        assertLiteral("TINYINT '1'", constant(1L, TINYINT));
        assertLiteral("SMALLINT '1'", constant(1L, SMALLINT));
        assertLiteral("1", constant(1L, INTEGER));
        assertLiteral("BIGINT '1'", constant(1L, BIGINT));

        assertLiteral("1.1", constant(1.1, DOUBLE));
        assertLiteral("nan()", constant(Double.NaN, DOUBLE));
        assertLiteral("infinity()", constant(Double.POSITIVE_INFINITY, DOUBLE));
        assertLiteral("-infinity()", constant(Double.NEGATIVE_INFINITY, DOUBLE));

        assertLiteral("CAST(1.1 AS REAL)", constant((long) floatToIntBits(1.1f), REAL));
        assertLiteral("CAST(nan() AS REAL)", constant((long) floatToIntBits(Float.NaN), REAL));
        assertLiteral("CAST(infinity() AS REAL)", constant((long) floatToIntBits(Float.POSITIVE_INFINITY), REAL));
        assertLiteral("CAST(-infinity() AS REAL)", constant((long) floatToIntBits(Float.NEGATIVE_INFINITY), REAL));

        assertStringLiteral("'String Literal'", "String Literal", VarcharType.createVarcharType(14));
        assertLiteral("CAST(NULL AS VARCHAR)", constant(null, VARCHAR));

        assertLiteral("DATE '1991-01-01'", constant(7670L, DATE));
        assertLiteral("TIMESTAMP '1991-01-01 00:00:00.000'", constant(662727600000L, TIMESTAMP));
    }

    @Test
    public void testArrayLiteral()
    {
        RowExpression rowExpression = getRoundTrip("ARRAY [1, 2, 3]", true);
        assertTrue(rowExpression instanceof ConstantExpression);
        Object value = ((ConstantExpression) rowExpression).getValue();
        assertTrue(value instanceof IntArrayBlock);
        IntArrayBlock block = (IntArrayBlock) value;
        assertEquals(block.getPositionCount(), 3);
        assertEquals(block.getInt(0), 1);
        assertEquals(block.getInt(1), 2);
        assertEquals(block.getInt(2), 3);
    }

    @Test
    public void testArrayGet()
    {
        assertEquals(getRoundTrip("(ARRAY [1, 2, 3])[1]", false),
                call(SUBSCRIPT.name(),
                        operator(SUBSCRIPT, new ArrayType(INTEGER), BIGINT),
                        INTEGER,
                        call("array_constructor",
                                function("array_constructor", INTEGER, INTEGER, INTEGER),
                                new ArrayType(INTEGER),
                                constant(1L, INTEGER),
                                constant(2L, INTEGER),
                                constant(3L, INTEGER)),
                        constant(1L, INTEGER)));
        assertEquals(getRoundTrip("(ARRAY [1, 2, 3])[1]", true), constant(1L, INTEGER));
    }

    @Test
    public void testRowLiteral()
    {
        assertEquals(getRoundTrip("ROW(1, 1.1)", false),
                specialForm(
                        ROW_CONSTRUCTOR,
                        RowType.anonymous(
                                ImmutableList.of(
                                        INTEGER,
                                        DOUBLE)),
                        constant(1L, INTEGER),
                        constant(1.1, DOUBLE)));
    }

    @Test
    public void testDereference()
    {
        String sql = "CAST(ROW(1) AS ROW(col1 integer)).col1";
        RowExpression before = translate(expression(sql, new ParsingOptions(AS_DOUBLE)), false);
        RowExpression after = getRoundTrip(sql, false);
        assertEquals(before, after);
    }

    @Test
    public void testHllLiteral()
    {
        RowExpression rowExpression = getRoundTrip("empty_approx_set()", true);
        assertTrue(rowExpression instanceof ConstantExpression);
        Object value = ((ConstantExpression) rowExpression).getValue();
        assertEquals(HyperLogLog.newInstance((Slice) value).cardinality(), 0);
    }

    @Test
    public void testUnserializableType()
    {
        assertThrowsWhenSerialize("CAST('$.a' AS JsonPath)", true);
    }

    private void assertThrowsWhenSerialize(@Language("SQL") String sql, boolean optimize)
    {
        RowExpression rowExpression = translate(expression(sql, new ParsingOptions(AS_DOUBLE)), optimize);
        assertThrows(IllegalArgumentException.class, () -> codec.toJson(rowExpression));
    }

    private void assertLiteral(@Language("SQL") String sql, ConstantExpression expected)
    {
        assertEquals(getRoundTrip(sql, true), expected);
    }

    private void assertStringLiteral(@Language("SQL") String sql, String expectedString, Type expectedType)
    {
        RowExpression roundTrip = getRoundTrip(sql, true);
        assertTrue(roundTrip instanceof ConstantExpression);
        String roundTripValue = ((Slice) ((ConstantExpression) roundTrip).getValue()).toStringUtf8();
        Type roundTripType = roundTrip.getType();
        assertEquals(roundTripValue, expectedString);
        assertEquals(roundTripType, expectedType);
    }

    private RowExpression getRoundTrip(String sql, boolean optimize)
    {
        RowExpression rowExpression = translate(expression(sql, new ParsingOptions(AS_DOUBLE)), optimize);
        String json = codec.toJson(rowExpression);
        return codec.fromJson(json);
    }

    private FunctionHandle operator(OperatorType operatorType, Type... types)
    {
        return metadata.getFunctionAndTypeManager().resolveOperator(operatorType, fromTypes(types));
    }

    private FunctionHandle function(String name, Type... types)
    {
        return metadata.getFunctionAndTypeManager().lookupFunction(name, fromTypes(types));
    }

    private JsonCodec<RowExpression> getJsonCodec()
            throws Exception
    {
        Module module = binder -> {
            binder.install(new JsonModule());
            binder.install(new HandleJsonModule());
            configBinder(binder).bindConfig(FeaturesConfig.class);

            FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
            binder.bind(TypeManager.class).toInstance(functionAndTypeManager);
            jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
            newSetBinder(binder, Type.class);

            binder.bind(BlockEncodingSerde.class).to(BlockEncodingManager.class).in(Scopes.SINGLETON);
            newSetBinder(binder, BlockEncoding.class);
            jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
            jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);
            jsonCodecBinder(binder).bindJsonCodec(RowExpression.class);
        };
        Bootstrap app = new Bootstrap(ImmutableList.of(module));
        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        return injector.getInstance(new Key<JsonCodec<RowExpression>>() {});
    }

    private RowExpression translate(Expression expression, boolean optimize)
    {
        RowExpression rowExpression = SqlToRowExpressionTranslator.translate(expression, getExpressionTypes(expression), ImmutableMap.of(), metadata.getFunctionAndTypeManager(), TEST_SESSION);
        if (optimize) {
            RowExpressionOptimizer optimizer = new RowExpressionOptimizer(metadata);
            return optimizer.optimize(rowExpression, OPTIMIZED, TEST_SESSION.toConnectorSession());
        }
        return rowExpression;
    }

    private Map<NodeRef<Expression>, Type> getExpressionTypes(Expression expression)
    {
        ExpressionAnalyzer expressionAnalyzer = ExpressionAnalyzer.createWithoutSubqueries(
                metadata.getFunctionAndTypeManager(),
                TEST_SESSION,
                TypeProvider.empty(),
                emptyList(),
                node -> new IllegalStateException("Unexpected node: %s" + node),
                WarningCollector.NOOP,
                false);
        expressionAnalyzer.analyze(expression, Scope.create());
        return expressionAnalyzer.getExpressionTypes();
    }
}
