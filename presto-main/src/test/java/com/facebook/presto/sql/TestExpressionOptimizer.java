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

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.block.IntArrayBlock;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.optimizer.ExpressionOptimizer;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.block.BlockAssertions.toValues;
import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.metadata.Signature.internalScalarFunction;
import static com.facebook.presto.operator.scalar.JsonStringToArrayCast.JSON_STRING_TO_ARRAY_NAME;
import static com.facebook.presto.operator.scalar.JsonStringToMapCast.JSON_STRING_TO_MAP_NAME;
import static com.facebook.presto.operator.scalar.JsonStringToRowCast.JSON_STRING_TO_ROW_NAME;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.sql.relational.Signatures.CAST;
import static com.facebook.presto.type.JsonType.JSON;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertEquals;

public class TestExpressionOptimizer
{
    private TypeRegistry typeManager;
    private ExpressionOptimizer optimizer;

    @BeforeClass
    public void setUp()
    {
        typeManager = new TypeRegistry();
        optimizer = new ExpressionOptimizer(new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig()), typeManager, TEST_SESSION);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        typeManager = null;
        optimizer = null;
    }

    @Test(timeOut = 10_000)
    public void testPossibleExponentialOptimizationTime()
    {
        RowExpression expression = constant(1L, BIGINT);
        for (int i = 0; i < 100; i++) {
            Signature signature = internalOperator(OperatorType.ADD.name(), parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.BIGINT));
            expression = new CallExpression(signature, BIGINT, ImmutableList.of(expression, constant(1L, BIGINT)));
        }
        optimizer.optimize(expression);
    }

    @Test
    public void testIfConstantOptimization()
    {
        assertEquals(optimizer.optimize(ifExpression(constant(true, BOOLEAN), 1L, 2L)), constant(1L, BIGINT));
        assertEquals(optimizer.optimize(ifExpression(constant(false, BOOLEAN), 1L, 2L)), constant(2L, BIGINT));
        assertEquals(optimizer.optimize(ifExpression(constant(null, BOOLEAN), 1L, 2L)), constant(2L, BIGINT));

        Signature bigintEquals = internalOperator(OperatorType.EQUAL.name(), BOOLEAN.getTypeSignature(), BIGINT.getTypeSignature(), BIGINT.getTypeSignature());
        RowExpression condition = new CallExpression(bigintEquals, BOOLEAN, ImmutableList.of(constant(3L, BIGINT), constant(3L, BIGINT)));
        assertEquals(optimizer.optimize(ifExpression(condition, 1L, 2L)), constant(1L, BIGINT));
    }

    @Test
    public void testCastWithJsonParseOptimization()
    {
        Signature jsonParseSignature = new Signature("json_parse", SCALAR, JSON.getTypeSignature(), ImmutableList.of(VARCHAR.getTypeSignature()));

        // constant
        Signature jsonCastSignature = new Signature(CAST, SCALAR, parseTypeSignature("array(integer)"), ImmutableList.of(JSON.getTypeSignature()));
        RowExpression jsonCastExpression = new CallExpression(jsonCastSignature, new ArrayType(INTEGER), ImmutableList.of(call(jsonParseSignature, JSON, constant(utf8Slice("[1, 2]"), VARCHAR))));
        RowExpression resultExpression = optimizer.optimize(jsonCastExpression);
        assertInstanceOf(resultExpression, ConstantExpression.class);
        Object resultValue = ((ConstantExpression) resultExpression).getValue();
        assertInstanceOf(resultValue, IntArrayBlock.class);
        assertEquals(toValues(INTEGER, (IntArrayBlock) resultValue), ImmutableList.of(1, 2));

        // varchar to array
        jsonCastSignature = new Signature(CAST, SCALAR, parseTypeSignature("array(varchar)"), ImmutableList.of(JSON.getTypeSignature()));
        jsonCastExpression = new CallExpression(jsonCastSignature, new ArrayType(VARCHAR), ImmutableList.of(call(jsonParseSignature, JSON, field(1, VARCHAR))));
        resultExpression = optimizer.optimize(jsonCastExpression);
        assertEquals(
                resultExpression,
                call(internalScalarFunction(JSON_STRING_TO_ARRAY_NAME, parseTypeSignature("array(varchar)"), parseTypeSignature(StandardTypes.VARCHAR)), new ArrayType(VARCHAR), field(1, VARCHAR)));

        // varchar to map
        jsonCastSignature = new Signature(CAST, SCALAR, parseTypeSignature("map(integer,varchar)"), ImmutableList.of(JSON.getTypeSignature()));
        jsonCastExpression = new CallExpression(jsonCastSignature, mapType(INTEGER, VARCHAR), ImmutableList.of(call(jsonParseSignature, JSON, field(1, VARCHAR))));
        resultExpression = optimizer.optimize(jsonCastExpression);
        assertEquals(
                resultExpression,
                call(internalScalarFunction(JSON_STRING_TO_MAP_NAME, parseTypeSignature("map(integer,varchar)"), parseTypeSignature(StandardTypes.VARCHAR)), mapType(INTEGER, VARCHAR), field(1, VARCHAR)));

        // varchar to row
        jsonCastSignature = new Signature(CAST, SCALAR, parseTypeSignature("row(varchar,bigint)"), ImmutableList.of(JSON.getTypeSignature()));
        jsonCastExpression = new CallExpression(jsonCastSignature, RowType.anonymous(ImmutableList.of(VARCHAR, BIGINT)), ImmutableList.of(call(jsonParseSignature, JSON, field(1, VARCHAR))));
        resultExpression = optimizer.optimize(jsonCastExpression);
        assertEquals(
                resultExpression,
                call(internalScalarFunction(JSON_STRING_TO_ROW_NAME, parseTypeSignature("row(varchar,bigint)"), parseTypeSignature(StandardTypes.VARCHAR)), RowType.anonymous(ImmutableList.of(VARCHAR, BIGINT)), field(1, VARCHAR)));
    }

    private static RowExpression ifExpression(RowExpression condition, long trueValue, long falseValue)
    {
        Signature signature = new Signature("IF", SCALAR, BIGINT.getTypeSignature(), BOOLEAN.getTypeSignature(), BIGINT.getTypeSignature(), BIGINT.getTypeSignature());
        return new CallExpression(signature, BIGINT, ImmutableList.of(condition, constant(trueValue, BIGINT), constant(falseValue, BIGINT)));
    }
}
