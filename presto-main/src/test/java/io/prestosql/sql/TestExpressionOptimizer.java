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
package io.prestosql.sql;

import com.google.common.collect.ImmutableList;
import io.prestosql.block.BlockEncodingManager;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.Signature;
import io.prestosql.spi.block.IntArrayBlock;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.relational.CallExpression;
import io.prestosql.sql.relational.ConstantExpression;
import io.prestosql.sql.relational.RowExpression;
import io.prestosql.sql.relational.optimizer.ExpressionOptimizer;
import io.prestosql.type.TypeRegistry;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.block.BlockAssertions.toValues;
import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.metadata.Signature.internalOperator;
import static io.prestosql.metadata.Signature.internalScalarFunction;
import static io.prestosql.operator.scalar.JsonStringToArrayCast.JSON_STRING_TO_ARRAY_NAME;
import static io.prestosql.operator.scalar.JsonStringToMapCast.JSON_STRING_TO_MAP_NAME;
import static io.prestosql.operator.scalar.JsonStringToRowCast.JSON_STRING_TO_ROW_NAME;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.relational.Expressions.call;
import static io.prestosql.sql.relational.Expressions.constant;
import static io.prestosql.sql.relational.Expressions.field;
import static io.prestosql.sql.relational.Signatures.CAST;
import static io.prestosql.type.JsonType.JSON;
import static io.prestosql.util.StructuralTestUtil.mapType;
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
