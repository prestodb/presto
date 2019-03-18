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
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.spi.block.IntArrayBlock;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.relational.optimizer.ExpressionOptimizer;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.block.BlockAssertions.toValues;
import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.metadata.CastType.JSON_TO_ARRAY_CAST;
import static com.facebook.presto.metadata.CastType.JSON_TO_MAP_CAST;
import static com.facebook.presto.metadata.CastType.JSON_TO_ROW_CAST;
import static com.facebook.presto.spi.function.OperatorType.ADD;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.type.JsonType.JSON;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertEquals;

public class TestExpressionOptimizer
{
    private TypeRegistry typeManager;
    private FunctionManager functionManager;
    private ExpressionOptimizer optimizer;

    @BeforeClass
    public void setUp()
    {
        typeManager = new TypeRegistry();
        functionManager = new FunctionManager(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig());
        optimizer = new ExpressionOptimizer(functionManager, typeManager, TEST_SESSION);
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
            FunctionHandle functionHandle = functionManager.resolveOperator(ADD, fromTypes(BIGINT, BIGINT));
            expression = new CallExpression(functionHandle, BIGINT, ImmutableList.of(expression, constant(1L, BIGINT)));
        }
        optimizer.optimize(expression);
    }

    @Test
    public void testIfConstantOptimization()
    {
        assertEquals(optimizer.optimize(ifExpression(constant(true, BOOLEAN), 1L, 2L)), constant(1L, BIGINT));
        assertEquals(optimizer.optimize(ifExpression(constant(false, BOOLEAN), 1L, 2L)), constant(2L, BIGINT));
        assertEquals(optimizer.optimize(ifExpression(constant(null, BOOLEAN), 1L, 2L)), constant(2L, BIGINT));

        FunctionHandle bigintEquals = functionManager.resolveOperator(EQUAL, fromTypes(BIGINT, BIGINT));
        RowExpression condition = new CallExpression(bigintEquals, BOOLEAN, ImmutableList.of(constant(3L, BIGINT), constant(3L, BIGINT)));
        assertEquals(optimizer.optimize(ifExpression(condition, 1L, 2L)), constant(1L, BIGINT));
    }

    @Test
    public void testCastWithJsonParseOptimization()
    {
        FunctionHandle jsonParseFunctionHandle = functionManager.lookupFunction(QualifiedName.of("json_parse"), fromTypes(VARCHAR));

        // constant
        FunctionHandle jsonCastFunctionHandle = functionManager.lookupCast(CAST, JSON.getTypeSignature(), parseTypeSignature("array(integer)"));
        RowExpression jsonCastExpression = new CallExpression(jsonCastFunctionHandle, new ArrayType(INTEGER), ImmutableList.of(call(jsonParseFunctionHandle, JSON, constant(utf8Slice("[1, 2]"), VARCHAR))));
        RowExpression resultExpression = optimizer.optimize(jsonCastExpression);
        assertInstanceOf(resultExpression, ConstantExpression.class);
        Object resultValue = ((ConstantExpression) resultExpression).getValue();
        assertInstanceOf(resultValue, IntArrayBlock.class);
        assertEquals(toValues(INTEGER, (IntArrayBlock) resultValue), ImmutableList.of(1, 2));

        // varchar to array
        jsonCastFunctionHandle = functionManager.lookupCast(CAST, JSON.getTypeSignature(), parseTypeSignature("array(varchar)"));
        jsonCastExpression = call(jsonCastFunctionHandle, new ArrayType(VARCHAR), ImmutableList.of(call(jsonParseFunctionHandle, JSON, field(1, VARCHAR))));
        resultExpression = optimizer.optimize(jsonCastExpression);
        assertEquals(
                resultExpression,
                call(functionManager.lookupCast(JSON_TO_ARRAY_CAST, VARCHAR.getTypeSignature(), parseTypeSignature("array(varchar)")), new ArrayType(VARCHAR), field(1, VARCHAR)));

        // varchar to map
        jsonCastFunctionHandle = functionManager.lookupCast(CAST, JSON.getTypeSignature(), parseTypeSignature("map(integer,varchar)"));
        jsonCastExpression = call(jsonCastFunctionHandle, mapType(INTEGER, VARCHAR), ImmutableList.of(call(jsonParseFunctionHandle, JSON, field(1, VARCHAR))));
        resultExpression = optimizer.optimize(jsonCastExpression);
        assertEquals(
                resultExpression,
                call(functionManager.lookupCast(JSON_TO_MAP_CAST, VARCHAR.getTypeSignature(), parseTypeSignature("map(integer, varchar)")), mapType(INTEGER, VARCHAR), field(1, VARCHAR)));

        // varchar to row
        jsonCastFunctionHandle = functionManager.lookupCast(CAST, JSON.getTypeSignature(), parseTypeSignature("row(varchar,bigint)"));
        jsonCastExpression = call(jsonCastFunctionHandle, RowType.anonymous(ImmutableList.of(VARCHAR, BIGINT)), ImmutableList.of(call(jsonParseFunctionHandle, JSON, field(1, VARCHAR))));
        resultExpression = optimizer.optimize(jsonCastExpression);
        assertEquals(
                resultExpression,
                call(functionManager.lookupCast(JSON_TO_ROW_CAST, VARCHAR.getTypeSignature(), parseTypeSignature("row(varchar,bigint)")), RowType.anonymous(ImmutableList.of(VARCHAR, BIGINT)), field(1, VARCHAR)));
    }

    private static RowExpression ifExpression(RowExpression condition, long trueValue, long falseValue)
    {
        return new SpecialFormExpression(IF, BIGINT, ImmutableList.of(condition, constant(trueValue, BIGINT), constant(falseValue, BIGINT)));
    }
}
