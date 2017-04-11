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
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.optimizer.ExpressionOptimizer;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static org.testng.Assert.assertEquals;

public class TestExpressionOptimizer
{
    @Test(timeOut = 10_000)
    public void testPossibleExponentialOptimizationTime()
    {
        TypeRegistry typeManager = new TypeRegistry();
        ExpressionOptimizer optimizer = new ExpressionOptimizer(new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig()), typeManager, TEST_SESSION);
        RowExpression expression = new ConstantExpression(1L, BIGINT);
        for (int i = 0; i < 100; i++) {
            Signature signature = internalOperator(OperatorType.ADD.name(), parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.BIGINT));
            expression = new CallExpression(signature, BIGINT, ImmutableList.of(expression, new ConstantExpression(1L, BIGINT)));
        }
        optimizer.optimize(expression);
    }

    @Test
    public void testTryOptimization()
    {
        TypeRegistry typeManager = new TypeRegistry();
        ExpressionOptimizer optimizer = new ExpressionOptimizer(new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig()), typeManager, TEST_SESSION);
        Signature signature = new Signature("TRY", SCALAR, BIGINT.getTypeSignature());

        RowExpression tryExpression =  new CallExpression(signature, BIGINT, ImmutableList.of(new ConstantExpression(1L, BIGINT)));
        assertEquals(optimizer.optimize(tryExpression), new ConstantExpression(1L, BIGINT));

        tryExpression =  new CallExpression(signature, BIGINT, ImmutableList.of(new InputReferenceExpression(1, BIGINT)));
        assertEquals(optimizer.optimize(tryExpression), new InputReferenceExpression(1, BIGINT));
    }

    @Test
    public void testIfConstantOptimization()
    {
        TypeRegistry typeManager = new TypeRegistry();
        ExpressionOptimizer optimizer = new ExpressionOptimizer(new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig()), typeManager, TEST_SESSION);

        assertEquals(optimizer.optimize(ifExpression(new ConstantExpression(true, BOOLEAN), 1L, 2L)), new ConstantExpression(1L, BIGINT));
        assertEquals(optimizer.optimize(ifExpression(new ConstantExpression(false, BOOLEAN), 1L, 2L)), new ConstantExpression(2L, BIGINT));
        assertEquals(optimizer.optimize(ifExpression(new ConstantExpression(null, BOOLEAN), 1L, 2L)), new ConstantExpression(2L, BIGINT));

        Signature bigintEquals = internalOperator(OperatorType.EQUAL.name(), BOOLEAN.getTypeSignature(), BIGINT.getTypeSignature(), BIGINT.getTypeSignature());
        RowExpression condition = new CallExpression(bigintEquals, BOOLEAN, ImmutableList.of(new ConstantExpression(3L, BIGINT), new ConstantExpression(3L, BIGINT)));
        assertEquals(optimizer.optimize(ifExpression(condition, 1L, 2L)), new ConstantExpression(1L, BIGINT));
    }

    private static RowExpression ifExpression(RowExpression condition, long trueValue, long falseValue)
    {
        Signature signature = new Signature("IF", SCALAR, BIGINT.getTypeSignature(), BOOLEAN.getTypeSignature(), BIGINT.getTypeSignature(), BIGINT.getTypeSignature());
        return new CallExpression(signature, BIGINT, ImmutableList.of(condition, new ConstantExpression(trueValue, BIGINT), new ConstantExpression(falseValue, BIGINT)));
    }
}
