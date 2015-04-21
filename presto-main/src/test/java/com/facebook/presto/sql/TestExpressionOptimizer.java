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
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.optimizer.ExpressionOptimizer;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;

public class TestExpressionOptimizer
{
    @Test(timeOut = 10_000)
    public void testPossibleExponentialOptimizationTime()
    {
        TypeRegistry typeManager = new TypeRegistry();
        ExpressionOptimizer optimizer = new ExpressionOptimizer(new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), false), typeManager, TEST_SESSION);
        RowExpression expression = new ConstantExpression(1L, BIGINT);
        for (int i = 0; i < 100; i++) {
            Signature signature = Signature.internalOperator(OperatorType.ADD.name(), parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.BIGINT));
            expression = new CallExpression(signature, BIGINT, ImmutableList.of(expression, new ConstantExpression(1L, BIGINT)));
        }
        optimizer.optimize(expression);
    }
}
