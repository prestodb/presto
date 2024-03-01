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
package com.facebook.presto.bytecode.expression;

import org.testng.annotations.Test;

import static com.facebook.presto.bytecode.expression.BytecodeExpressionAssertions.assertBytecodeExpression;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.and;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantTrue;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.not;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.or;

public class TestLogicalBytecodeExpression
{
    @Test
    public void testAnd()
            throws Exception
    {
        assertBytecodeExpression(and(constantTrue(), constantTrue()), true && true, "(true && true)");
        assertBytecodeExpression(and(constantTrue(), constantFalse()), true && false, "(true && false)");
        assertBytecodeExpression(and(constantFalse(), constantTrue()), false && true, "(false && true)");
        assertBytecodeExpression(and(constantFalse(), constantFalse()), false && false, "(false && false)");
    }

    @Test
    public void testOr()
            throws Exception
    {
        assertBytecodeExpression(or(constantTrue(), constantTrue()), true || true, "(true || true)");
        assertBytecodeExpression(or(constantTrue(), constantFalse()), true || false, "(true || false)");
        assertBytecodeExpression(or(constantFalse(), constantTrue()), false || true, "(false || true)");
        assertBytecodeExpression(or(constantFalse(), constantFalse()), false || false, "(false || false)");
    }

    @Test
    public void testNot()
            throws Exception
    {
        assertBytecodeExpression(not(constantTrue()), !true, "(!true)");
        assertBytecodeExpression(not(constantFalse()), !false, "(!false)");
    }
}
