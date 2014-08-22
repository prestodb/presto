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
package com.facebook.presto.byteCode.expression;

import org.testng.annotations.Test;

import static com.facebook.presto.byteCode.expression.ByteCodeExpressionAssertions.assertByteCodeExpression;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.and;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantFalse;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantTrue;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.not;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.or;

public class TestLogicalByteCodeExpression
{
    @Test
    public void testAnd()
            throws Exception
    {
        assertByteCodeExpression(and(constantTrue(), constantTrue()), true && true, "(true && true)");
        assertByteCodeExpression(and(constantTrue(), constantFalse()), true && false, "(true && false)");
        assertByteCodeExpression(and(constantFalse(), constantTrue()), false && true, "(false && true)");
        assertByteCodeExpression(and(constantFalse(), constantFalse()), false && false, "(false && false)");
    }

    @Test
    public void testOr()
            throws Exception
    {
        assertByteCodeExpression(or(constantTrue(), constantTrue()), true || true, "(true || true)");
        assertByteCodeExpression(or(constantTrue(), constantFalse()), true || false, "(true || false)");
        assertByteCodeExpression(or(constantFalse(), constantTrue()), false || true, "(false || true)");
        assertByteCodeExpression(or(constantFalse(), constantFalse()), false || false, "(false || false)");
    }

    @Test
    public void testNot()
            throws Exception
    {
        assertByteCodeExpression(not(constantTrue()), !true, "(!true)");
        assertByteCodeExpression(not(constantFalse()), !false, "(!false)");
    }
}
