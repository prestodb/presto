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

import java.util.List;

import static com.facebook.presto.byteCode.expression.ByteCodeExpressionAssertions.assertByteCodeExpression;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantBoolean;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantClass;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantDouble;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantFalse;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantFloat;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantInt;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantLong;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantNull;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantString;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantTrue;

public class TestConstantByteCodeExpression
{
    @Test
    public void test()
            throws Exception
    {
        assertByteCodeExpression(constantNull(List.class), null, "null");

        assertByteCodeExpression(constantTrue(), true, "true");
        assertByteCodeExpression(constantFalse(), false, "false");
        assertByteCodeExpression(constantBoolean(true), true, "true");
        assertByteCodeExpression(constantBoolean(false), false, "false");

        assertByteCodeExpression(constantInt(0), 0, "0");
        assertByteCodeExpression(constantInt(Integer.MAX_VALUE), Integer.MAX_VALUE, String.valueOf(Integer.MAX_VALUE));
        assertByteCodeExpression(constantInt(Integer.MIN_VALUE), Integer.MIN_VALUE, String.valueOf(Integer.MIN_VALUE));

        assertByteCodeExpression(constantLong(0L), 0L, "0L");
        assertByteCodeExpression(constantLong(Long.MAX_VALUE), Long.MAX_VALUE, Long.MAX_VALUE + "L");
        assertByteCodeExpression(constantLong(Long.MIN_VALUE), Long.MIN_VALUE, Long.MIN_VALUE + "L");

        assertByteCodeExpression(constantFloat(0.0f), 0.0f, "0.0f");
        assertByteCodeExpression(constantFloat(Float.MAX_VALUE), Float.MAX_VALUE, Float.MAX_VALUE + "f");
        assertByteCodeExpression(constantFloat(Float.MIN_VALUE), Float.MIN_VALUE, Float.MIN_VALUE + "f");
        assertByteCodeExpression(constantFloat(Float.NaN), Float.NaN, "NaNf");

        assertByteCodeExpression(constantDouble(0.0), 0.0, "0.0");
        assertByteCodeExpression(constantDouble(Double.MAX_VALUE), Double.MAX_VALUE, String.valueOf(Double.MAX_VALUE));
        assertByteCodeExpression(constantDouble(Double.MIN_VALUE), Double.MIN_VALUE, String.valueOf(Double.MIN_VALUE));
        assertByteCodeExpression(constantDouble(Double.NaN), Double.NaN, "NaN");

        assertByteCodeExpression(constantString(""), "", "\"\"");
        assertByteCodeExpression(constantString("foo"), "foo", "\"foo\"");

        assertByteCodeExpression(constantClass(List.class), List.class, "List.class");

        assertByteCodeExpression(constantClass(boolean.class), boolean.class, "boolean.class");
        assertByteCodeExpression(constantClass(byte.class), byte.class, "byte.class");
        assertByteCodeExpression(constantClass(char.class), char.class, "char.class");
        assertByteCodeExpression(constantClass(double.class), double.class, "double.class");
        assertByteCodeExpression(constantClass(float.class), float.class, "float.class");
        assertByteCodeExpression(constantClass(int.class), int.class, "int.class");
        assertByteCodeExpression(constantClass(long.class), long.class, "long.class");
        assertByteCodeExpression(constantClass(short.class), short.class, "short.class");
        assertByteCodeExpression(constantClass(void.class), void.class, "void.class");
    }
}
