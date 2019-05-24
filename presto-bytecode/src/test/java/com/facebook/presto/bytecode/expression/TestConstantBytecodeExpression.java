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

import java.util.List;

import static com.facebook.presto.bytecode.expression.BytecodeExpressionAssertions.assertBytecodeExpression;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantBoolean;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantClass;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantDouble;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFloat;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantLong;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantNull;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantString;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantTrue;

public class TestConstantBytecodeExpression
{
    @Test
    public void test()
            throws Exception
    {
        assertBytecodeExpression(constantNull(List.class), null, "null");

        assertBytecodeExpression(constantTrue(), true, "true");
        assertBytecodeExpression(constantFalse(), false, "false");
        assertBytecodeExpression(constantBoolean(true), true, "true");
        assertBytecodeExpression(constantBoolean(false), false, "false");

        assertBytecodeExpression(constantInt(0), 0, "0");
        assertBytecodeExpression(constantInt(Integer.MAX_VALUE), Integer.MAX_VALUE, String.valueOf(Integer.MAX_VALUE));
        assertBytecodeExpression(constantInt(Integer.MIN_VALUE), Integer.MIN_VALUE, String.valueOf(Integer.MIN_VALUE));

        assertBytecodeExpression(constantLong(0L), 0L, "0L");
        assertBytecodeExpression(constantLong(Long.MAX_VALUE), Long.MAX_VALUE, Long.MAX_VALUE + "L");
        assertBytecodeExpression(constantLong(Long.MIN_VALUE), Long.MIN_VALUE, Long.MIN_VALUE + "L");

        assertBytecodeExpression(constantFloat(0.0f), 0.0f, "0.0f");
        assertBytecodeExpression(constantFloat(Float.MAX_VALUE), Float.MAX_VALUE, Float.MAX_VALUE + "f");
        assertBytecodeExpression(constantFloat(Float.MIN_VALUE), Float.MIN_VALUE, Float.MIN_VALUE + "f");
        assertBytecodeExpression(constantFloat(Float.NaN), Float.NaN, "NaNf");

        assertBytecodeExpression(constantDouble(0.0), 0.0, "0.0");
        assertBytecodeExpression(constantDouble(Double.MAX_VALUE), Double.MAX_VALUE, String.valueOf(Double.MAX_VALUE));
        assertBytecodeExpression(constantDouble(Double.MIN_VALUE), Double.MIN_VALUE, String.valueOf(Double.MIN_VALUE));
        assertBytecodeExpression(constantDouble(Double.NaN), Double.NaN, "NaN");

        assertBytecodeExpression(constantString(""), "", "\"\"");
        assertBytecodeExpression(constantString("foo"), "foo", "\"foo\"");

        assertBytecodeExpression(constantClass(List.class), List.class, "List.class");

        assertBytecodeExpression(constantClass(boolean.class), boolean.class, "boolean.class");
        assertBytecodeExpression(constantClass(byte.class), byte.class, "byte.class");
        assertBytecodeExpression(constantClass(char.class), char.class, "char.class");
        assertBytecodeExpression(constantClass(double.class), double.class, "double.class");
        assertBytecodeExpression(constantClass(float.class), float.class, "float.class");
        assertBytecodeExpression(constantClass(int.class), int.class, "int.class");
        assertBytecodeExpression(constantClass(long.class), long.class, "long.class");
        assertBytecodeExpression(constantClass(short.class), short.class, "short.class");
        assertBytecodeExpression(constantClass(void.class), void.class, "void.class");
    }
}
