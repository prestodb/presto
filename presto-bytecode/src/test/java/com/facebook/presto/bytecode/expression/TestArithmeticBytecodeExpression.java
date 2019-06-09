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
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.add;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.bitwiseAnd;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.bitwiseOr;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.bitwiseXor;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantDouble;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFloat;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantLong;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.divide;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.multiply;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.negate;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.remainder;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.shiftLeft;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.shiftRight;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.shiftRightUnsigned;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.subtract;

public class TestArithmeticBytecodeExpression
{
    @Test
    public void testAdd()
            throws Exception
    {
        assertBytecodeExpression(add(constantInt(3), constantInt(7)), 3 + 7, "(3 + 7)");
        assertBytecodeExpression(add(constantLong(3), constantLong(7)), 3L + 7L, "(3L + 7L)");
        assertBytecodeExpression(add(constantFloat(3.1f), constantFloat(7.5f)), 3.1f + 7.5f, "(3.1f + 7.5f)");
        assertBytecodeExpression(add(constantDouble(3.1), constantDouble(7.5)), 3.1 + 7.5, "(3.1 + 7.5)");
    }

    @Test
    public void testSubtract()
            throws Exception
    {
        assertBytecodeExpression(subtract(constantInt(3), constantInt(7)), 3 - 7, "(3 - 7)");
        assertBytecodeExpression(subtract(constantLong(3), constantLong(7)), 3L - 7L, "(3L - 7L)");
        assertBytecodeExpression(subtract(constantFloat(3.1f), constantFloat(7.5f)), 3.1f - 7.5f, "(3.1f - 7.5f)");
        assertBytecodeExpression(subtract(constantDouble(3.1), constantDouble(7.5)), 3.1 - 7.5, "(3.1 - 7.5)");
    }

    @Test
    public void testMultiply()
            throws Exception
    {
        assertBytecodeExpression(multiply(constantInt(3), constantInt(7)), 3 * 7, "(3 * 7)");
        assertBytecodeExpression(multiply(constantLong(3), constantLong(7)), 3L * 7L, "(3L * 7L)");
        assertBytecodeExpression(multiply(constantFloat(3.1f), constantFloat(7.5f)), 3.1f * 7.5f, "(3.1f * 7.5f)");
        assertBytecodeExpression(multiply(constantDouble(3.1), constantDouble(7.5)), 3.1 * 7.5, "(3.1 * 7.5)");
    }

    @Test
    public void testDivide()
            throws Exception
    {
        assertBytecodeExpression(divide(constantInt(7), constantInt(3)), 7 / 3, "(7 / 3)");
        assertBytecodeExpression(divide(constantLong(7), constantLong(3)), 7L / 3L, "(7L / 3L)");
        assertBytecodeExpression(divide(constantFloat(3.1f), constantFloat(7.5f)), 3.1f / 7.5f, "(3.1f / 7.5f)");
        assertBytecodeExpression(divide(constantDouble(3.1), constantDouble(7.5)), 3.1 / 7.5, "(3.1 / 7.5)");
    }

    @Test
    public void testRemainder()
            throws Exception
    {
        assertBytecodeExpression(remainder(constantInt(7), constantInt(3)), 7 % 3, "(7 % 3)");
        assertBytecodeExpression(remainder(constantLong(7), constantLong(3)), 7L % 3L, "(7L % 3L)");
        assertBytecodeExpression(remainder(constantFloat(3.1f), constantFloat(7.5f)), 3.1f % 7.5f, "(3.1f % 7.5f)");
        assertBytecodeExpression(remainder(constantDouble(3.1), constantDouble(7.5)), 3.1 % 7.5, "(3.1 % 7.5)");
    }

    @Test
    public void testShiftLeft()
            throws Exception
    {
        assertBytecodeExpression(shiftLeft(constantInt(7), constantInt(3)), 7 << 3, "(7 << 3)");
        assertBytecodeExpression(shiftLeft(constantLong(7), constantInt(3)), 7L << 3, "(7L << 3)");
    }

    @Test
    public void testShiftRight()
            throws Exception
    {
        assertBytecodeExpression(shiftRight(constantInt(-7), constantInt(3)), -7 >> 3, "(-7 >> 3)");
        assertBytecodeExpression(shiftRight(constantLong(-7), constantInt(3)), -7L >> 3, "(-7L >> 3)");
    }

    @Test
    public void testShiftRightUnsigned()
            throws Exception
    {
        assertBytecodeExpression(shiftRightUnsigned(constantInt(-7), constantInt(3)), -7 >>> 3, "(-7 >>> 3)");
        assertBytecodeExpression(shiftRightUnsigned(constantLong(-7), constantInt(3)), -7L >>> 3, "(-7L >>> 3)");
    }

    @Test
    public void testBitwiseAnd()
            throws Exception
    {
        assertBytecodeExpression(bitwiseAnd(constantInt(101), constantInt(37)), 101 & 37, "(101 & 37)");
        assertBytecodeExpression(bitwiseAnd(constantLong(101), constantLong(37)), 101L & 37L, "(101L & 37L)");
    }

    @Test
    public void testBitwiseOr()
            throws Exception
    {
        assertBytecodeExpression(bitwiseOr(constantInt(101), constantInt(37)), 101 | 37, "(101 | 37)");
        assertBytecodeExpression(bitwiseOr(constantLong(101), constantLong(37)), 101L | 37L, "(101L | 37L)");
    }

    @Test
    public void testBitwiseXor()
            throws Exception
    {
        assertBytecodeExpression(bitwiseXor(constantInt(101), constantInt(37)), 101 ^ 37, "(101 ^ 37)");
        assertBytecodeExpression(bitwiseXor(constantLong(101), constantLong(37)), 101L ^ 37L, "(101L ^ 37L)");
    }

    @Test
    public void testNegate()
            throws Exception
    {
        assertBytecodeExpression(negate(constantInt(3)), -3, "-(3)");
        assertBytecodeExpression(negate(constantLong(3)), -3L, "-(3L)");
        assertBytecodeExpression(negate(constantFloat(3.1f)), -3.1f, "-(3.1f)");
        assertBytecodeExpression(negate(constantDouble(3.1)), -3.1, "-(3.1)");
        assertBytecodeExpression(negate(constantInt(-3)), -(-3), "-(-3)");
        assertBytecodeExpression(negate(constantLong(-3)), -(-3L), "-(-3L)");
        assertBytecodeExpression(negate(constantFloat(-3.1f)), -(-3.1f), "-(-3.1f)");
        assertBytecodeExpression(negate(constantDouble(-3.1)), -(-3.1), "-(-3.1)");
    }
}
