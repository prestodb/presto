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

import static com.facebook.presto.bytecode.expression.BytecodeExpressionAssertions.assertByteCodeExpression;
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
        assertByteCodeExpression(add(constantInt(3), constantInt(7)), 3 + 7, "(3 + 7)");
        assertByteCodeExpression(add(constantLong(3), constantLong(7)), 3L + 7L, "(3L + 7L)");
        assertByteCodeExpression(add(constantFloat(3.1f), constantFloat(7.5f)), 3.1f + 7.5f, "(3.1f + 7.5f)");
        assertByteCodeExpression(add(constantDouble(3.1), constantDouble(7.5)), 3.1 + 7.5, "(3.1 + 7.5)");
    }

    @Test
    public void testSubtract()
            throws Exception
    {
        assertByteCodeExpression(subtract(constantInt(3), constantInt(7)), 3 - 7, "(3 - 7)");
        assertByteCodeExpression(subtract(constantLong(3), constantLong(7)), 3L - 7L, "(3L - 7L)");
        assertByteCodeExpression(subtract(constantFloat(3.1f), constantFloat(7.5f)), 3.1f - 7.5f, "(3.1f - 7.5f)");
        assertByteCodeExpression(subtract(constantDouble(3.1), constantDouble(7.5)), 3.1 - 7.5, "(3.1 - 7.5)");
    }

    @Test
    public void testMultiply()
            throws Exception
    {
        assertByteCodeExpression(multiply(constantInt(3), constantInt(7)), 3 * 7, "(3 * 7)");
        assertByteCodeExpression(multiply(constantLong(3), constantLong(7)), 3L * 7L, "(3L * 7L)");
        assertByteCodeExpression(multiply(constantFloat(3.1f), constantFloat(7.5f)), 3.1f * 7.5f, "(3.1f * 7.5f)");
        assertByteCodeExpression(multiply(constantDouble(3.1), constantDouble(7.5)), 3.1 * 7.5, "(3.1 * 7.5)");
    }

    @Test
    public void testDivide()
            throws Exception
    {
        assertByteCodeExpression(divide(constantInt(7), constantInt(3)), 7 / 3, "(7 / 3)");
        assertByteCodeExpression(divide(constantLong(7), constantLong(3)), 7L / 3L, "(7L / 3L)");
        assertByteCodeExpression(divide(constantFloat(3.1f), constantFloat(7.5f)), 3.1f / 7.5f, "(3.1f / 7.5f)");
        assertByteCodeExpression(divide(constantDouble(3.1), constantDouble(7.5)), 3.1 / 7.5, "(3.1 / 7.5)");
    }

    @Test
    public void testRemainder()
            throws Exception
    {
        assertByteCodeExpression(remainder(constantInt(7), constantInt(3)), 7 % 3, "(7 % 3)");
        assertByteCodeExpression(remainder(constantLong(7), constantLong(3)), 7L % 3L, "(7L % 3L)");
        assertByteCodeExpression(remainder(constantFloat(3.1f), constantFloat(7.5f)), 3.1f % 7.5f, "(3.1f % 7.5f)");
        assertByteCodeExpression(remainder(constantDouble(3.1), constantDouble(7.5)), 3.1 % 7.5, "(3.1 % 7.5)");
    }

    @Test
    public void testShiftLeft()
            throws Exception
    {
        assertByteCodeExpression(shiftLeft(constantInt(7), constantInt(3)), 7 << 3, "(7 << 3)");
        assertByteCodeExpression(shiftLeft(constantLong(7), constantInt(3)), 7L << 3, "(7L << 3)");
    }

    @Test
    public void testShiftRight()
            throws Exception
    {
        assertByteCodeExpression(shiftRight(constantInt(-7), constantInt(3)), -7 >> 3, "(-7 >> 3)");
        assertByteCodeExpression(shiftRight(constantLong(-7), constantInt(3)), -7L >> 3, "(-7L >> 3)");
    }

    @Test
    public void testShiftRightUnsigned()
            throws Exception
    {
        assertByteCodeExpression(shiftRightUnsigned(constantInt(-7), constantInt(3)), -7 >>> 3, "(-7 >>> 3)");
        assertByteCodeExpression(shiftRightUnsigned(constantLong(-7), constantInt(3)), -7L >>> 3, "(-7L >>> 3)");
    }

    @Test
    public void testBitwiseAnd()
            throws Exception
    {
        assertByteCodeExpression(bitwiseAnd(constantInt(101), constantInt(37)), 101 & 37, "(101 & 37)");
        assertByteCodeExpression(bitwiseAnd(constantLong(101), constantLong(37)), 101L & 37L, "(101L & 37L)");
    }

    @Test
    public void testBitwiseOr()
            throws Exception
    {
        assertByteCodeExpression(bitwiseOr(constantInt(101), constantInt(37)), 101 | 37, "(101 | 37)");
        assertByteCodeExpression(bitwiseOr(constantLong(101), constantLong(37)), 101L | 37L, "(101L | 37L)");
    }

    @Test
    public void testBitwiseXor()
            throws Exception
    {
        assertByteCodeExpression(bitwiseXor(constantInt(101), constantInt(37)), 101 ^ 37, "(101 ^ 37)");
        assertByteCodeExpression(bitwiseXor(constantLong(101), constantLong(37)), 101L ^ 37L, "(101L ^ 37L)");
    }

    @Test
    public void testNegate()
            throws Exception
    {
        assertByteCodeExpression(negate(constantInt(3)), -3, "-(3)");
        assertByteCodeExpression(negate(constantLong(3)), -3L, "-(3L)");
        assertByteCodeExpression(negate(constantFloat(3.1f)), -3.1f, "-(3.1f)");
        assertByteCodeExpression(negate(constantDouble(3.1)), -3.1, "-(3.1)");
        assertByteCodeExpression(negate(constantInt(-3)), -(-3), "-(-3)");
        assertByteCodeExpression(negate(constantLong(-3)), -(-3L), "-(-3L)");
        assertByteCodeExpression(negate(constantFloat(-3.1f)), -(-3.1f), "-(-3.1f)");
        assertByteCodeExpression(negate(constantDouble(-3.1)), -(-3.1), "-(-3.1)");
    }
}
