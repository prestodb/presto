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
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.add;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.bitwiseAnd;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.bitwiseOr;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.bitwiseXor;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantDouble;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantFloat;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantInt;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantLong;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.divide;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.multiply;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.negate;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.remainder;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.shiftLeft;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.shiftRight;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.shiftRightUnsigned;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.subtract;

public class TestArithmeticByteCodeExpression
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
