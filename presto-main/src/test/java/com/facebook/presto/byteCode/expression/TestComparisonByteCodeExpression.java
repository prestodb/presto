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
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantDouble;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantFloat;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantInt;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantLong;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantString;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.equal;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.greaterThan;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.greaterThanOrEqual;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.lessThan;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.lessThanOrEqual;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.notEqual;

public class TestComparisonByteCodeExpression
{
    @Test
    public void testLessThan()
            throws Exception
    {
        assertByteCodeExpression(lessThan(constantInt(3), constantInt(7)), 3 < 7, "(3 < 7)");
        assertByteCodeExpression(lessThan(constantInt(7), constantInt(3)), 7 < 3, "(7 < 3)");
        assertByteCodeExpression(lessThan(constantInt(7), constantInt(7)), 7 < 7, "(7 < 7)");

        assertByteCodeExpression(lessThan(constantLong(3L), constantLong(7L)), 3L < 7L, "(3L < 7L)");
        assertByteCodeExpression(lessThan(constantLong(7L), constantLong(3L)), 7L < 3L, "(7L < 3L)");
        assertByteCodeExpression(lessThan(constantLong(7L), constantLong(7L)), 7L < 7L, "(7L < 7L)");

        assertByteCodeExpression(lessThan(constantFloat(3.3f), constantFloat(7.7f)), 3.3f < 7.7f, "(3.3f < 7.7f)");
        assertByteCodeExpression(lessThan(constantFloat(7.7f), constantFloat(3.3f)), 7.7f < 3.3f, "(7.7f < 3.3f)");
        assertByteCodeExpression(lessThan(constantFloat(7.7f), constantFloat(7.7f)), 7.7f < 7.7f, "(7.7f < 7.7f)");
        assertByteCodeExpression(lessThan(constantFloat(Float.NaN), constantFloat(7.7f)), Float.NaN < 7.7f, "(NaNf < 7.7f)");
        assertByteCodeExpression(lessThan(constantFloat(7.7f), constantFloat(Float.NaN)), 7.7f < Float.NaN, "(7.7f < NaNf)");

        assertByteCodeExpression(lessThan(constantDouble(3.3), constantDouble(7.7)), 3.3 < 7.7, "(3.3 < 7.7)");
        assertByteCodeExpression(lessThan(constantDouble(7.7), constantDouble(3.3)), 7.7 < 3.3, "(7.7 < 3.3)");
        assertByteCodeExpression(lessThan(constantDouble(7.7), constantDouble(7.7)), 7.7 < 7.7, "(7.7 < 7.7)");
        assertByteCodeExpression(lessThan(constantDouble(Double.NaN), constantDouble(7.7)), Double.NaN < 7.7, "(NaN < 7.7)");
        assertByteCodeExpression(lessThan(constantDouble(7.7), constantDouble(Double.NaN)), 7.7 < Double.NaN, "(7.7 < NaN)");
    }

    @Test
    public void testGreaterThan()
            throws Exception
    {
        assertByteCodeExpression(greaterThan(constantInt(3), constantInt(7)), 3 > 7, "(3 > 7)");
        assertByteCodeExpression(greaterThan(constantInt(7), constantInt(3)), 7 > 3, "(7 > 3)");
        assertByteCodeExpression(greaterThan(constantInt(7), constantInt(7)), 7 > 7, "(7 > 7)");

        assertByteCodeExpression(greaterThan(constantLong(3L), constantLong(7L)), 3L > 7L, "(3L > 7L)");
        assertByteCodeExpression(greaterThan(constantLong(7L), constantLong(3L)), 7L > 3L, "(7L > 3L)");
        assertByteCodeExpression(greaterThan(constantLong(7L), constantLong(7L)), 7L > 7L, "(7L > 7L)");

        assertByteCodeExpression(greaterThan(constantFloat(3.3f), constantFloat(7.7f)), 3.3f > 7.7f, "(3.3f > 7.7f)");
        assertByteCodeExpression(greaterThan(constantFloat(7.7f), constantFloat(3.3f)), 7.7f > 3.3f, "(7.7f > 3.3f)");
        assertByteCodeExpression(greaterThan(constantFloat(7.7f), constantFloat(7.7f)), 7.7f > 7.7f, "(7.7f > 7.7f)");
        assertByteCodeExpression(greaterThan(constantFloat(Float.NaN), constantFloat(7.7f)), Float.NaN > 7.7f, "(NaNf > 7.7f)");
        assertByteCodeExpression(greaterThan(constantFloat(7.7f), constantFloat(Float.NaN)), 7.7f > Float.NaN, "(7.7f > NaNf)");

        assertByteCodeExpression(greaterThan(constantDouble(3.3), constantDouble(7.7)), 3.3 > 7.7, "(3.3 > 7.7)");
        assertByteCodeExpression(greaterThan(constantDouble(7.7), constantDouble(3.3)), 7.7 > 3.3, "(7.7 > 3.3)");
        assertByteCodeExpression(greaterThan(constantDouble(7.7), constantDouble(7.7)), 7.7 > 7.7, "(7.7 > 7.7)");
        assertByteCodeExpression(greaterThan(constantDouble(Double.NaN), constantDouble(7.7)), Double.NaN > 7.7, "(NaN > 7.7)");
        assertByteCodeExpression(greaterThan(constantDouble(7.7), constantDouble(Double.NaN)), 7.7 > Double.NaN, "(7.7 > NaN)");
    }

    @Test
    public void testLessThanOrEqual()
            throws Exception
    {
        assertByteCodeExpression(lessThanOrEqual(constantInt(3), constantInt(7)), 3 <= 7, "(3 <= 7)");
        assertByteCodeExpression(lessThanOrEqual(constantInt(7), constantInt(3)), 7 <= 3, "(7 <= 3)");
        assertByteCodeExpression(lessThanOrEqual(constantInt(7), constantInt(7)), 7 <= 7, "(7 <= 7)");

        assertByteCodeExpression(lessThanOrEqual(constantLong(3L), constantLong(7L)), 3L <= 7L, "(3L <= 7L)");
        assertByteCodeExpression(lessThanOrEqual(constantLong(7L), constantLong(3L)), 7L <= 3L, "(7L <= 3L)");
        assertByteCodeExpression(lessThanOrEqual(constantLong(7L), constantLong(7L)), 7L <= 7L, "(7L <= 7L)");

        assertByteCodeExpression(lessThanOrEqual(constantFloat(3.3f), constantFloat(7.7f)), 3.3f <= 7.7f, "(3.3f <= 7.7f)");
        assertByteCodeExpression(lessThanOrEqual(constantFloat(7.7f), constantFloat(3.3f)), 7.7f <= 3.3f, "(7.7f <= 3.3f)");
        assertByteCodeExpression(lessThanOrEqual(constantFloat(7.7f), constantFloat(7.7f)), 7.7f <= 7.7f, "(7.7f <= 7.7f)");
        assertByteCodeExpression(lessThanOrEqual(constantFloat(Float.NaN), constantFloat(7.7f)), Float.NaN <= 7.7f, "(NaNf <= 7.7f)");
        assertByteCodeExpression(lessThanOrEqual(constantFloat(7.7f), constantFloat(Float.NaN)), 7.7f <= Float.NaN, "(7.7f <= NaNf)");

        assertByteCodeExpression(lessThanOrEqual(constantDouble(3.3), constantDouble(7.7)), 3.3 <= 7.7, "(3.3 <= 7.7)");
        assertByteCodeExpression(lessThanOrEqual(constantDouble(7.7), constantDouble(3.3)), 7.7 <= 3.3, "(7.7 <= 3.3)");
        assertByteCodeExpression(lessThanOrEqual(constantDouble(7.7), constantDouble(7.7)), 7.7 <= 7.7, "(7.7 <= 7.7)");
        assertByteCodeExpression(lessThanOrEqual(constantDouble(Double.NaN), constantDouble(7.7)), Double.NaN <= 7.7, "(NaN <= 7.7)");
        assertByteCodeExpression(lessThanOrEqual(constantDouble(7.7), constantDouble(Double.NaN)), 7.7 <= Double.NaN, "(7.7 <= NaN)");
    }

    @Test
    public void testGreaterThanOrEqual()
            throws Exception
    {
        assertByteCodeExpression(greaterThanOrEqual(constantInt(3), constantInt(7)), 3 >= 7, "(3 >= 7)");
        assertByteCodeExpression(greaterThanOrEqual(constantInt(7), constantInt(3)), 7 >= 3, "(7 >= 3)");
        assertByteCodeExpression(greaterThanOrEqual(constantInt(7), constantInt(7)), 7 >= 7, "(7 >= 7)");

        assertByteCodeExpression(greaterThanOrEqual(constantLong(3L), constantLong(7L)), 3L >= 7L, "(3L >= 7L)");
        assertByteCodeExpression(greaterThanOrEqual(constantLong(7L), constantLong(3L)), 7L >= 3L, "(7L >= 3L)");
        assertByteCodeExpression(greaterThanOrEqual(constantLong(7L), constantLong(7L)), 7L >= 7L, "(7L >= 7L)");

        assertByteCodeExpression(greaterThanOrEqual(constantFloat(3.3f), constantFloat(7.7f)), 3.3f >= 7.7f, "(3.3f >= 7.7f)");
        assertByteCodeExpression(greaterThanOrEqual(constantFloat(7.7f), constantFloat(3.3f)), 7.7f >= 3.3f, "(7.7f >= 3.3f)");
        assertByteCodeExpression(greaterThanOrEqual(constantFloat(7.7f), constantFloat(7.7f)), 7.7f >= 7.7f, "(7.7f >= 7.7f)");
        assertByteCodeExpression(greaterThanOrEqual(constantFloat(Float.NaN), constantFloat(7.7f)), Float.NaN >= 7.7f, "(NaNf >= 7.7f)");
        assertByteCodeExpression(greaterThanOrEqual(constantFloat(7.7f), constantFloat(Float.NaN)), 7.7f >= Float.NaN, "(7.7f >= NaNf)");

        assertByteCodeExpression(greaterThanOrEqual(constantDouble(3.3), constantDouble(7.7)), 3.3 >= 7.7, "(3.3 >= 7.7)");
        assertByteCodeExpression(greaterThanOrEqual(constantDouble(7.7), constantDouble(3.3)), 7.7 >= 3.3, "(7.7 >= 3.3)");
        assertByteCodeExpression(greaterThanOrEqual(constantDouble(7.7), constantDouble(7.7)), 7.7 >= 7.7, "(7.7 >= 7.7)");
        assertByteCodeExpression(greaterThanOrEqual(constantDouble(Double.NaN), constantDouble(7.7)), Double.NaN >= 7.7, "(NaN >= 7.7)");
        assertByteCodeExpression(greaterThanOrEqual(constantDouble(7.7), constantDouble(Double.NaN)), 7.7 >= Double.NaN, "(7.7 >= NaN)");
    }

    @SuppressWarnings({"FloatingPointEquality", "ComparisonToNaN", "EqualsNaN", "EqualsWithItself"})
    @Test
    public void testEqual()
            throws Exception
    {
        assertByteCodeExpression(equal(constantInt(7), constantInt(3)), 7 == 3, "(7 == 3)");
        assertByteCodeExpression(equal(constantInt(7), constantInt(7)), 7 == 7, "(7 == 7)");

        assertByteCodeExpression(equal(constantLong(7L), constantLong(3L)), 7L == 3L, "(7L == 3L)");
        assertByteCodeExpression(equal(constantLong(7L), constantLong(7L)), 7L == 7L, "(7L == 7L)");

        assertByteCodeExpression(equal(constantFloat(7.7f), constantFloat(3.3f)), 7.7f == 3.3f, "(7.7f == 3.3f)");
        assertByteCodeExpression(equal(constantFloat(7.7f), constantFloat(7.7f)), 7.7f == 7.7f, "(7.7f == 7.7f)");
        assertByteCodeExpression(equal(constantFloat(Float.NaN), constantFloat(7.7f)), Float.NaN == 7.7f, "(NaNf == 7.7f)");
        assertByteCodeExpression(equal(constantFloat(Float.NaN), constantFloat(Float.NaN)), Float.NaN == Float.NaN, "(NaNf == NaNf)");

        assertByteCodeExpression(equal(constantDouble(7.7), constantDouble(3.3)), 7.7 == 3.3, "(7.7 == 3.3)");
        assertByteCodeExpression(equal(constantDouble(7.7), constantDouble(7.7)), 7.7 == 7.7, "(7.7 == 7.7)");
        assertByteCodeExpression(equal(constantDouble(Double.NaN), constantDouble(7.7)), Double.NaN == 7.7, "(NaN == 7.7)");
        assertByteCodeExpression(equal(constantDouble(7.7), constantDouble(Double.NaN)), 7.7 == Double.NaN, "(7.7 == NaN)");
        assertByteCodeExpression(equal(constantDouble(Double.NaN), constantDouble(Double.NaN)), Double.NaN == Double.NaN, "(NaN == NaN)");

        // the byte code is verifying with == but that breaks check style so we use
        assertByteCodeExpression(equal(constantString("foo"), constantString("bar")), "foo".equals("bar"), "(\"foo\" == \"bar\")");
        assertByteCodeExpression(equal(constantString("foo"), constantString("foo")), "foo".equals("foo"), "(\"foo\" == \"foo\")");
    }

    @SuppressWarnings({"FloatingPointEquality", "ComparisonToNaN", "EqualsNaN", "EqualsWithItself"})
    @Test
    public void testNotEqual()
            throws Exception
    {
        assertByteCodeExpression(notEqual(constantInt(7), constantInt(3)), 7 != 3, "(7 != 3)");
        assertByteCodeExpression(notEqual(constantInt(7), constantInt(7)), 7 != 7, "(7 != 7)");

        assertByteCodeExpression(notEqual(constantLong(7L), constantLong(3L)), 7L != 3L, "(7L != 3L)");
        assertByteCodeExpression(notEqual(constantLong(7L), constantLong(7L)), 7L != 7L, "(7L != 7L)");

        assertByteCodeExpression(notEqual(constantFloat(7.7f), constantFloat(3.3f)), 7.7f != 3.3f, "(7.7f != 3.3f)");
        assertByteCodeExpression(notEqual(constantFloat(7.7f), constantFloat(7.7f)), 7.7f != 7.7f, "(7.7f != 7.7f)");
        assertByteCodeExpression(notEqual(constantFloat(Float.NaN), constantFloat(7.7f)), Float.NaN != 7.7f, "(NaNf != 7.7f)");
        assertByteCodeExpression(notEqual(constantFloat(Float.NaN), constantFloat(Float.NaN)), Float.NaN != Float.NaN, "(NaNf != NaNf)");

        assertByteCodeExpression(notEqual(constantDouble(7.7), constantDouble(3.3)), 7.7 != 3.3, "(7.7 != 3.3)");
        assertByteCodeExpression(notEqual(constantDouble(7.7), constantDouble(7.7)), 7.7 != 7.7, "(7.7 != 7.7)");
        assertByteCodeExpression(notEqual(constantDouble(Double.NaN), constantDouble(7.7)), Double.NaN != 7.7, "(NaN != 7.7)");
        assertByteCodeExpression(notEqual(constantDouble(7.7), constantDouble(Double.NaN)), 7.7 != Double.NaN, "(7.7 != NaN)");
        assertByteCodeExpression(notEqual(constantDouble(Double.NaN), constantDouble(Double.NaN)), Double.NaN != Double.NaN, "(NaN != NaN)");

        // the byte code is verifying with != but that breaks check style so we use
        assertByteCodeExpression(notEqual(constantString("foo"), constantString("bar")), !"foo".equals("bar"), "(\"foo\" != \"bar\")");
        assertByteCodeExpression(notEqual(constantString("foo"), constantString("foo")), !"foo".equals("foo"), "(\"foo\" != \"foo\")");
    }
}
