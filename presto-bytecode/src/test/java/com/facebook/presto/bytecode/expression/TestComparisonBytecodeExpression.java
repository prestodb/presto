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
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantDouble;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFloat;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantLong;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantString;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.equal;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.greaterThan;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.greaterThanOrEqual;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.lessThan;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.lessThanOrEqual;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.notEqual;

public class TestComparisonBytecodeExpression
{
    @Test
    public void testLessThan()
            throws Exception
    {
        assertBytecodeExpression(lessThan(constantInt(3), constantInt(7)), 3 < 7, "(3 < 7)");
        assertBytecodeExpression(lessThan(constantInt(7), constantInt(3)), 7 < 3, "(7 < 3)");
        assertBytecodeExpression(lessThan(constantInt(7), constantInt(7)), 7 < 7, "(7 < 7)");

        assertBytecodeExpression(lessThan(constantLong(3L), constantLong(7L)), 3L < 7L, "(3L < 7L)");
        assertBytecodeExpression(lessThan(constantLong(7L), constantLong(3L)), 7L < 3L, "(7L < 3L)");
        assertBytecodeExpression(lessThan(constantLong(7L), constantLong(7L)), 7L < 7L, "(7L < 7L)");

        assertBytecodeExpression(lessThan(constantFloat(3.3f), constantFloat(7.7f)), 3.3f < 7.7f, "(3.3f < 7.7f)");
        assertBytecodeExpression(lessThan(constantFloat(7.7f), constantFloat(3.3f)), 7.7f < 3.3f, "(7.7f < 3.3f)");
        assertBytecodeExpression(lessThan(constantFloat(7.7f), constantFloat(7.7f)), 7.7f < 7.7f, "(7.7f < 7.7f)");
        assertBytecodeExpression(lessThan(constantFloat(Float.NaN), constantFloat(7.7f)), Float.NaN < 7.7f, "(NaNf < 7.7f)");
        assertBytecodeExpression(lessThan(constantFloat(7.7f), constantFloat(Float.NaN)), 7.7f < Float.NaN, "(7.7f < NaNf)");

        assertBytecodeExpression(lessThan(constantDouble(3.3), constantDouble(7.7)), 3.3 < 7.7, "(3.3 < 7.7)");
        assertBytecodeExpression(lessThan(constantDouble(7.7), constantDouble(3.3)), 7.7 < 3.3, "(7.7 < 3.3)");
        assertBytecodeExpression(lessThan(constantDouble(7.7), constantDouble(7.7)), 7.7 < 7.7, "(7.7 < 7.7)");
        assertBytecodeExpression(lessThan(constantDouble(Double.NaN), constantDouble(7.7)), Double.NaN < 7.7, "(NaN < 7.7)");
        assertBytecodeExpression(lessThan(constantDouble(7.7), constantDouble(Double.NaN)), 7.7 < Double.NaN, "(7.7 < NaN)");
    }

    @Test
    public void testGreaterThan()
            throws Exception
    {
        assertBytecodeExpression(greaterThan(constantInt(3), constantInt(7)), 3 > 7, "(3 > 7)");
        assertBytecodeExpression(greaterThan(constantInt(7), constantInt(3)), 7 > 3, "(7 > 3)");
        assertBytecodeExpression(greaterThan(constantInt(7), constantInt(7)), 7 > 7, "(7 > 7)");

        assertBytecodeExpression(greaterThan(constantLong(3L), constantLong(7L)), 3L > 7L, "(3L > 7L)");
        assertBytecodeExpression(greaterThan(constantLong(7L), constantLong(3L)), 7L > 3L, "(7L > 3L)");
        assertBytecodeExpression(greaterThan(constantLong(7L), constantLong(7L)), 7L > 7L, "(7L > 7L)");

        assertBytecodeExpression(greaterThan(constantFloat(3.3f), constantFloat(7.7f)), 3.3f > 7.7f, "(3.3f > 7.7f)");
        assertBytecodeExpression(greaterThan(constantFloat(7.7f), constantFloat(3.3f)), 7.7f > 3.3f, "(7.7f > 3.3f)");
        assertBytecodeExpression(greaterThan(constantFloat(7.7f), constantFloat(7.7f)), 7.7f > 7.7f, "(7.7f > 7.7f)");
        assertBytecodeExpression(greaterThan(constantFloat(Float.NaN), constantFloat(7.7f)), Float.NaN > 7.7f, "(NaNf > 7.7f)");
        assertBytecodeExpression(greaterThan(constantFloat(7.7f), constantFloat(Float.NaN)), 7.7f > Float.NaN, "(7.7f > NaNf)");

        assertBytecodeExpression(greaterThan(constantDouble(3.3), constantDouble(7.7)), 3.3 > 7.7, "(3.3 > 7.7)");
        assertBytecodeExpression(greaterThan(constantDouble(7.7), constantDouble(3.3)), 7.7 > 3.3, "(7.7 > 3.3)");
        assertBytecodeExpression(greaterThan(constantDouble(7.7), constantDouble(7.7)), 7.7 > 7.7, "(7.7 > 7.7)");
        assertBytecodeExpression(greaterThan(constantDouble(Double.NaN), constantDouble(7.7)), Double.NaN > 7.7, "(NaN > 7.7)");
        assertBytecodeExpression(greaterThan(constantDouble(7.7), constantDouble(Double.NaN)), 7.7 > Double.NaN, "(7.7 > NaN)");
    }

    @Test
    public void testLessThanOrEqual()
            throws Exception
    {
        assertBytecodeExpression(lessThanOrEqual(constantInt(3), constantInt(7)), 3 <= 7, "(3 <= 7)");
        assertBytecodeExpression(lessThanOrEqual(constantInt(7), constantInt(3)), 7 <= 3, "(7 <= 3)");
        assertBytecodeExpression(lessThanOrEqual(constantInt(7), constantInt(7)), 7 <= 7, "(7 <= 7)");

        assertBytecodeExpression(lessThanOrEqual(constantLong(3L), constantLong(7L)), 3L <= 7L, "(3L <= 7L)");
        assertBytecodeExpression(lessThanOrEqual(constantLong(7L), constantLong(3L)), 7L <= 3L, "(7L <= 3L)");
        assertBytecodeExpression(lessThanOrEqual(constantLong(7L), constantLong(7L)), 7L <= 7L, "(7L <= 7L)");

        assertBytecodeExpression(lessThanOrEqual(constantFloat(3.3f), constantFloat(7.7f)), 3.3f <= 7.7f, "(3.3f <= 7.7f)");
        assertBytecodeExpression(lessThanOrEqual(constantFloat(7.7f), constantFloat(3.3f)), 7.7f <= 3.3f, "(7.7f <= 3.3f)");
        assertBytecodeExpression(lessThanOrEqual(constantFloat(7.7f), constantFloat(7.7f)), 7.7f <= 7.7f, "(7.7f <= 7.7f)");
        assertBytecodeExpression(lessThanOrEqual(constantFloat(Float.NaN), constantFloat(7.7f)), Float.NaN <= 7.7f, "(NaNf <= 7.7f)");
        assertBytecodeExpression(lessThanOrEqual(constantFloat(7.7f), constantFloat(Float.NaN)), 7.7f <= Float.NaN, "(7.7f <= NaNf)");

        assertBytecodeExpression(lessThanOrEqual(constantDouble(3.3), constantDouble(7.7)), 3.3 <= 7.7, "(3.3 <= 7.7)");
        assertBytecodeExpression(lessThanOrEqual(constantDouble(7.7), constantDouble(3.3)), 7.7 <= 3.3, "(7.7 <= 3.3)");
        assertBytecodeExpression(lessThanOrEqual(constantDouble(7.7), constantDouble(7.7)), 7.7 <= 7.7, "(7.7 <= 7.7)");
        assertBytecodeExpression(lessThanOrEqual(constantDouble(Double.NaN), constantDouble(7.7)), Double.NaN <= 7.7, "(NaN <= 7.7)");
        assertBytecodeExpression(lessThanOrEqual(constantDouble(7.7), constantDouble(Double.NaN)), 7.7 <= Double.NaN, "(7.7 <= NaN)");
    }

    @Test
    public void testGreaterThanOrEqual()
            throws Exception
    {
        assertBytecodeExpression(greaterThanOrEqual(constantInt(3), constantInt(7)), 3 >= 7, "(3 >= 7)");
        assertBytecodeExpression(greaterThanOrEqual(constantInt(7), constantInt(3)), 7 >= 3, "(7 >= 3)");
        assertBytecodeExpression(greaterThanOrEqual(constantInt(7), constantInt(7)), 7 >= 7, "(7 >= 7)");

        assertBytecodeExpression(greaterThanOrEqual(constantLong(3L), constantLong(7L)), 3L >= 7L, "(3L >= 7L)");
        assertBytecodeExpression(greaterThanOrEqual(constantLong(7L), constantLong(3L)), 7L >= 3L, "(7L >= 3L)");
        assertBytecodeExpression(greaterThanOrEqual(constantLong(7L), constantLong(7L)), 7L >= 7L, "(7L >= 7L)");

        assertBytecodeExpression(greaterThanOrEqual(constantFloat(3.3f), constantFloat(7.7f)), 3.3f >= 7.7f, "(3.3f >= 7.7f)");
        assertBytecodeExpression(greaterThanOrEqual(constantFloat(7.7f), constantFloat(3.3f)), 7.7f >= 3.3f, "(7.7f >= 3.3f)");
        assertBytecodeExpression(greaterThanOrEqual(constantFloat(7.7f), constantFloat(7.7f)), 7.7f >= 7.7f, "(7.7f >= 7.7f)");
        assertBytecodeExpression(greaterThanOrEqual(constantFloat(Float.NaN), constantFloat(7.7f)), Float.NaN >= 7.7f, "(NaNf >= 7.7f)");
        assertBytecodeExpression(greaterThanOrEqual(constantFloat(7.7f), constantFloat(Float.NaN)), 7.7f >= Float.NaN, "(7.7f >= NaNf)");

        assertBytecodeExpression(greaterThanOrEqual(constantDouble(3.3), constantDouble(7.7)), 3.3 >= 7.7, "(3.3 >= 7.7)");
        assertBytecodeExpression(greaterThanOrEqual(constantDouble(7.7), constantDouble(3.3)), 7.7 >= 3.3, "(7.7 >= 3.3)");
        assertBytecodeExpression(greaterThanOrEqual(constantDouble(7.7), constantDouble(7.7)), 7.7 >= 7.7, "(7.7 >= 7.7)");
        assertBytecodeExpression(greaterThanOrEqual(constantDouble(Double.NaN), constantDouble(7.7)), Double.NaN >= 7.7, "(NaN >= 7.7)");
        assertBytecodeExpression(greaterThanOrEqual(constantDouble(7.7), constantDouble(Double.NaN)), 7.7 >= Double.NaN, "(7.7 >= NaN)");
    }

    @SuppressWarnings({"FloatingPointEquality", "ComparisonToNaN", "EqualsNaN", "EqualsWithItself"})
    @Test
    public void testEqual()
            throws Exception
    {
        assertBytecodeExpression(equal(constantInt(7), constantInt(3)), 7 == 3, "(7 == 3)");
        assertBytecodeExpression(equal(constantInt(7), constantInt(7)), 7 == 7, "(7 == 7)");

        assertBytecodeExpression(equal(constantLong(7L), constantLong(3L)), 7L == 3L, "(7L == 3L)");
        assertBytecodeExpression(equal(constantLong(7L), constantLong(7L)), 7L == 7L, "(7L == 7L)");

        assertBytecodeExpression(equal(constantFloat(7.7f), constantFloat(3.3f)), 7.7f == 3.3f, "(7.7f == 3.3f)");
        assertBytecodeExpression(equal(constantFloat(7.7f), constantFloat(7.7f)), 7.7f == 7.7f, "(7.7f == 7.7f)");
        assertBytecodeExpression(equal(constantFloat(Float.NaN), constantFloat(7.7f)), Float.NaN == 7.7f, "(NaNf == 7.7f)");
        assertBytecodeExpression(equal(constantFloat(Float.NaN), constantFloat(Float.NaN)), Float.NaN == Float.NaN, "(NaNf == NaNf)");

        assertBytecodeExpression(equal(constantDouble(7.7), constantDouble(3.3)), 7.7 == 3.3, "(7.7 == 3.3)");
        assertBytecodeExpression(equal(constantDouble(7.7), constantDouble(7.7)), 7.7 == 7.7, "(7.7 == 7.7)");
        assertBytecodeExpression(equal(constantDouble(Double.NaN), constantDouble(7.7)), Double.NaN == 7.7, "(NaN == 7.7)");
        assertBytecodeExpression(equal(constantDouble(7.7), constantDouble(Double.NaN)), 7.7 == Double.NaN, "(7.7 == NaN)");
        assertBytecodeExpression(equal(constantDouble(Double.NaN), constantDouble(Double.NaN)), Double.NaN == Double.NaN, "(NaN == NaN)");

        // the byte code is verifying with == but that breaks check style so we use
        assertBytecodeExpression(equal(constantString("foo"), constantString("bar")), "foo".equals("bar"), "(\"foo\" == \"bar\")");
        assertBytecodeExpression(equal(constantString("foo"), constantString("foo")), "foo".equals("foo"), "(\"foo\" == \"foo\")");
    }

    @SuppressWarnings({"FloatingPointEquality", "ComparisonToNaN", "EqualsNaN", "EqualsWithItself"})
    @Test
    public void testNotEqual()
            throws Exception
    {
        assertBytecodeExpression(notEqual(constantInt(7), constantInt(3)), 7 != 3, "(7 != 3)");
        assertBytecodeExpression(notEqual(constantInt(7), constantInt(7)), 7 != 7, "(7 != 7)");

        assertBytecodeExpression(notEqual(constantLong(7L), constantLong(3L)), 7L != 3L, "(7L != 3L)");
        assertBytecodeExpression(notEqual(constantLong(7L), constantLong(7L)), 7L != 7L, "(7L != 7L)");

        assertBytecodeExpression(notEqual(constantFloat(7.7f), constantFloat(3.3f)), 7.7f != 3.3f, "(7.7f != 3.3f)");
        assertBytecodeExpression(notEqual(constantFloat(7.7f), constantFloat(7.7f)), 7.7f != 7.7f, "(7.7f != 7.7f)");
        assertBytecodeExpression(notEqual(constantFloat(Float.NaN), constantFloat(7.7f)), Float.NaN != 7.7f, "(NaNf != 7.7f)");
        assertBytecodeExpression(notEqual(constantFloat(Float.NaN), constantFloat(Float.NaN)), Float.NaN != Float.NaN, "(NaNf != NaNf)");

        assertBytecodeExpression(notEqual(constantDouble(7.7), constantDouble(3.3)), 7.7 != 3.3, "(7.7 != 3.3)");
        assertBytecodeExpression(notEqual(constantDouble(7.7), constantDouble(7.7)), 7.7 != 7.7, "(7.7 != 7.7)");
        assertBytecodeExpression(notEqual(constantDouble(Double.NaN), constantDouble(7.7)), Double.NaN != 7.7, "(NaN != 7.7)");
        assertBytecodeExpression(notEqual(constantDouble(7.7), constantDouble(Double.NaN)), 7.7 != Double.NaN, "(7.7 != NaN)");
        assertBytecodeExpression(notEqual(constantDouble(Double.NaN), constantDouble(Double.NaN)), Double.NaN != Double.NaN, "(NaN != NaN)");

        // the byte code is verifying with != but that breaks check style so we use
        assertBytecodeExpression(notEqual(constantString("foo"), constantString("bar")), !"foo".equals("bar"), "(\"foo\" != \"bar\")");
        assertBytecodeExpression(notEqual(constantString("foo"), constantString("foo")), !"foo".equals("foo"), "(\"foo\" != \"foo\")");
    }
}
