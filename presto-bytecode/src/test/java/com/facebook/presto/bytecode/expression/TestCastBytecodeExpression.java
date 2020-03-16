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

import com.google.common.primitives.Primitives;
import org.testng.annotations.Test;

import static com.facebook.presto.bytecode.expression.BytecodeExpressionAssertions.assertBytecodeExpression;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.getStatic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestCastBytecodeExpression
{
    public static final Object OBJECT_FIELD = "foo";
    public static final boolean BOOLEAN_FIELD = true;
    public static final byte BYTE_FIELD = 99;
    public static final char CHAR_FIELD = 11;
    public static final short SHORT_FIELD = 22;
    public static final int INT_FIELD = 33;
    public static final long LONG_FIELD = 44;
    public static final float FLOAT_FIELD = 3.3f;
    public static final double DOUBLE_FIELD = 4.4;

    @Test
    public void testDownCastObject()
            throws Exception
    {
        assertBytecodeExpression(getStatic(getClass(), "OBJECT_FIELD").cast(String.class).invoke("length", int.class),
                ((String) OBJECT_FIELD).length(),
                "((String) " + getClass().getSimpleName() + ".OBJECT_FIELD).length()");
    }

    @Test
    public void testCastBetweenObjectAndPrimitive()
            throws Exception
    {
        assertCast(getStatic(getClass(), "INT_FIELD"), 33, Object.class);
        assertCast(getStatic(getClass(), "INT_FIELD").cast(Object.class), 33, int.class);
    }

    @Test
    public void testInvalildCast()
    {
        // Cast between a boxed primitive and a primitive that are different
        assertInvalidCast(getStatic(getClass(), "INT_FIELD"), Double.class);
        assertInvalidCast(getStatic(getClass(), "INT_FIELD").cast(Integer.class), double.class);

        // Cast between two different boxed primitives
        assertInvalidCast(getStatic(getClass(), "INT_FIELD").cast(Integer.class), Double.class);

        // Cast between a primitive and an object (that is not java.lang.Object)
        assertInvalidCast(getStatic(getClass(), "OBJECT_FIELD").cast(String.class), double.class);
        assertInvalidCast(getStatic(getClass(), "INT_FIELD"), String.class);
    }

    @Test
    public void testCastPrimitive()
            throws Exception
    {
        assertPrimitiveCast("BOOLEAN_FIELD", boolean.class, BOOLEAN_FIELD);

        assertPrimitiveCast("BYTE_FIELD", byte.class, BYTE_FIELD);
        assertPrimitiveCast("BYTE_FIELD", char.class, (char) BYTE_FIELD);
        assertPrimitiveCast("BYTE_FIELD", short.class, (short) BYTE_FIELD);
        assertPrimitiveCast("BYTE_FIELD", int.class, (int) BYTE_FIELD);
        assertPrimitiveCast("BYTE_FIELD", long.class, (long) BYTE_FIELD);
        assertPrimitiveCast("BYTE_FIELD", float.class, (float) BYTE_FIELD);
        assertPrimitiveCast("BYTE_FIELD", double.class, (double) BYTE_FIELD);

        assertPrimitiveCast("CHAR_FIELD", byte.class, (byte) CHAR_FIELD);
        assertPrimitiveCast("CHAR_FIELD", char.class, CHAR_FIELD);
        assertPrimitiveCast("CHAR_FIELD", short.class, (short) CHAR_FIELD);
        assertPrimitiveCast("CHAR_FIELD", int.class, (int) CHAR_FIELD);
        assertPrimitiveCast("CHAR_FIELD", long.class, (long) CHAR_FIELD);
        assertPrimitiveCast("CHAR_FIELD", float.class, (float) CHAR_FIELD);
        assertPrimitiveCast("CHAR_FIELD", double.class, (double) CHAR_FIELD);

        assertPrimitiveCast("SHORT_FIELD", byte.class, (byte) SHORT_FIELD);
        assertPrimitiveCast("SHORT_FIELD", char.class, (char) SHORT_FIELD);
        assertPrimitiveCast("SHORT_FIELD", short.class, SHORT_FIELD);
        assertPrimitiveCast("SHORT_FIELD", int.class, (int) SHORT_FIELD);
        assertPrimitiveCast("SHORT_FIELD", long.class, (long) SHORT_FIELD);
        assertPrimitiveCast("SHORT_FIELD", float.class, (float) SHORT_FIELD);
        assertPrimitiveCast("SHORT_FIELD", double.class, (double) SHORT_FIELD);

        assertPrimitiveCast("INT_FIELD", byte.class, (byte) INT_FIELD);
        assertPrimitiveCast("INT_FIELD", char.class, (char) INT_FIELD);
        assertPrimitiveCast("INT_FIELD", short.class, (short) INT_FIELD);
        assertPrimitiveCast("INT_FIELD", int.class, INT_FIELD);
        assertPrimitiveCast("INT_FIELD", long.class, (long) INT_FIELD);
        assertPrimitiveCast("INT_FIELD", float.class, (float) INT_FIELD);
        assertPrimitiveCast("INT_FIELD", double.class, (double) INT_FIELD);

        assertPrimitiveCast("LONG_FIELD", byte.class, (byte) LONG_FIELD);
        assertPrimitiveCast("LONG_FIELD", char.class, (char) LONG_FIELD);
        assertPrimitiveCast("LONG_FIELD", short.class, (short) LONG_FIELD);
        assertPrimitiveCast("LONG_FIELD", int.class, (int) LONG_FIELD);
        assertPrimitiveCast("LONG_FIELD", long.class, LONG_FIELD);
        assertPrimitiveCast("LONG_FIELD", float.class, (float) LONG_FIELD);
        assertPrimitiveCast("LONG_FIELD", double.class, (double) LONG_FIELD);

        assertPrimitiveCast("FLOAT_FIELD", byte.class, (byte) FLOAT_FIELD);
        assertPrimitiveCast("FLOAT_FIELD", char.class, (char) FLOAT_FIELD);
        assertPrimitiveCast("FLOAT_FIELD", short.class, (short) FLOAT_FIELD);
        assertPrimitiveCast("FLOAT_FIELD", int.class, (int) FLOAT_FIELD);
        assertPrimitiveCast("FLOAT_FIELD", long.class, (long) FLOAT_FIELD);
        assertPrimitiveCast("FLOAT_FIELD", float.class, FLOAT_FIELD);
        assertPrimitiveCast("FLOAT_FIELD", double.class, (double) FLOAT_FIELD);

        assertPrimitiveCast("DOUBLE_FIELD", byte.class, (byte) DOUBLE_FIELD);
        assertPrimitiveCast("DOUBLE_FIELD", char.class, (char) DOUBLE_FIELD);
        assertPrimitiveCast("DOUBLE_FIELD", short.class, (short) DOUBLE_FIELD);
        assertPrimitiveCast("DOUBLE_FIELD", int.class, (int) DOUBLE_FIELD);
        assertPrimitiveCast("DOUBLE_FIELD", long.class, (long) DOUBLE_FIELD);
        assertPrimitiveCast("DOUBLE_FIELD", float.class, (float) DOUBLE_FIELD);
        assertPrimitiveCast("DOUBLE_FIELD", double.class, DOUBLE_FIELD);
    }

    public void assertPrimitiveCast(String fieldName, Class<?> castToType, Object expected)
            throws Exception
    {
        // simple cast
        BytecodeExpression baseExpression = getStatic(getClass(), fieldName);
        assertCast(baseExpression, expected, castToType);

        // box result
        baseExpression = baseExpression.cast(castToType);
        Class<?> boxedType = Primitives.wrap(castToType);
        assertCast(baseExpression, expected, boxedType);

        // unbox the boxed result
        baseExpression = baseExpression.cast(boxedType);
        assertCast(baseExpression, expected, castToType);
    }

    public static void assertCast(BytecodeExpression expression, Object expectedValue, Class<?> castToType)
            throws Exception
    {
        BytecodeExpression castExpression = expression.cast(castToType);
        assertBytecodeExpression(castExpression, expectedValue, expectedCastRendering(expression.toString(), castToType));
        assertEquals(castExpression.getType().getJavaClassName(), castToType.getName());
    }

    public static void assertInvalidCast(BytecodeExpression expression, Class<?> castToType)
    {
        try {
            // Exception must be thrown here.
            // An exception that is thrown at actual byte code generation time is too late. At that point, stack trace is generally not useful.
            expression.cast(castToType);
            fail();
        }
        catch (IllegalArgumentException ignored) {
        }
    }

    public static String expectedCastRendering(String expectedRendering, Class<?> castToType)
    {
        return "((" + castToType.getSimpleName() + ") " + expectedRendering + ")";
    }
}
