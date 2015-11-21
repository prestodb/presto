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

import com.facebook.presto.byteCode.ByteCodeBlock;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.byteCode.Variable;
import org.testng.annotations.Test;

import java.awt.Point;
import java.util.function.Function;

import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressionAssertions.assertByteCodeExpression;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressionAssertions.assertByteCodeNode;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantInt;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantString;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.newInstance;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.setStatic;
import static com.facebook.presto.util.Reflection.field;
import static org.testng.Assert.assertEquals;

public class TestSetFieldByteCodeExpression
{
    public static String testField;

    @Test
    public void testSetField()
            throws Exception
    {
        assertSetPoint(point -> point.setField("x", constantInt(42)));
        assertSetPoint(point -> point.setField(field(Point.class, "x"), constantInt(42)));
    }

    public static void assertSetPoint(Function<ByteCodeExpression, ByteCodeExpression> setX)
            throws Exception
    {
        Function<Scope, ByteCodeNode> nodeGenerator = scope -> {
            Variable point = scope.declareVariable(Point.class, "point");

            ByteCodeExpression setExpression = setX.apply(point);
            assertEquals(setExpression.toString(), "point.x = 42;");

            return new ByteCodeBlock()
                    .append(point.set(newInstance(Point.class, constantInt(3), constantInt(7))))
                    .append(setExpression)
                    .append(point.ret());
        };

        assertByteCodeNode(nodeGenerator, type(Point.class), new Point(42, 7));
    }

    @Test
    public void testSetStaticField()
            throws Exception
    {
        assertSetStaticField(setStatic(getClass(), "testField", constantString("testValue")));
        assertSetStaticField(setStatic(getClass().getField("testField"), constantString("testValue")));
        assertSetStaticField(setStatic(type(getClass()), "testField", constantString("testValue")));
    }

    public void assertSetStaticField(ByteCodeExpression setStaticField)
            throws Exception
    {
        testField = "fail";
        assertByteCodeExpression(setStaticField, null, getClass().getSimpleName() + ".testField = \"testValue\";");
        assertEquals(testField, "testValue");
    }
}
