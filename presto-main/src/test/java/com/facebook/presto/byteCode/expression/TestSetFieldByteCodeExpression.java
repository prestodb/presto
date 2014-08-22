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

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.Variable;
import org.testng.annotations.Test;

import java.awt.Point;

import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressionAssertions.assertByteCodeExpression;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressionAssertions.assertByteCodeNode;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantInt;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantString;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.newInstance;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.setStatic;
import static com.facebook.presto.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static org.testng.Assert.assertEquals;

public class TestSetFieldByteCodeExpression
{
    public static String testField;

    @Test
    public void testSetField()
            throws Exception
    {
        CompilerContext context = new CompilerContext(BOOTSTRAP_METHOD);
        Variable point = context.declareVariable(Point.class, "point");

        assertSetPoint(context, point.setField("x", constantInt(42)));
        assertSetPoint(context, point.setField(Point.class.getField("x"), constantInt(42)));
    }

    public static void assertSetPoint(CompilerContext context, ByteCodeExpression setX)
            throws Exception
    {
        assertEquals(setX.toString(), "point.x = 42;");

        Block block = new Block(context)
                .append(context.getVariable("point").set(newInstance(Point.class, constantInt(3), constantInt(7))))
                .append(setX)
                .append(context.getVariable("point").ret());

        assertByteCodeNode(block, type(Point.class), new Point(42, 7));
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
