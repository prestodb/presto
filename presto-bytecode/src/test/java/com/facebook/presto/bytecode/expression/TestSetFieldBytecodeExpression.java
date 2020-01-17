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

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import org.testng.annotations.Test;

import java.awt.Point;
import java.lang.reflect.Field;
import java.util.function.Function;

import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressionAssertions.assertBytecodeExpression;
import static com.facebook.presto.bytecode.expression.BytecodeExpressionAssertions.assertBytecodeNode;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantString;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newInstance;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.setStatic;
import static org.testng.Assert.assertEquals;

public class TestSetFieldBytecodeExpression
{
    public static String testField;

    @Test
    public void testSetField()
            throws Exception
    {
        assertSetPoint(point -> point.setField("x", constantInt(42)));
        assertSetPoint(point -> point.setField(field(Point.class, "x"), constantInt(42)));
    }

    public static void assertSetPoint(Function<BytecodeExpression, BytecodeExpression> setX)
            throws Exception
    {
        Function<Scope, BytecodeNode> nodeGenerator = scope -> {
            Variable point = scope.declareVariable(Point.class, "point");

            BytecodeExpression setExpression = setX.apply(point);
            assertEquals(setExpression.toString(), "point.x = 42;");

            return new BytecodeBlock()
                    .append(point.set(newInstance(Point.class, constantInt(3), constantInt(7))))
                    .append(setExpression)
                    .append(point.ret());
        };

        assertBytecodeNode(nodeGenerator, type(Point.class), new Point(42, 7));
    }

    @Test
    public void testSetStaticField()
            throws Exception
    {
        assertSetStaticField(setStatic(getClass(), "testField", constantString("testValue")));
        assertSetStaticField(setStatic(getClass().getField("testField"), constantString("testValue")));
        assertSetStaticField(setStatic(type(getClass()), "testField", constantString("testValue")));
    }

    public void assertSetStaticField(BytecodeExpression setStaticField)
            throws Exception
    {
        testField = "fail";
        assertBytecodeExpression(setStaticField, null, getClass().getSimpleName() + ".testField = \"testValue\";");
        assertEquals(testField, "testValue");
    }

    private static Field field(Class<?> clazz, String name)
    {
        try {
            return clazz.getField(name);
        }
        catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }
}
