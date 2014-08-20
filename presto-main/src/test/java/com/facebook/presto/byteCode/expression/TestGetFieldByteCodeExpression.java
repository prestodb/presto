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

import java.awt.Point;

import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressionAssertions.assertByteCodeExpression;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantInt;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.getStatic;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.newInstance;

public class TestGetFieldByteCodeExpression
{
    @Test
    public void testGetField()
            throws Exception
    {
        assertByteCodeExpression(newInstance(Point.class, constantInt(3), constantInt(7)).getField("x", int.class), new Point(3, 7).x, "new Point(3, 7).x");
        assertByteCodeExpression(newInstance(Point.class, constantInt(3), constantInt(7)).getField(Point.class, "x"), new Point(3, 7).x, "new Point(3, 7).x");
        assertByteCodeExpression(newInstance(Point.class, constantInt(3), constantInt(7)).getField(Point.class.getField("x")), new Point(3, 7).x, "new Point(3, 7).x");
    }

    @Test
    public void testGetStaticField()
            throws Exception
    {
        assertByteCodeExpression(getStatic(Long.class, "MIN_VALUE"), Long.MIN_VALUE, "Long.MIN_VALUE");
        assertByteCodeExpression(getStatic(Long.class.getField("MIN_VALUE")), Long.MIN_VALUE, "Long.MIN_VALUE");
        assertByteCodeExpression(getStatic(type(Long.class), "MIN_VALUE", type(long.class)), Long.MIN_VALUE, "Long.MIN_VALUE");
    }
}
