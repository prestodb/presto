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
import static com.facebook.presto.byteCode.expression.ByteCodeExpressionAssertions.assertByteCodeNode;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantInt;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.newInstance;
import static org.testng.Assert.assertEquals;

public class TestSetVariableByteCodeExpression
{
    @Test
    public void testGetField()
            throws Exception
    {
        Function<Scope, ByteCodeNode> nodeGenerator = scope -> {
            Variable point = scope.declareVariable(Point.class, "point");
            ByteCodeExpression setPoint = point.set(newInstance(Point.class, constantInt(3), constantInt(7)));

            assertEquals(setPoint.toString(), "point = new Point(3, 7);");

            return new ByteCodeBlock()
                    .append(setPoint)
                    .append(point.ret());
        };

        assertByteCodeNode(nodeGenerator, type(Point.class), new Point(3, 7));
    }
}
