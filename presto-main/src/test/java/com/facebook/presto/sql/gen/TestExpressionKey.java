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
package com.facebook.presto.sql.gen;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.sql.tree.InputReference;
import com.facebook.presto.sql.tree.LongLiteral;
import org.testng.annotations.Test;

import java.util.IdentityHashMap;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.tree.ArithmeticExpression.Type.ADD;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.EQUAL;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TestExpressionKey
{
    @Test
    public void testEqual()
            throws Exception
    {
        IdentityHashMap<Expression, Type> firstTypes = new IdentityHashMap<>();
        ComparisonExpression firstExpression = comparison(firstTypes,
                EQUAL,
                longLiteral(firstTypes, "42"), add(firstTypes,
                        longLiteral(firstTypes, "40"),
                        longLiteral(firstTypes, "2"))
        );

        IdentityHashMap<Expression, Type> secondTypes = new IdentityHashMap<>();
        ComparisonExpression secondExpression = comparison(secondTypes,
                EQUAL,
                longLiteral(secondTypes, "42"), add(secondTypes,
                        longLiteral(secondTypes, "40"),
                        longLiteral(secondTypes, "2"))
        );

        assertEquals(firstExpression, secondExpression);
        assertNotEquals(firstTypes, secondTypes);
        assertEquals(new ExpressionKey(firstExpression, firstTypes), new ExpressionKey(secondExpression, secondTypes));
    }

    @Test
    public void testNotEqual()
            throws Exception
    {
        IdentityHashMap<Expression, Type> firstTypes = new IdentityHashMap<>();
        ComparisonExpression firstExpression = comparison(firstTypes,
                EQUAL,
                input(firstTypes, 1, BIGINT),
                add(firstTypes,
                        input(firstTypes, 2, BIGINT),
                        input(firstTypes, 3, BIGINT))
        );

        IdentityHashMap<Expression, Type> secondTypes = new IdentityHashMap<>();
        ComparisonExpression secondExpression = comparison(secondTypes, EQUAL,
                input(secondTypes, 1, DOUBLE),
                add(secondTypes,
                        input(secondTypes, 2, DOUBLE),
                        input(secondTypes, 3, DOUBLE))
        );

        assertEquals(firstExpression, secondExpression);
        assertNotEquals(firstTypes, secondTypes);
        assertNotEquals(new ExpressionKey(firstExpression, firstTypes), new ExpressionKey(secondExpression, secondTypes));
    }

    public static ComparisonExpression comparison(IdentityHashMap<Expression, Type> types, ComparisonExpression.Type type, Expression left, Expression right)
    {
        ComparisonExpression expression = new ComparisonExpression(type, left, right);
        types.put(expression, BOOLEAN);
        return expression;
    }

    public static ArithmeticExpression add(IdentityHashMap<Expression, Type> types, Expression left, Expression right)
    {
        ArithmeticExpression expression = new ArithmeticExpression(ADD, left, right);
        types.put(expression, BIGINT);
        return expression;
    }

    public static LongLiteral longLiteral(IdentityHashMap<Expression, Type> types, String value)
    {
        LongLiteral expression = new LongLiteral(value);
        types.put(expression, BIGINT);
        return expression;
    }

    public static InputReference input(IdentityHashMap<Expression, Type> types, int channel, Type type)
    {
        InputReference expression = new InputReference(new Input(channel));
        types.put(expression, type);
        return expression;
    }
}
