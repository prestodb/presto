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
package com.facebook.presto.sql.relational;

import com.facebook.presto.spi.block.ArrayBlockBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestRowExpressionMatcher
{
    private static int entrySize = 10;

    private static final FunctionHandle TEST_FUNCTION = () -> null;

    @Test
    public void testConstant()
    {
        BlockBuilder arrayBlockBuilder = new ArrayBlockBuilder(BIGINT, null, entrySize);
        for (int i = 0; i < entrySize; i++) {
            BlockBuilder arrayElementBuilder = arrayBlockBuilder.beginBlockEntry();
            writeNativeValue(BIGINT, arrayElementBuilder, castIntegerToObject(i, BIGINT));
            arrayBlockBuilder.closeEntry();
        }
        Block block = arrayBlockBuilder.build();
        ConstantExpression actual = ConstantExpression.createConstantExpression(block, new ArrayType(INTEGER));
        ConstantExpression expected = ConstantExpression.createConstantExpression(block, new ArrayType(INTEGER));
        assertTrue(actual.accept(new RowExpressionMatcher().new RowExpressionMatchVisitor(), expected));

        ConstantExpression unexpected = ConstantExpression.createConstantExpression(block, new ArrayType(BIGINT));
        assertFalse(actual.accept(new RowExpressionMatcher().new RowExpressionMatchVisitor(), unexpected));
    }

    @Test
    public void testLambda()
    {
        LambdaDefinitionExpression actual = new LambdaDefinitionExpression(ImmutableList.of(INTEGER), ImmutableList.of("a"), new CallExpression("add", TEST_FUNCTION, BIGINT, ImmutableList.of(variable("lambda_argument"), variable("a"))));
        LambdaDefinitionExpression expected = new LambdaDefinitionExpression(ImmutableList.of(INTEGER), ImmutableList.of("c"), new CallExpression("add", TEST_FUNCTION, BIGINT, ImmutableList.of(variable("lambda_argument"), variable("c"))));
        assertTrue(actual.accept(new RowExpressionMatcher().new RowExpressionMatchVisitor(), expected));

        LambdaDefinitionExpression unexpected = new LambdaDefinitionExpression(ImmutableList.of(INTEGER), ImmutableList.of("c"), new CallExpression("add", TEST_FUNCTION, INTEGER, ImmutableList.of(variable("lambda_argument"), variable("d"))));
        assertFalse(actual.accept(new RowExpressionMatcher().new RowExpressionMatchVisitor(), unexpected));
    }

    @Test
    public void testCallExpression()
    {
        CallExpression actual = new CallExpression("add", TEST_FUNCTION, BIGINT, ImmutableList.of(variable("lambda_argument"), variable("a")));
        CallExpression expected = new CallExpression("add", TEST_FUNCTION, BIGINT, ImmutableList.of(variable("lambda_argument"), variable("a")));
        assertTrue(actual.accept(new RowExpressionMatcher().new RowExpressionMatchVisitor(), expected));

        CallExpression unexpected = new CallExpression("add", TEST_FUNCTION, BIGINT, ImmutableList.of(variable("lambda_argument"), variable("b")));
        assertFalse(actual.accept(new RowExpressionMatcher().new RowExpressionMatchVisitor(), unexpected));
    }

    @Test
    public void testInputReferenceExpression()
    {
        InputReferenceExpression actual = new InputReferenceExpression(0, BIGINT);
        InputReferenceExpression expected = new InputReferenceExpression(0, BIGINT);
        assertTrue(actual.accept(new RowExpressionMatcher().new RowExpressionMatchVisitor(), expected));

        InputReferenceExpression unexpected = new InputReferenceExpression(0, VARCHAR);
        assertFalse(actual.accept(new RowExpressionMatcher().new RowExpressionMatchVisitor(), unexpected));
    }

    @Test
    public void testVariableReferenceExpression()
    {
        VariableReferenceExpression actual = new VariableReferenceExpression("a", BIGINT);
        VariableReferenceExpression expected = new VariableReferenceExpression("a", BIGINT);
        assertTrue(actual.accept(new RowExpressionMatcher().new RowExpressionMatchVisitor(), expected));

        VariableReferenceExpression unexpected = new VariableReferenceExpression("b", BIGINT);
        assertFalse(actual.accept(new RowExpressionMatcher().new RowExpressionMatchVisitor(), unexpected));
    }

    @Test
    public void testSpecialFormExpression()
    {
        SpecialFormExpression actual = new SpecialFormExpression(IF, BIGINT, ImmutableList.of(constant(true, BOOLEAN), constant(1L, BIGINT), constant(2L, BIGINT)));
        SpecialFormExpression expected = new SpecialFormExpression(IF, BIGINT, ImmutableList.of(constant(true, BOOLEAN), constant(1L, BIGINT), constant(2L, BIGINT)));
        assertTrue(actual.accept(new RowExpressionMatcher().new RowExpressionMatchVisitor(), expected));

        SpecialFormExpression unexpected = new SpecialFormExpression(IF, BIGINT, ImmutableList.of(constant(true, BOOLEAN), constant(2L, BIGINT), constant(2L, BIGINT)));
        assertFalse(actual.accept(new RowExpressionMatcher().new RowExpressionMatchVisitor(), unexpected));
    }

    private static Object castIntegerToObject(int value, Type type)
    {
        if (type == INTEGER || type == TINYINT || type == BIGINT) {
            return (long) value;
        }
        if (type == VARCHAR) {
            return String.valueOf(value);
        }
        if (type == DOUBLE) {
            return (double) value;
        }
        throw new UnsupportedOperationException();
    }

    private VariableReferenceExpression variable(String name)
    {
        return new VariableReferenceExpression(name, BIGINT);
    }
}
