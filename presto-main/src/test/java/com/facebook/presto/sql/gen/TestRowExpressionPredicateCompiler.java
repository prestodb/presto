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

import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.Predicate;
import com.facebook.presto.spi.relation.PredicateCompiler;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.primitives.Ints;
import org.testng.annotations.Test;

import java.util.Arrays;

import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.MULTIPLY;
import static com.facebook.presto.spi.function.OperatorType.SUBTRACT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestRowExpressionPredicateCompiler
{
    private FunctionManager functionManager = createTestMetadataManager().getFunctionManager();
    private FunctionResolution functionResolution = new FunctionResolution(functionManager);

    @Test
    public void test()
    {
        InputReferenceExpression a = new InputReferenceExpression(0, BIGINT);
        InputReferenceExpression b = new InputReferenceExpression(1, BIGINT);

        Block aBlock = createLongBlock(5, 5, 5, 5, 5);
        Block bBlock = createLongBlock(1, 3, 5, 7, 0);

        // b - a >= 0
        RowExpression sum = call(
                "<",
                functionResolution.comparisonFunction(GREATER_THAN_OR_EQUAL, BIGINT, BIGINT),
                BOOLEAN,
                call("b - a", functionResolution.arithmeticFunction(SUBTRACT, BIGINT, BIGINT), BIGINT, b, a),
                constant(0L, BIGINT));

        PredicateCompiler compiler = new RowExpressionPredicateCompiler(functionManager, 10_000);
        Predicate compiledSum = compiler.compilePredicate(sum).get();

        assertEquals(Arrays.asList(1, 0), Ints.asList(compiledSum.getInputChannels()));

        Page page = new Page(bBlock, aBlock);
        assertFalse(compiledSum.evaluate(SESSION, page, 0));
        assertFalse(compiledSum.evaluate(SESSION, page, 1));
        assertTrue(compiledSum.evaluate(SESSION, page, 2));
        assertTrue(compiledSum.evaluate(SESSION, page, 3));
        assertFalse(compiledSum.evaluate(SESSION, page, 4));

        // b * 2 < 10
        RowExpression timesTwo = call(
                "=",
                functionResolution.comparisonFunction(LESS_THAN, BIGINT, BIGINT),
                BOOLEAN,
                call("b * 2", functionResolution.arithmeticFunction(MULTIPLY, BIGINT, BIGINT), BIGINT, b, constant(2L, BIGINT)),
                constant(10L, BIGINT));
        Predicate compiledTimesTwo = compiler.compilePredicate(timesTwo).get();

        assertEquals(Arrays.asList(1), Ints.asList(compiledTimesTwo.getInputChannels()));

        page = new Page(bBlock);
        assertTrue(compiledTimesTwo.evaluate(SESSION, page, 0));
        assertTrue(compiledTimesTwo.evaluate(SESSION, page, 1));
        assertFalse(compiledTimesTwo.evaluate(SESSION, page, 2));
        assertFalse(compiledTimesTwo.evaluate(SESSION, page, 3));
        assertTrue(compiledTimesTwo.evaluate(SESSION, page, 4));
    }

    @Test
    public void testCache()
    {
        // a * 2 < 10
        RowExpression predicate = call(
                "=",
                functionResolution.comparisonFunction(LESS_THAN, BIGINT, BIGINT),
                BOOLEAN,
                call("a * 2", functionResolution.arithmeticFunction(MULTIPLY, BIGINT, BIGINT), BIGINT, new InputReferenceExpression(1, BIGINT), constant(2L, BIGINT)),
                constant(10L, BIGINT));

        PredicateCompiler compiler = new RowExpressionPredicateCompiler(functionManager, 10_000);
        assertSame(compiler.compilePredicate(predicate), compiler.compilePredicate(predicate));

        PredicateCompiler noCacheCompiler = new RowExpressionPredicateCompiler(functionManager, 0);
        assertNotSame(noCacheCompiler.compilePredicate(predicate), noCacheCompiler.compilePredicate(predicate));
    }

    private static Block createLongBlock(long... values)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(values.length);
        for (long value : values) {
            BIGINT.writeLong(builder, value);
        }
        return builder.build();
    }
}
