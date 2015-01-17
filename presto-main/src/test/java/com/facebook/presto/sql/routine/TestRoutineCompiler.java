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
package com.facebook.presto.sql.routine;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.metadata.OperatorType.ADD;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.MULTIPLY;
import static com.facebook.presto.metadata.OperatorType.SUBTRACT;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

public class TestRoutineCompiler
{
    private static final TypeManager TYPE_MANAGER = new TypeRegistry();
    private static final FunctionRegistry FUNCTION_REGISTRY = new FunctionRegistry(TYPE_MANAGER, new BlockEncodingManager(TYPE_MANAGER), true);
    private static final RoutineCompiler COMPILER = new RoutineCompiler(FUNCTION_REGISTRY);

    @Test
    public void testSimpleExpression()
            throws Throwable
    {
        // CREATE FUNCTION test(a bigint)
        // RETURNS bigint
        // BEGIN
        //   DECLARE x bigint DEFAULT 99;
        //   RETURN x * a;
        // END

        SqlVariable arg = new SqlVariable(0, BIGINT, constantNull(BIGINT));
        SqlVariable variable = new SqlVariable(1, BIGINT, constant(99L, BIGINT));

        Signature multiply = operator(MULTIPLY, BIGINT, BIGINT, BIGINT);

        SqlRoutine routine = new SqlRoutine(
                BIGINT,
                parameters(arg),
                new SqlBlock(
                        variables(variable),
                        statements(
                                new SqlSet(variable, call(multiply, BIGINT, reference(variable), reference(arg))),
                                new SqlReturn(reference(variable)))));

        Class<?> clazz = COMPILER.compile(routine);

        MethodHandle handle = methodHandle(clazz, "run", Long.class);

        assertEquals(handle.invoke(0L), 0L);
        assertEquals(handle.invoke(1L), 99L);
        assertEquals(handle.invoke(42L), 42L * 99);
        assertEquals(handle.invoke(123L), 123L * 99);
    }

    @Test
    public void testFibonacciWhileLoop()
            throws Throwable
    {
        // CREATE FUNCTION fib(n bigint)
        // RETURNS bigint
        // BEGIN
        //   DECLARE a bigint DEFAULT 1;
        //   DECLARE b bigint DEFAULT 1;
        //   DECLARE c bigint;
        //
        //   IF n <= 2 THEN
        //     RETURN 1;
        //   END IF;
        //
        //   WHILE n > 2 DO
        //     SET n = n - 1;
        //     SET c = a + b;
        //     SET a = b;
        //     SET b = c;
        //   END WHILE;
        //
        //   RETURN c;
        // END

        SqlVariable n = new SqlVariable(0, BIGINT, constantNull(BIGINT));
        SqlVariable a = new SqlVariable(1, BIGINT, constant(1L, BIGINT));
        SqlVariable b = new SqlVariable(2, BIGINT, constant(1L, BIGINT));
        SqlVariable c = new SqlVariable(3, BIGINT, constantNull(BIGINT));

        Signature add = operator(ADD, BIGINT, BIGINT, BIGINT);
        Signature subtract = operator(SUBTRACT, BIGINT, BIGINT, BIGINT);
        Signature lessThanOrEqual = operator(LESS_THAN_OR_EQUAL, BOOLEAN, BIGINT, BIGINT);
        Signature greaterThan = operator(GREATER_THAN, BOOLEAN, BIGINT, BIGINT);

        SqlRoutine routine = new SqlRoutine(
                BIGINT,
                parameters(n),
                new SqlBlock(
                        variables(a, b, c),
                        statements(
                                new SqlIf(
                                        call(lessThanOrEqual, BOOLEAN, reference(n), constant(2L, BIGINT)),
                                        new SqlReturn(constant(1L, BIGINT))),
                                new SqlWhile(
                                        call(greaterThan, BOOLEAN, reference(n), constant(2L, BIGINT)),
                                        new SqlBlock(
                                                variables(),
                                                statements(
                                                        new SqlSet(n, call(subtract, BIGINT, reference(n), constant(1L, BIGINT))),
                                                        new SqlSet(c, call(add, BIGINT, reference(a), reference(b))),
                                                        new SqlSet(a, reference(b)),
                                                        new SqlSet(b, reference(c))))),
                                new SqlReturn(reference(c)))));

        Class<?> clazz = COMPILER.compile(routine);

        MethodHandle handle = methodHandle(clazz, "run", Long.class);

        assertEquals(handle.invoke(1L), 1L);
        assertEquals(handle.invoke(2L), 1L);
        assertEquals(handle.invoke(3L), 2L);
        assertEquals(handle.invoke(4L), 3L);
        assertEquals(handle.invoke(5L), 5L);
        assertEquals(handle.invoke(6L), 8L);
        assertEquals(handle.invoke(7L), 13L);
        assertEquals(handle.invoke(8L), 21L);
    }

    @Test
    public void testBreakContinue()
            throws Throwable
    {
        // CREATE FUNCTION test()
        // RETURNS bigint
        // BEGIN
        //   DECLARE a bigint DEFAULT 0;
        //   DECLARE b bigint DEFAULT 0;
        //
        //   top: WHILE a < 10 DO
        //     SET a = a + 1;
        //     IF a < 3 THEN
        //       ITERATE top;
        //     END IF;
        //     SET b = b + 1;
        //     IF a > 6 THEN
        //       LEAVE top;
        //     END IF;
        //   END WHILE;
        //
        //   RETURN b;
        // END

        SqlVariable a = new SqlVariable(0, BIGINT, constant(0L, BIGINT));
        SqlVariable b = new SqlVariable(1, BIGINT, constant(0L, BIGINT));

        Signature add = operator(ADD, BIGINT, BIGINT, BIGINT);
        Signature lessThan = operator(LESS_THAN, BOOLEAN, BIGINT, BIGINT);
        Signature greaterThan = operator(GREATER_THAN, BOOLEAN, BIGINT, BIGINT);

        SqlLabel label = new SqlLabel();

        SqlRoutine routine = new SqlRoutine(
                BIGINT,
                parameters(),
                new SqlBlock(
                        variables(a, b),
                        statements(
                                new SqlWhile(
                                        Optional.of(label),
                                        call(lessThan, BOOLEAN, reference(a), constant(10L, BIGINT)),
                                        new SqlBlock(
                                                variables(),
                                                statements(
                                                        new SqlSet(a, call(add, BIGINT, reference(a), constant(1L, BIGINT))),
                                                        new SqlIf(
                                                                call(lessThan, BOOLEAN, reference(a), constant(3L, BIGINT)),
                                                                new SqlContinue(label)),
                                                        new SqlSet(b, call(add, BIGINT, reference(b), constant(1L, BIGINT))),
                                                        new SqlIf(
                                                                call(greaterThan, BOOLEAN, reference(a), constant(6L, BIGINT)),
                                                                new SqlBreak(label))))),
                                new SqlReturn(reference(b)))));

        Class<?> clazz = COMPILER.compile(routine);

        MethodHandle handle = methodHandle(clazz, "run");

        assertEquals(handle.invoke(), 5L);
    }

    private static List<SqlVariable> parameters(SqlVariable... variables)
    {
        return ImmutableList.copyOf(variables);
    }

    private static List<SqlVariable> variables(SqlVariable... variables)
    {
        return ImmutableList.copyOf(variables);
    }

    private static List<SqlStatement> statements(SqlStatement... statements)
    {
        return ImmutableList.copyOf(statements);
    }

    private static RowExpression reference(SqlVariable variable)
    {
        return new InputReferenceExpression(variable.getField(), variable.getType());
    }

    private static Signature operator(OperatorType operator, Type returnType, Type... argumentTypes)
    {
        return internalOperator(
                operator.name(),
                returnType.getTypeSignature(),
                Arrays.stream(argumentTypes).map(Type::getTypeSignature).collect(toList()));
    }
}
