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
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.CreateFunction;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Throwables;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertEquals;

public class TestRoutines
{
    private static final TypeManager TYPE_MANAGER = new TypeRegistry();
    private static final FunctionRegistry FUNCTION_REGISTRY = new FunctionRegistry(TYPE_MANAGER, new BlockEncodingManager(TYPE_MANAGER), true);
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testConstantReturn()
            throws Throwable
    {
        MethodHandle handle = createFunction("" +
                "CREATE FUNCTION answer()\n" +
                "RETURNS BIGINT\n" +
                "RETURN 42");

        assertEquals(handle.invoke(), 42L);
    }

    @Test
    public void testSimpleReturn()
            throws Throwable
    {
        MethodHandle handle = createFunction("" +
                "CREATE FUNCTION hello(s VARCHAR)\n" +
                "RETURNS VARCHAR\n" +
                "RETURN 'Hello, ' || s || '!'");

        assertEquals(handle.invoke(utf8Slice("world")), utf8Slice("Hello, world!"));
        assertEquals(handle.invoke(utf8Slice("WORLD")), utf8Slice("Hello, WORLD!"));
    }

    @Test
    public void testSimpleExpression()
            throws Throwable
    {
        MethodHandle handle = createFunction("" +
                "CREATE FUNCTION test(a bigint)\n" +
                "RETURNS bigint\n" +
                "BEGIN\n" +
                "  DECLARE x bigint DEFAULT 99;\n" +
                "  RETURN x * a;\n" +
                "END");

        assertEquals(handle.invoke(0L), 0L);
        assertEquals(handle.invoke(1L), 99L);
        assertEquals(handle.invoke(42L), 42L * 99);
        assertEquals(handle.invoke(123L), 123L * 99);
    }

    @Test
    public void testFibonacciWhileLoop()
            throws Throwable
    {
        MethodHandle handle = createFunction("" +
                "CREATE FUNCTION fib(n bigint)\n" +
                "RETURNS bigint\n" +
                "BEGIN\n" +
                "  DECLARE a bigint DEFAULT 1;\n" +
                "  DECLARE b bigint DEFAULT 1;\n" +
                "  DECLARE c bigint;\n" +
                "  IF n <= 2 THEN\n" +
                "    RETURN 1;\n" +
                "  END IF;\n" +
                "  WHILE n > 2 DO\n" +
                "    SET n = n - 1;\n" +
                "    SET c = a + b;\n" +
                "    SET a = b;\n" +
                "    SET b = c;\n" +
                "  END WHILE;\n" +
                "  RETURN c;\n" +
                "END");

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
        MethodHandle handle = createFunction("" +
                "CREATE FUNCTION test()\n" +
                "RETURNS bigint\n" +
                "BEGIN\n" +
                "  DECLARE a bigint DEFAULT 0;\n" +
                "  DECLARE b bigint DEFAULT 0;\n" +
                "  top: WHILE a < 10 DO\n" +
                "    SET a = a + 1;\n" +
                "    IF a < 3 THEN\n" +
                "      ITERATE top;\n" +
                "    END IF;\n" +
                "    SET b = b + 1;\n" +
                "    IF a > 6 THEN\n" +
                "      LEAVE top;\n" +
                "    END IF;\n" +
                "  END WHILE;\n" +
                "  RETURN b;\n" +
                "END");

        assertEquals(handle.invoke(), 5L);
    }

    private static MethodHandle createFunction(@Language("SQL") String sql)
    {
        CreateFunction function = parseFunction(sql);

        FunctionAnalyzer analyzer = new FunctionAnalyzer(TYPE_MANAGER, FUNCTION_REGISTRY);
        AnalyzedFunction result = analyzer.analyze(function);

        RoutineCompiler compiler = new RoutineCompiler(FUNCTION_REGISTRY);
        Class<?> clazz = compiler.compile(result.getRoutine());

        return getRunMethod(clazz);
    }

    private static CreateFunction parseFunction(@Language("SQL") String sql)
    {
        Statement statement = SQL_PARSER.createStatement(sql);
        assertInstanceOf(statement, CreateFunction.class);
        return (CreateFunction) statement;
    }

    private static MethodHandle getRunMethod(Class<?> clazz)
    {
        for (Method method : clazz.getMethods()) {
            if (method.getName().equals("run")) {
                try {
                    return MethodHandles.lookup().unreflect(method);
                }
                catch (IllegalAccessException e) {
                    throw Throwables.propagate(e);
                }
            }
        }
        throw new RuntimeException("cannot find run method");
    }
}
