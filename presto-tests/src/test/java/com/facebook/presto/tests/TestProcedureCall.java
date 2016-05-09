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
package com.facebook.presto.tests;

import com.facebook.presto.testing.ProcedureTester;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestProcedureCall
        extends AbstractTestQueryFramework
{
    private final ProcedureTester tester;

    public TestProcedureCall()
            throws Exception
    {
        super(createQueryRunner());
        tester = ((DistributedQueryRunner) queryRunner).getCoordinator().getProcedureTester();
    }

    @Test
    public void testProcedureCall()
            throws Exception
    {
        String catalog = getSession().getCatalog().get();
        String schema = getSession().getSchema().get();

        assertCall("CALL test_simple()", "simple");
        assertCall(format("CALL %s.test_simple()", schema), "simple");
        assertCall(format("CALL %s.%s.test_simple()", catalog, schema), "simple");

        assertCall("CALL test_args(123, 4.5, 'hello', true)", "args", 123L, 4.5, "hello", true);
        assertCall("CALL test_args(-5, nan(), 'bye', false)", "args", -5L, Double.NaN, "bye", false);
        assertCall("CALL test_args(3, 88, 'coerce', true)", "args", 3L, 88.0, "coerce", true);
        assertCall("CALL test_args(x => 123, y => 4.5, z => 'hello', q => true)", "args", 123L, 4.5, "hello", true);
        assertCall("CALL test_args(q => true, z => 'hello', y => 4.5, x => 123)", "args", 123L, 4.5, "hello", true);

        assertCall("CALL test_nulls(123, null)", "nulls", 123L, null);
        assertCall("CALL test_nulls(null, 'apple')", "nulls", null, "apple");

        assertCall("CALL test_arrays(ARRAY [12, 34], ARRAY['abc', 'xyz'])", "arrays", list(12L, 34L), list("abc", "xyz"));
        assertCall("CALL test_arrays(ARRAY [], ARRAY[])", "arrays", list(), list());
        assertCall("CALL test_nested(ARRAY [ARRAY[12, 34], ARRAY[56]])", "nested", list(list(12L, 34L), list(56L)));

        assertCall("CALL test_nested(ARRAY [])", "nested", list());
        assertCall("CALL test_nested(ARRAY [ARRAY[]])", "nested", list(list()));

        assertCall("CALL test_session_first(123)", "session_first", 123L);
        assertCall("CALL test_session_last('grape')", "session_last", "grape");

        assertCallThrows("CALL test_exception()", "exception", "test exception from procedure");
        assertCallThrows("CALL test_error()", "error", "test error from procedure");

        assertCallFails("CALL test_args(null, 4.5, 'hello', true)", "Procedure argument cannot be null: x");
        assertCallFails("CALL test_args(123, null, 'hello', true)", "Procedure argument cannot be null: y");
        assertCallFails("CALL test_args(123, 4.5, 'hello', null)", "Procedure argument cannot be null: q");

        assertCallFails("CALL test_simple(123)", "line 1:1: Too many arguments for procedure");
        assertCallFails("CALL test_args(123, 4.5, 'hello')", "line 1:1: Too few arguments for procedure");
        assertCallFails("CALL test_args(x => 123, y => 4.5, q => true)", "line 1:1: Too few arguments for procedure");
        assertCallFails("CALL test_args(123, 4.5, 'hello', q => true)", "line 1:1: Named and positional arguments cannot be mixed");
        assertCallFails("CALL test_args(x => 3, x => 4)", "line 1:24: Duplicate procedure argument: x");
        assertCallFails("CALL test_args(t => 404)", "line 1:16: Unknown argument name: t");
        assertCallFails("CALL test_nulls('hello', null)", "line 1:17: Cannot cast type bigint to varchar(5)");
        assertCallFails("CALL test_nulls(null, 123)", "line 1:23: Cannot cast type varchar to integer");
    }

    private void assertCall(@Language("SQL") String sql, String name, Object... arguments)
    {
        tester.reset();
        assertUpdate(sql);
        assertEquals(tester.getCalledName(), name);
        assertEquals(tester.getCalledArguments(), list(arguments));
    }

    private void assertCallThrows(@Language("SQL") String sql, String name, String message)
    {
        tester.reset();
        try {
            assertUpdate(sql);
            fail("expected exception");
        }
        catch (RuntimeException e) {
            assertEquals(tester.getCalledName(), name);
            assertEquals(tester.getCalledArguments(), list());
            assertEquals(e.getMessage(), message);
        }
    }

    private void assertCallFails(@Language("SQL") String sql, String message)
    {
        tester.reset();
        try {
            assertUpdate(sql);
            fail("expected exception");
        }
        catch (RuntimeException e) {
            assertFalse(tester.wasCalled());
            assertEquals(e.getMessage(), message);
        }
    }

    @SafeVarargs
    private static <T> List<T> list(T... elements)
    {
        return asList(elements);
    }
}
