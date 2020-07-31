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

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestSqlFunctions
        extends AbstractTestQueryFramework
{
    protected TestSqlFunctions()
    {
        super(TestSqlFunctions::createQueryRunner);
    }

    private static QueryRunner createQueryRunner()
    {
        try {
            Session session = testSessionBuilder()
                    .setCatalog("tpch")
                    .setSchema(TINY_SCHEMA_NAME)
                    .build();
            DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                    .setCoordinatorProperties(ImmutableMap.of("list-built-in-functions-only", "false"))
                    .build();
            queryRunner.enableTestFunctionNamespaces(ImmutableList.of("testing", "example"), ImmutableMap.of("supported-function-languages", "{\"sql\": \"SQL\", \"java\": \"THRIFT\"}"));
            queryRunner.createTestFunctionNamespace("testing", "common");
            queryRunner.createTestFunctionNamespace("testing", "test");
            queryRunner.createTestFunctionNamespace("example", "example");
            return queryRunner;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterMethod
    public void dropSqlFunctions()
    {
        List<MaterializedRow> sqlFunctions = computeActual("SHOW FUNCTIONS").getMaterializedRows().stream()
                .filter(row -> !((boolean) row.getField(7)))
                .collect(toImmutableList());
        for (MaterializedRow function : sqlFunctions) {
            assertQuerySucceeds(format("DROP FUNCTION %s (%s)", function.getField(0), function.getField(2)));
        }
    }

    @Test
    public void testCreateFunctionInvalidFunctionName()
    {
        assertQueryFails(
                "CREATE FUNCTION testing.tan (x int) RETURNS double COMMENT 'tangent trigonometric function' RETURN sin(x) / cos(x)",
                ".*Function name should be in the form of catalog\\.schema\\.function_name, found: testing\\.tan");
        assertQueryFails(
                "CREATE FUNCTION presto.default.tan (x int) RETURNS double COMMENT 'tangent trigonometric function' RETURN sin(x) / cos(x)",
                "Cannot create function in built-in function namespace: presto\\.default\\.tan");
    }

    @Test
    public void testCreateFunctionInvalidSemantics()
    {
        assertQueryFails(
                "CREATE FUNCTION testing.common.tan (x int) RETURNS varchar COMMENT 'tangent trigonometric function' RETURN sin(x) / cos(x)",
                "Function implementation type 'double' does not match declared return type 'varchar'");
        assertQueryFails(
                "CREATE FUNCTION testing.common.tan (x int) RETURNS varchar COMMENT 'tangent trigonometric function' RETURN sin(y) / cos(y)",
                ".*Column 'y' cannot be resolved");
        assertQueryFails(
                "CREATE FUNCTION testing.common.tan (x double) RETURNS double COMMENT 'tangent trigonometric function' RETURN sum(x)",
                ".*CREATE FUNCTION body cannot contain aggregations, window functions or grouping operations:.*");
    }

    @Test
    public void testCreateFunction()
    {
        assertQuerySucceeds("CREATE FUNCTION testing.test.tan (x int) RETURNS double RETURN sin(x) / cos(x)");
        assertQuerySucceeds("CREATE FUNCTION testing.test.tan (x double) RETURNS double LANGUAGE JAVA RETURN sin(x) / cos(x)");

        // external function
        assertQuerySucceeds("CREATE FUNCTION testing.test.foo(x varchar) RETURNS varchar LANGUAGE JAVA EXTERNAL");
        assertQuerySucceeds("CREATE FUNCTION testing.test.foo(x varchar(3)) RETURNS varchar LANGUAGE SQL EXTERNAL");
        assertQuerySucceeds("CREATE FUNCTION testing.test.foo(x int) RETURNS bigint LANGUAGE JAVA EXTERNAL NAME foo_from_another_library");
        assertQuerySucceeds("CREATE FUNCTION testing.test.foo(x bigint) RETURNS bigint LANGUAGE JAVA EXTERNAL NAME \"foo.from.another.library\"");
        assertQuerySucceeds("CREATE FUNCTION testing.test.foo(x double) RETURNS double LANGUAGE \"JAVA\" EXTERNAL");
        assertQueryFails("CREATE FUNCTION testing.test.foo(x smallint) RETURNS bigint LANGUAGE JAVA EXTERNAL NAME 'foo.from.another.library'", ".*mismatched input ''foo.from.another.library''. Expecting: <identifier>");
        assertQueryFails("CREATE FUNCTION testing.test.foo(x varchar) RETURNS varchar LANGUAGE JAVA EXTERNAL NAME", ".*mismatched input '<EOF>'. Expecting: <identifier>");
        assertQueryFails("CREATE FUNCTION testing.test.foo(x varchar) RETURNS varchar LANGUAGE UNSUPPORTED EXTERNAL", "Catalog testing does not support functions implemented in language UNSUPPORTED");
    }

    @Test
    public void testCreateFunctionWithCoercion()
    {
        assertQuerySucceeds("CREATE FUNCTION testing.test.return_double() RETURNS DOUBLE RETURN 1");
        String createFunctionReturnDoubleFormatted = "CREATE FUNCTION testing.test.return_double ()\n" +
                "RETURNS DOUBLE\n" +
                "COMMENT ''\n" +
                "LANGUAGE SQL\n" +
                "NOT DETERMINISTIC\n" +
                "CALLED ON NULL INPUT\n" +
                "RETURN CAST(1 AS double)";

        MaterializedResult rows = computeActual("SHOW CREATE FUNCTION testing.test.return_double()");
        assertEquals(rows.getMaterializedRows().get(0).getFields(), ImmutableList.of(createFunctionReturnDoubleFormatted, ""));

        rows = computeActual("SELECT testing.test.return_double() + 1");
        assertEquals(rows.getMaterializedRows().get(0).getFields().get(0), 2.0);

        assertQuerySucceeds("CREATE FUNCTION testing.test.return_varchar() RETURNS VARCHAR RETURN 'ABC'");
        String createFunctionReturnVarcharFormatted = "CREATE FUNCTION testing.test.return_varchar ()\n" +
                "RETURNS varchar\n" +
                "COMMENT ''\n" +
                "LANGUAGE SQL\n" +
                "NOT DETERMINISTIC\n" +
                "CALLED ON NULL INPUT\n" +
                "RETURN CAST('ABC' AS varchar)";

        rows = computeActual("SHOW CREATE FUNCTION testing.test.return_varchar()");
        assertEquals(rows.getMaterializedRows().get(0).getFields(), ImmutableList.of(createFunctionReturnVarcharFormatted, ""));

        rows = computeActual("SELECT lower(testing.test.return_varchar())");
        assertEquals(rows.getMaterializedRows().get(0).getFields().get(0), "abc");

        // no explicit cast added
        assertQuerySucceeds("CREATE FUNCTION testing.test.return_int() RETURNS INTEGER RETURN 1");
        String createFunctionReturnIntFormatted = "CREATE FUNCTION testing.test.return_int ()\n" +
                "RETURNS INTEGER\n" +
                "COMMENT ''\n" +
                "LANGUAGE SQL\n" +
                "NOT DETERMINISTIC\n" +
                "CALLED ON NULL INPUT\n" +
                "RETURN 1";

        rows = computeActual("SHOW CREATE FUNCTION testing.test.return_int()");
        assertEquals(rows.getMaterializedRows().get(0).getFields(), ImmutableList.of(createFunctionReturnIntFormatted, ""));

        rows = computeActual("SELECT testing.test.return_int() + 3");
        assertEquals(rows.getMaterializedRows().get(0).getFields().get(0), 4);
    }

    @Test
    public void testAlterFunctionInvalidFunctionName()
    {
        assertQueryFails(
                "ALTER FUNCTION tan CALLED ON NULL INPUT",
                ".*Function name should be in the form of catalog\\.schema\\.function_name, found: tan");
        assertQueryFails(
                "ALTER FUNCTION testing.tan CALLED ON NULL INPUT",
                ".*Function name should be in the form of catalog\\.schema\\.function_name, found: testing\\.tan");
        assertQueryFails(
                "ALTER FUNCTION presto.default.sin RETURNS NULL ON NULL INPUT",
                "Cannot alter function in built-in function namespace: presto\\.default\\.sin");
    }

    @Test
    public void testDropFunctionInvalidFunctionName()
    {
        assertQueryFails(
                "DROP FUNCTION IF EXISTS testing.tan",
                ".*Function name should be in the form of catalog\\.schema\\.function_name, found: testing\\.tan");
        assertQueryFails(
                "DROP FUNCTION presto.default.sin (double)",
                "Cannot drop function in built-in function namespace: presto\\.default\\.sin");
    }

    @Test
    public void testNestedSqlFunctions()
    {
        assertQuerySucceeds("CREATE FUNCTION testing.common.a() RETURNS int RETURN 1");
        assertQueryFails(
                "CREATE FUNCTION testing.common.b() RETURNS int RETURN testing.common.a()",
                "Invoking a dynamically registered function in SQL function body is not supported");
    }

    public void testInvalidFunctionName()
    {
        assertQueryFails("SELECT x.y(1)", ".*Non-builtin functions must be referenced by 'catalog\\.schema\\.function_name', found: x\\.y");
        assertQueryFails("SELECT x.y.z.w()", ".*Non-builtin functions must be referenced by 'catalog\\.schema\\.function_name', found: x\\.y\\.z\\.w");
    }

    @Test
    public void testSqlFunctions()
    {
        assertQuerySucceeds("CREATE FUNCTION testing.common.array_append(a array<int>, x int)\n" +
                "RETURNS array<int>\n" +
                "RETURN concat(a, array[x])");
        assertQuery("SELECT testing.common.array_append(ARRAY[1, 2, 4], 8)", "SELECT ARRAY[1, 2, 4, 8]");
    }

    @Test
    public void testShowFunctions()
    {
        MaterializedResult initial = computeActual("SHOW FUNCTIONS");

        assertQuerySucceeds("CREATE FUNCTION testing.common.d() RETURNS int RETURN 1");
        assertQuerySucceeds("CREATE FUNCTION testing.test.c() RETURNS int RETURN 1");
        assertQuerySucceeds("CREATE FUNCTION example.example.b() RETURNS int RETURN 1");
        assertQuerySucceeds("CREATE FUNCTION testing.common.a() RETURNS int RETURN 1");
        MaterializedResult expanded = computeActual("SHOW FUNCTIONS");
        int rowCount = expanded.getRowCount();

        assertEquals(rowCount, initial.getRowCount() + 4);
        assertEquals(expanded.getMaterializedRows().subList(0, rowCount - 4), initial.getMaterializedRows());

        List<String> functionNames = expanded.getMaterializedRows().subList(rowCount - 4, rowCount).stream()
                .map(MaterializedRow::getFields)
                .map(list -> list.get(0))
                .map(String.class::cast)
                .collect(toImmutableList());
        assertEquals(functionNames, ImmutableList.of("example.example.b", "testing.common.a", "testing.common.d", "testing.test.c"));
    }

    @Test
    public void testShowFunctionsLike()
    {
        assertQuerySucceeds("CREATE FUNCTION testing.test.foo() RETURNS int RETURN 1");
        assertQuerySucceeds("CREATE FUNCTION testing.test.bar() RETURNS int RETURN 1");
        assertQuerySucceeds("CREATE FUNCTION testing.test.foo_bar() RETURNS int RETURN 1");
        assertQuerySucceeds("CREATE FUNCTION testing.test.fooobar() RETURNS int RETURN 1");

        // Match function names with prefix foo
        MaterializedResult functionsLike = computeActual("SHOW FUNCTIONS LIKE 'testing.test.foo%'");
        List<String> functionNamesLike = functionsLike.getMaterializedRows().stream()
                .map(MaterializedRow::getFields)
                .map(list -> list.get(0))
                .map(String.class::cast)
                .collect(toImmutableList());
        assertEquals(functionNamesLike, ImmutableList.of("testing.test.foo", "testing.test.foo_bar", "testing.test.fooobar"));

        // Match both "foo_bar" and "fooobar" because '_' is treated as a wildcard, not a literal
        MaterializedResult functionsLikeWithoutEscape = computeActual("SHOW FUNCTIONS LIKE 'testing.test.foo_bar'");
        List<String> functionNamesLikeWithoutEscape = functionsLikeWithoutEscape.getMaterializedRows().stream()
                .map(MaterializedRow::getFields)
                .map(list -> list.get(0))
                .map(String.class::cast)
                .collect(toImmutableList());
        assertEquals(functionNamesLikeWithoutEscape, ImmutableList.of("testing.test.foo_bar", "testing.test.fooobar"));

        // Match "foo_bar" but not "fooobar" because '_' is now escaped
        MaterializedResult functionsLikeWithEscape = computeActual("SHOW FUNCTIONS LIKE 'testing.test.foo$_bar' ESCAPE '$'");
        List<String> functionNamesLikeWithEscape = functionsLikeWithEscape.getMaterializedRows().stream()
                .map(MaterializedRow::getFields)
                .map(list -> list.get(0))
                .map(String.class::cast)
                .collect(toImmutableList());
        assertEquals(functionNamesLikeWithEscape, ImmutableList.of("testing.test.foo_bar"));
    }

    public void testShowCreateFunctions()
    {
        @Language("SQL") String createFunctionInt = "CREATE FUNCTION testing.common.array_append(a array<int>, x int)\n" +
                "RETURNS array<int>\n" +
                "RETURN concat(a, array[x])";
        @Language("SQL") String createFunctionDouble = "CREATE FUNCTION testing.common.array_append(a array<double>, x double)\n" +
                "RETURNS array<double>\n" +
                "RETURN concat(a, array[x])";
        @Language("SQL") String createFunctionRand = "CREATE FUNCTION testing.common.rand()\n" +
                "RETURNS double\n" +
                "RETURN rand()";
        String createFunctionIntFormatted = "CREATE FUNCTION testing.common.array_append (\n" +
                "   a ARRAY(integer),\n" +
                "   x integer\n" +
                ")\n" +
                "RETURNS ARRAY(integer)\n" +
                "COMMENT ''\n" +
                "LANGUAGE SQL\n" +
                "NOT DETERMINISTIC\n" +
                "CALLED ON NULL INPUT\n" +
                "RETURN \"concat\"(a, ARRAY[x])";
        String createFunctionDoubleFormatted = "CREATE FUNCTION testing.common.array_append (\n" +
                "   a ARRAY(double),\n" +
                "   x double\n" +
                ")\n" +
                "RETURNS ARRAY(double)\n" +
                "COMMENT ''\n" +
                "LANGUAGE SQL\n" +
                "NOT DETERMINISTIC\n" +
                "CALLED ON NULL INPUT\n" +
                "RETURN \"concat\"(a, ARRAY[x])";
        String createFunctionRandFormatted = "CREATE FUNCTION testing.common.rand ()\n" +
                "RETURNS double\n" +
                "COMMENT ''\n" +
                "LANGUAGE SQL\n" +
                "NOT DETERMINISTIC\n" +
                "CALLED ON NULL INPUT\n" +
                "RETURN \"rand\"()";
        String parameterTypeInt = "ARRAY(integer), integer";
        String parameterTypeDouble = "ARRAY(double), double";

        assertQuerySucceeds(createFunctionInt);
        assertQuerySucceeds(createFunctionDouble);
        assertQuerySucceeds(createFunctionRand);

        MaterializedResult rows = computeActual("SHOW CREATE FUNCTION testing.common.array_append");
        assertEquals(rows.getRowCount(), 2);
        assertEquals(rows.getMaterializedRows().get(0).getFields(), ImmutableList.of(createFunctionDoubleFormatted, parameterTypeDouble));
        assertEquals(rows.getMaterializedRows().get(1).getFields(), ImmutableList.of(createFunctionIntFormatted, parameterTypeInt));

        rows = computeActual("SHOW CREATE FUNCTION testing.common.array_append(array(int), int)");
        assertEquals(rows.getRowCount(), 1);
        assertEquals(rows.getMaterializedRows().get(0).getFields(), ImmutableList.of(createFunctionIntFormatted, parameterTypeInt));

        rows = computeActual("SHOW CREATE FUNCTION testing.common.rand()");
        assertEquals(rows.getMaterializedRows().get(0).getFields(), ImmutableList.of(createFunctionRandFormatted, ""));

        assertQueryFails("SHOW CREATE FUNCTION testing.common.array_append()", "Function not found: testing\\.common\\.array_append\\(\\)");

        assertQueryFails("SHOW CREATE FUNCTION array_agg", "SHOW CREATE FUNCTION is only supported for SQL functions");
        assertQueryFails("SHOW CREATE FUNCTION presto.default.array_agg", "SHOW CREATE FUNCTION is only supported for SQL functions");
    }

    @Test
    void testParameterCaseInsensitive()
    {
        @Language("SQL") String createFunctionInt = "CREATE FUNCTION testing.common.array_append(input array<int>, x int)\n" +
                "RETURNS array<int>\n" +
                "RETURN concat(inPut, array[x])";
        @Language("SQL") String createFunctionDouble = "CREATE FUNCTION testing.common.array_append(inPut array<bigint>, x bigint)\n" +
                "RETURNS array<bigint>\n" +
                "RETURN concat(input, array[x])";
        @Language("SQL") String createFunctionArraySum = "CREATE FUNCTION testing.common.array_sum(INPUT array<bigint>)\n" +
                "RETURNS bigint\n" +
                "RETURN reduce(input, 0, (s, x) -> s + x, s -> s)";
        assertQuerySucceeds(createFunctionInt);
        assertQuerySucceeds(createFunctionDouble);
        assertQuerySucceeds(createFunctionArraySum);

        assertQuery("SELECT testing.common.array_append(array[1, 2, 3], 4)", "VALUES array[1, 2, 3, 4]");
        assertQuery("SELECT testing.common.array_append(array[bigint'1', bigint'2', bigint'3'], bigint'4')", "VALUES array[1L, 2L, 3L, 4L]");
        assertQuery("SELECT testing.common.ARRAY_APPEND(Array, ITEM) FROM (VALUES (array[1, 2, 3], 4), (array[2, 3, 4], 5)) t(array, item)", "VALUES array[1, 2, 3, 4], array[2, 3, 4, 5]");
        assertQuery("SELECT testing.common.array_sum(Array) FROM (VALUES (array[1, 2, 3]), (array[4, 5, 6])) t(array)", "VALUES 6L, 15L");
    }

    @Test
    void testLambdaVariableScoping()
    {
        @Language("SQL") String createFunction = "CREATE FUNCTION testing.test.array_sum(x array<int>)\n" +
                "RETURNS int \n" +
                "RETURN reduce(x, 0, (s, x) -> s + x, s -> s)";
        assertQuerySucceeds(createFunction);

        assertQuery("SELECT testing.test.array_sum(array[1, 2, 3])", "VALUES 6L");
    }

    @Test
    void testUnsupportedRemoteFunctions()
    {
        assertQuerySucceeds("CREATE FUNCTION testing.test.foo(x varchar) RETURNS varchar LANGUAGE JAVA EXTERNAL");
        assertQueryFails("SELECT reduce(a, '', (s, x) -> s || testing.test.foo(x), s -> s) from (VALUES (array['a', 'b'])) t(a)", ".*External functions in Lambda expression is not supported:.*");
    }
}
