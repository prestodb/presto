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
            queryRunner.enableTestFunctionNamespaces(ImmutableList.of("testing", "example"));
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
                "CREATE FUNCTION testing.default.tan (x int) RETURNS varchar COMMENT 'tangent trigonometric function' RETURN sin(x) / cos(x)",
                "Function implementation type 'double' does not match declared return type 'varchar'");
        assertQueryFails(
                "CREATE FUNCTION testing.default.tan (x int) RETURNS varchar COMMENT 'tangent trigonometric function' RETURN sin(y) / cos(y)",
                ".*Column 'y' cannot be resolved");
        assertQueryFails(
                "CREATE FUNCTION testing.default.tan (x double) RETURNS double COMMENT 'tangent trigonometric function' RETURN sum(x)",
                ".*CREATE FUNCTION body cannot contain aggregations, window functions or grouping operations:.*");
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
}
