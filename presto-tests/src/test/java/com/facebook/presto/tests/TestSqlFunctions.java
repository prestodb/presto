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

import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;
import org.testng.annotations.Test;

public class TestSqlFunctions
        extends AbstractTestQueryFramework
{
    protected TestSqlFunctions()
    {
        super(() -> TpchQueryRunnerBuilder.builder().build());
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
    public void testDropFunctionInvalidFunctionName()
    {
        assertQueryFails(
                "DROP FUNCTION IF EXISTS testing.tan",
                ".*Function name should be in the form of catalog\\.schema\\.function_name, found: testing\\.tan");
        assertQueryFails(
                "DROP FUNCTION presto.default.tan (double)",
                "Cannot drop function in built-in function namespace: presto\\.default\\.tan");
    }
}
