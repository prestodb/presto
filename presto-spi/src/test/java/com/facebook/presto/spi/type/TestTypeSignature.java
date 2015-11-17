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
package com.facebook.presto.spi.type;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestTypeSignature
{
    @Test
    public void testBindParameters()
            throws Exception
    {
        Map<String, Type> boundParameters = ImmutableMap.of("T1", VarcharType.VARCHAR, "T2", BigintType.BIGINT);

        assertBindSignature("bigint", boundParameters, "bigint");
        assertBindSignature("T1", boundParameters, "varchar");
        assertBindSignature("T2", boundParameters, "bigint");
        assertBindSignature("array(T1)", boundParameters, "array(varchar)");
        assertBindSignature("array<T1>", boundParameters, "array(varchar)");
        assertBindSignature("map(T1,T2)", boundParameters, "map(varchar,bigint)");
        assertBindSignature("map<T1,T2>", boundParameters, "map(varchar,bigint)");
        assertBindSignature("row<T1,T2>('a','b')", boundParameters, "row<varchar,bigint>('a','b')");
        assertBindSignature("bla(T1,42,T2)", boundParameters, "bla(varchar,42,bigint)");

        assertBindSignatureFails("T1(bigint)", boundParameters, "Unbounded parameters can not have parameters");
    }

    private void assertBindSignatureFails(String typeName, Map<String, Type> boundParameters, String reason)
    {
        try {
            parseTypeSignature(typeName).bindParameters(boundParameters);
            fail(reason);
        }
        catch (RuntimeException e) {
            // Expected
        }
    }

    private void assertBindSignature(String typeName, Map<String, Type> boundParameters, String expectedTypeName)
    {
        assertEquals(parseTypeSignature(typeName).bindParameters(boundParameters).toString(), expectedTypeName);
    }

    @Test
    public void parseRowSignature()
            throws Exception
    {
        assertRowSignature(
                "row<bigint,varchar>('a','b')",
                "row",
                ImmutableList.of("bigint", "varchar"),
                ImmutableList.of("a", "b"));
        assertRowSignature(
                "row<bigint,array(bigint),row<bigint>('a')>('a','b','c')",
                "row",
                ImmutableList.of("bigint", "array(bigint)", "row<bigint>('a')"),
                ImmutableList.of("a", "b", "c"));
        assertRowSignature(
                "row<varchar(10),row<bigint>('a')>('a','b')",
                "row",
                ImmutableList.of("varchar(10)", "row<bigint>('a')"),
                ImmutableList.of("a", "b"));
        assertRowSignature(
                "array(row<bigint,double>('col0','col1'))",
                "array",
                ImmutableList.of("row<bigint,double>('col0','col1')"),
                ImmutableList.of());
        assertRowSignature(
                "row<array(row<bigint,double>('col0','col1'))>('col0')",
                "row",
                ImmutableList.of("array(row<bigint,double>('col0','col1'))"),
                ImmutableList.of("col0"));
    }

    @Test
    public void parseSignature()
            throws Exception
    {
        assertSignature("bigint", "bigint", ImmutableList.<String>of());
        assertSignature("boolean", "boolean", ImmutableList.<String>of());
        assertSignature("varchar", "varchar", ImmutableList.<String>of());

        assertSignature("array(bigint)", "array", ImmutableList.of("bigint"));
        assertSignature("array(array(bigint))", "array", ImmutableList.of("array(bigint)"));
        assertSignature(
                "array(timestamp with time zone)",
                "array",
                ImmutableList.of("timestamp with time zone"));

        assertSignature(
                "map(bigint,bigint)",
                "map",
                ImmutableList.of("bigint", "bigint"));
        assertSignature(
                "map(bigint,array(bigint))",
                "map", ImmutableList.of("bigint", "array(bigint)"));
        assertSignature(
                "map(bigint,map(bigint,map(varchar,bigint)))",
                "map",
                ImmutableList.of("bigint", "map(bigint,map(varchar,bigint))"));

        assertSignatureFail("blah()");
        assertSignatureFail("array()");
        assertSignatureFail("map()");
    }

    @Test
    public void parseWithLiteralParameters()
    {
        assertSignature("foo(42)", "foo", ImmutableList.<String>of("42"));
        assertSignature("varchar(10)", "varchar", ImmutableList.<String>of("10"));
    }

    private static void assertRowSignature(
            String typeName,
            String base,
            List<String> parameters,
            List<Object> literalParameters)
    {
        assertSignature(typeName, base, parameters, literalParameters, typeName);
    }

    private static void assertSignature(String typeName, String base, List<String> parameters)
    {
        assertSignature(typeName, base, parameters, ImmutableList.of(), typeName.replace("<", "(").replace(">", ")"));
    }

    private static void assertSignature(
            String typeName,
            String base,
            List<String> parameters,
            List<Object> literalParameters,
            String expectedTypeName)
    {
        TypeSignature signature = parseTypeSignature(typeName);
        assertEquals(signature.getBase(), base);
        assertEquals(signature.getParameters().size(), parameters.size());
        for (int i = 0; i < signature.getParameters().size(); i++) {
            assertEquals(signature.getParameters().get(i).toString(), parameters.get(i));
        }
        assertEquals(signature.getLiteralParameters(), literalParameters);
        assertEquals(signature.toString(), expectedTypeName);
    }

    private void assertSignatureFail(String typeName)
    {
        try {
            parseTypeSignature(typeName);
            fail("Type signatures with zero parameters should fail to parse");
        }
        catch (RuntimeException e) {
            // Expected
        }
    }
}
