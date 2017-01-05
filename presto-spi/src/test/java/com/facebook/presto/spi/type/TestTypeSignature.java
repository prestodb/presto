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
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.google.common.collect.Lists.transform;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestTypeSignature
{
    @Test
    public void parseSignatureWithLiterals() throws Exception
    {
        TypeSignature result = parseTypeSignature("decimal(X,42)", ImmutableSet.of("X"));
        assertEquals(result.getParameters().size(), 2);
        assertEquals(result.getParameters().get(0).isVariable(), true);
        assertEquals(result.getParameters().get(1).isLongLiteral(), true);
    }

    @Test
    public void parseRowSignature()
            throws Exception
    {
        assertRowSignature(
                "row(a bigint,b varchar)",
                rowSignature(namedParameter("a", signature("bigint")), namedParameter("b", varchar())));
        assertRowSignature(
                "ROW(a bigint,b varchar)",
                "ROW",
                ImmutableList.of("a bigint", "b varchar"),
                "row(a bigint,b varchar)");
        assertRowSignature(
                "row(a bigint,b array(bigint),c row(a bigint))",
                rowSignature(
                        namedParameter("a", signature("bigint")),
                        namedParameter("b", array(signature("bigint"))),
                        namedParameter("c", rowSignature(namedParameter("a", signature("bigint"))))));
        assertRowSignature(
                "row(a varchar(10),b row(a bigint))",
                rowSignature(
                        namedParameter("a", varchar(10)),
                        namedParameter("b", rowSignature(namedParameter("a", signature("bigint"))))));
        assertRowSignature(
                "array(row(col0 bigint,col1 double))",
                array(rowSignature(namedParameter("col0", signature("bigint")), namedParameter("col1", signature("double")))));
        assertRowSignature(
                "row(col0 array(row(col0 bigint,col1 double)))",
                rowSignature(namedParameter("col0", array(
                        rowSignature(namedParameter("col0", signature("bigint")), namedParameter("col1", signature("double")))))));
        assertRowSignature(
                "row(a decimal(p1,s1),b decimal(p2,s2))",
                ImmutableSet.of("p1", "s1", "p2", "s2"),
                rowSignature(namedParameter("a", decimal("p1", "s1")), namedParameter("b", decimal("p2", "s2"))));

        // TODO: remove the following tests when the old style row type has been completely dropped
        assertOldRowSignature(
                "row<bigint,varchar>('a','b')",
                rowSignature(namedParameter("a", signature("bigint")), namedParameter("b", varchar())));
        assertOldRowSignature(
                "row<bigint,array(bigint),row<bigint>('a')>('a','b','c')",
                rowSignature(
                        namedParameter("a", signature("bigint")),
                        namedParameter("b", array(signature("bigint"))),
                        namedParameter("c", rowSignature(namedParameter("a", signature("bigint"))))));
        assertOldRowSignature(
                "row<varchar(10),row<bigint>('a')>('a','b')",
                rowSignature(
                        namedParameter("a", varchar(10)),
                        namedParameter("b", rowSignature(namedParameter("a", signature("bigint"))))));
        assertOldRowSignature(
                "array(row<bigint,double>('col0','col1'))",
                array(rowSignature(namedParameter("col0", signature("bigint")), namedParameter("col1", signature("double")))));
        assertOldRowSignature(
                "row<array(row<bigint,double>('col0','col1'))>('col0')",
                rowSignature(namedParameter("col0", array(
                        rowSignature(namedParameter("col0", signature("bigint")), namedParameter("col1", signature("double")))))));
        assertOldRowSignature(
                "row<decimal(p1,s1),decimal(p2,s2)>('a','b')",
                ImmutableSet.of("p1", "s1", "p2", "s2"),
                rowSignature(namedParameter("a", decimal("p1", "s1")), namedParameter("b", decimal("p2", "s2"))));
    }

    private TypeSignature varchar()
    {
        return new TypeSignature(StandardTypes.VARCHAR);
    }

    private TypeSignature varchar(long length)
    {
        return new TypeSignature(StandardTypes.VARCHAR, TypeSignatureParameter.of(length));
    }

    private TypeSignature decimal(String precisionVariable, String scaleVariable)
    {
        return new TypeSignature(StandardTypes.DECIMAL, ImmutableList.of(
                TypeSignatureParameter.of(precisionVariable), TypeSignatureParameter.of(scaleVariable)));
    }

    private static TypeSignature rowSignature(NamedTypeSignature... columns)
    {
        return new TypeSignature("row", transform(asList(columns), TypeSignatureParameter::of));
    }

    private static NamedTypeSignature namedParameter(String name, TypeSignature value)
    {
        return new NamedTypeSignature(name, value);
    }

    private static TypeSignature array(TypeSignature type)
    {
        return new TypeSignature(StandardTypes.ARRAY, TypeSignatureParameter.of(type));
    }

    private TypeSignature signature(String name)
    {
        return new TypeSignature(name);
    }

    @Test
    public void parseSignature()
            throws Exception
    {
        assertSignature("bigint", "bigint", ImmutableList.of());
        assertSignature("boolean", "boolean", ImmutableList.of());
        assertSignature("varchar", "varchar", ImmutableList.of());

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
        assertSignatureFail("x", ImmutableSet.of("x"));

        // ensure this is not treated as a row type
        assertSignature("rowxxx<a>", "rowxxx", ImmutableList.of("a"));
    }

    @Test
    public void parseWithLiteralParameters()
    {
        assertSignature("foo(42)", "foo", ImmutableList.of("42"));
        assertSignature("varchar(10)", "varchar", ImmutableList.of("10"));
    }

    @Test
    public void testVarchar()
            throws Exception
    {
        assertEquals(VARCHAR.getTypeSignature().toString(), "varchar");
        assertEquals(createVarcharType(42).getTypeSignature().toString(), "varchar(42)");
        assertEquals(parseTypeSignature("varchar"), createUnboundedVarcharType().getTypeSignature());
        assertEquals(createUnboundedVarcharType().getTypeSignature(), parseTypeSignature("varchar"));
        assertEquals(parseTypeSignature("varchar").hashCode(), createUnboundedVarcharType().getTypeSignature().hashCode());
        assertNotEquals(createUnboundedVarcharType().getTypeSignature(), parseTypeSignature("varchar(10)"));
    }

    @Test
    public void testIsCalculated()
            throws Exception
    {
        assertFalse(parseTypeSignature("bigint").isCalculated());
        assertTrue(parseTypeSignature("decimal(p, s)", ImmutableSet.of("p", "s")).isCalculated());
        assertFalse(parseTypeSignature("decimal(2, 1)").isCalculated());
        assertTrue(parseTypeSignature("array(decimal(p, s))", ImmutableSet.of("p", "s")).isCalculated());
        assertFalse(parseTypeSignature("array(decimal(2, 1))").isCalculated());
        assertTrue(parseTypeSignature("map(decimal(p1, s1),decimal(p2, s2))", ImmutableSet.of("p1", "s1", "p2", "s2")).isCalculated());
        assertFalse(parseTypeSignature("map(decimal(2, 1),decimal(3, 1))").isCalculated());
        assertTrue(parseTypeSignature("row(a decimal(p1,s1),b decimal(p2,s2))", ImmutableSet.of("p1", "s1", "p2", "s2")).isCalculated());
        assertFalse(parseTypeSignature("row(a decimal(2,1),b decimal(3,2))").isCalculated());
    }

    private static void assertRowSignature(
            String typeName,
            Set<String> literalParameters,
            TypeSignature expectedSignature)
    {
        TypeSignature signature = parseTypeSignature(typeName, literalParameters);
        assertEquals(signature, expectedSignature);
        assertEquals(signature.toString(), typeName);
    }

    private static void assertRowSignature(
            String typeName,
            TypeSignature expectedSignature)
    {
        assertRowSignature(typeName, ImmutableSet.of(), expectedSignature);
    }

    private static void assertSignature(String typeName, String base, List<String> parameters)
    {
        assertSignature(typeName, base, parameters, typeName.replace("<", "(").replace(">", ")"));
    }

    private static void assertRowSignature(
            String typeName,
            String base,
            List<String> parameters,
            String expected)
    {
        assertSignature(typeName, base, parameters, expected);
    }

    // TODO: remove this when old style row type is removed
    @Deprecated
    private static void assertOldRowSignature(
            String typeName,
            Set<String> literalParameters,
            TypeSignature expectedSignature)
    {
        TypeSignature signature = parseTypeSignature(typeName, literalParameters);
        assertEquals(signature, expectedSignature);
    }

    // TODO: remove this when old style row type is removed
    @Deprecated
    private static void assertOldRowSignature(
            String typeName,
            TypeSignature expectedSignature)
    {
        assertOldRowSignature(typeName, ImmutableSet.of(), expectedSignature);
    }

    private static void assertSignature(
            String typeName,
            String base,
            List<String> parameters,
            String expectedTypeName)
    {
        TypeSignature signature = parseTypeSignature(typeName);
        assertEquals(signature.getBase(), base);
        assertEquals(signature.getParameters().size(), parameters.size());
        for (int i = 0; i < signature.getParameters().size(); i++) {
            assertEquals(signature.getParameters().get(i).toString(), parameters.get(i));
        }
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

    private void assertSignatureFail(String typeName, Set<String> literalCalculationParameters)
    {
        try {
            parseTypeSignature(typeName, literalCalculationParameters);
            fail("Type signatures with zero parameters should fail to parse");
        }
        catch (RuntimeException e) {
            // Expected
        }
    }
}
