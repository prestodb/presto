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
import java.util.Optional;
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
    public void parseSignatureWithLiterals()
    {
        TypeSignature result = parseTypeSignature("decimal(X,42)", ImmutableSet.of("X"));
        assertEquals(result.getParameters().size(), 2);
        assertEquals(result.getParameters().get(0).isVariable(), true);
        assertEquals(result.getParameters().get(1).isLongLiteral(), true);
    }

    @Test
    public void parseRowSignature()
    {
        // row signature with named fields
        assertRowSignature(
                "row(a bigint,b varchar)",
                rowSignature(namedParameter("a", signature("bigint")), namedParameter("b", varchar())));
        assertRowSignature(
                "row(__a__ bigint,_b@_: _varchar)",
                rowSignature(namedParameter("__a__", signature("bigint")), namedParameter("_b@_:", signature("_varchar"))));
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

        // row with mixed fields
        assertRowSignature(
                "row(bigint,varchar)",
                rowSignature(unnamedParameter(signature("bigint")), unnamedParameter(varchar())));
        assertRowSignature(
                "row(bigint,array(bigint),row(a bigint))",
                rowSignature(
                        unnamedParameter(signature("bigint")),
                        unnamedParameter(array(signature("bigint"))),
                        unnamedParameter(rowSignature(namedParameter("a", signature("bigint"))))));
        assertRowSignature(
                "row(varchar(10),b row(bigint))",
                rowSignature(
                        unnamedParameter(varchar(10)),
                        namedParameter("b", rowSignature(unnamedParameter(signature("bigint"))))));
        assertRowSignature(
                "array(row(col0 bigint,double))",
                array(rowSignature(namedParameter("col0", signature("bigint")), unnamedParameter(signature("double")))));
        assertRowSignature(
                "row(col0 array(row(bigint,double)))",
                rowSignature(namedParameter("col0", array(
                        rowSignature(unnamedParameter(signature("bigint")), unnamedParameter(signature("double")))))));
        assertRowSignature(
                "row(a decimal(p1,s1),decimal(p2,s2))",
                ImmutableSet.of("p1", "s1", "p2", "s2"),
                rowSignature(namedParameter("a", decimal("p1", "s1")), unnamedParameter(decimal("p2", "s2"))));

        // named fields of types with spaces
        assertRowSignature(
                "row(time time with time zone)",
                rowSignature(namedParameter("time", signature("time with time zone"))));
        assertRowSignature(
                "row(time timestamp with time zone)",
                rowSignature(namedParameter("time", signature("timestamp with time zone"))));
        assertRowSignature(
                "row(interval interval day to second)",
                rowSignature(namedParameter("interval", signature("interval day to second"))));
        assertRowSignature(
                "row(interval interval year to month)",
                rowSignature(namedParameter("interval", signature("interval year to month"))));
        assertRowSignature(
                "row(double double precision)",
                rowSignature(namedParameter("double", signature("double precision"))));

        // unnamed fields of types with spaces
        assertRowSignature(
                "row(time with time zone)",
                rowSignature(unnamedParameter(signature("time with time zone"))));
        assertRowSignature(
                "row(timestamp with time zone)",
                rowSignature(unnamedParameter(signature("timestamp with time zone"))));
        assertRowSignature(
                "row(interval day to second)",
                rowSignature(unnamedParameter(signature("interval day to second"))));
        assertRowSignature(
                "row(interval year to month)",
                rowSignature(unnamedParameter(signature("interval year to month"))));
        assertRowSignature(
                "row(double precision)",
                rowSignature(unnamedParameter(signature("double precision"))));
        assertRowSignature(
                "row(array(time with time zone))",
                rowSignature(unnamedParameter(array(signature("time with time zone")))));
        assertRowSignature(
                "row(map(timestamp with time zone,interval day to second))",
                rowSignature(unnamedParameter(map(signature("timestamp with time zone"), signature("interval day to second")))));

        // quoted field names
        assertRowSignature(
                "row(\"time with time zone\" time with time zone,\"double\" double)",
                rowSignature(
                        namedParameter("time with time zone", signature("time with time zone")),
                        namedParameter("double", signature("double"))));

        // allow spaces
        assertRowSignature(
                "row( time  time with time zone)",
                rowSignature(
                        namedParameter("time", signature("time with time zone"))));

        // preserve base name case
        assertRowSignature(
                "RoW(a bigint,b varchar)",
                rowSignature(namedParameter("a", signature("bigint")), namedParameter("b", varchar())));

        // field type canonicalization
        assertEquals(parseTypeSignature("row(col iNt)"), parseTypeSignature("row(col integer)"));
        assertEquals(parseTypeSignature("row(a Int(p1))"), parseTypeSignature("row(a integer(p1))"));

        // signature with invalid type
        assertRowSignature(
                "row(\"time\" with time zone)",
                rowSignature(namedParameter("time", signature("with time zone"))));
    }

    private TypeSignature varchar()
    {
        return new TypeSignature(StandardTypes.VARCHAR, TypeSignatureParameter.of(VarcharType.UNBOUNDED_LENGTH));
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
        return new NamedTypeSignature(Optional.of(new RowFieldName(name)), value);
    }

    private static NamedTypeSignature unnamedParameter(TypeSignature value)
    {
        return new NamedTypeSignature(Optional.empty(), value);
    }

    private static TypeSignature array(TypeSignature type)
    {
        return new TypeSignature(StandardTypes.ARRAY, TypeSignatureParameter.of(type));
    }

    private static TypeSignature map(TypeSignature keyType, TypeSignature valueType)
    {
        return new TypeSignature(StandardTypes.MAP, TypeSignatureParameter.of(keyType), TypeSignatureParameter.of(valueType));
    }

    private TypeSignature signature(String name)
    {
        return new TypeSignature(name);
    }

    @Test
    public void parseSignature()
    {
        assertSignature("boolean", "boolean", ImmutableList.of());
        assertSignature("varchar", "varchar", ImmutableList.of(Integer.toString(VarcharType.UNBOUNDED_LENGTH)));
        assertEquals(parseTypeSignature("int"), parseTypeSignature("integer"));

        assertSignature("array(bigint)", "array", ImmutableList.of("bigint"));
        assertEquals(parseTypeSignature("array(int)"), parseTypeSignature("array(integer)"));

        assertSignature("array(array(bigint))", "array", ImmutableList.of("array(bigint)"));
        assertEquals(parseTypeSignature("array(array(int))"), parseTypeSignature("array(array(integer))"));
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
        assertSignature("rowxxx(a)", "rowxxx", ImmutableList.of("a"));
    }

    @Test
    public void parseWithLiteralParameters()
    {
        assertSignature("foo(42)", "foo", ImmutableList.of("42"));
        assertSignature("varchar(10)", "varchar", ImmutableList.of("10"));
    }

    @Test
    public void testVarchar()
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
        assertEquals(parseTypeSignature(signature.toString(), literalParameters), expectedSignature);
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
