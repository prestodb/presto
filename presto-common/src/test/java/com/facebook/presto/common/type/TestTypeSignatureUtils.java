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
package com.facebook.presto.common.type;

import com.facebook.presto.common.QualifiedObjectName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.TypeSignatureUtils.resolveIntermediateType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestTypeSignatureUtils
{
    @Test
    public void testResolveIntermediateTypeNonGenericBigintType()
    {
        TypeSignature actualIntermediateType = parseTypeSignature("bigint");
        TypeSignature expectedIntermediateType = parseTypeSignature("bigint");
        TypeSignature resolvedIntermediateType = resolveIntermediateType(
                actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);
    }

    @Test
    public void testResolveIntermediateTypeNonGenericDifferentArgTypes()
    {
        TypeSignature actualIntermediateType = parseTypeSignature("bigint");
        TypeSignature expectedIntermediateType = parseTypeSignature("bigint");
        TypeSignature resolvedIntermediateType = resolveIntermediateType(
                actualIntermediateType,
                ImmutableList.of(parseTypeSignature("double")),
                ImmutableList.of(parseTypeSignature("double")));
        assertEquals(resolvedIntermediateType, expectedIntermediateType);
    }

    @Test
    public void testResolveIntermediateTypeGenericBigintType()
    {
        TypeSignature actualIntermediateType = parseTypeSignature("T");
        TypeSignature expectedIntermediateType = parseTypeSignature("bigint");
        TypeSignature resolvedIntermediateType = resolveIntermediateType(
                actualIntermediateType,
                ImmutableList.of(actualIntermediateType),
                ImmutableList.of(expectedIntermediateType));
        assertEquals(resolvedIntermediateType, expectedIntermediateType);
    }

    @Test
    public void testResolveIntermediateTypeGenericBigintArgType()
    {
        TypeSignature actualIntermediateType = parseTypeSignature("test(T)");
        TypeSignature expectedIntermediateType = parseTypeSignature("test(bigint)");
        TypeSignature resolvedIntermediateType = resolveIntermediateType(
                actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);
    }

    @Test
    public void testResolveIntermediateTypeGenericMultipleArgs()
    {
        TypeSignature actualIntermediateType = parseTypeSignature("test(array(T),boolean,double,integer,E,array(T))");
        TypeSignature expectedIntermediateType = parseTypeSignature("test(array(bigint),boolean,double,integer,varchar(100), array(bigint))");
        TypeSignature resolvedIntermediateType = resolveIntermediateType(
                actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);
    }

    @Test
    public void testResolveIntermediateTypeNonGenericRowType()
    {
        TypeSignature actualIntermediateType = parseTypeSignature("row(bigint)");
        TypeSignature expectedIntermediateType = parseTypeSignature("row(bigint)");
        TypeSignature resolvedIntermediateType = resolveIntermediateType(
                actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);
    }

    @Test
    public void testResolveIntermediateTypeGenericRowType()
    {
        TypeSignature actualIntermediateType = parseTypeSignature("row(T)");
        TypeSignature expectedIntermediateType = parseTypeSignature("row(bigint)");
        TypeSignature resolvedIntermediateType = resolveIntermediateType(
                actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);
    }

    @Test
    public void testResolveIntermediateTypeGenericRowArgTypeSingleParam()
    {
        TypeSignature actualIntermediateType = parseTypeSignature("test(row(T))");
        TypeSignature expectedIntermediateType = parseTypeSignature("test(row(bigint))");
        TypeSignature resolvedIntermediateType = resolveIntermediateType(
                actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);
    }

    @Test
    public void testResolveIntermediateTypeGenericRowArgTypeMultipleParams()
    {
        TypeSignature actualIntermediateType = parseTypeSignature("test(row(array(T),boolean,double,integer,E,array(T)))");
        TypeSignature expectedIntermediateType = parseTypeSignature("test(row(array(bigint),boolean,double,integer,varchar(100), array(bigint)))");
        TypeSignature resolvedIntermediateType = resolveIntermediateType(
                actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);
    }

    @Test
    public void testResolveIntermediateTypeGenericRowTypeMultipleParams()
    {
        TypeSignature actualIntermediateType = parseTypeSignature("row(array(T),boolean,double,integer,E,array(T),array(integer))");
        TypeSignature expectedIntermediateType = parseTypeSignature("row(array(bigint),boolean,double,integer,varchar(100),array(bigint),array(integer))");
        TypeSignature resolvedIntermediateType = resolveIntermediateType(
                actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);
    }

    @Test
    public void testResolveIntermediateTypeNonGenericDecimalType()
    {
        TypeSignature actualIntermediateType = parseTypeSignature("decimal(15, 2)");
        TypeSignature expectedIntermediateType = parseTypeSignature("test(decimal(15, 2))");
        List<TypeSignature> expectedIntermediateTypeParameters = expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures();
        TypeSignature resolvedIntermediateType = resolveIntermediateType(
                actualIntermediateType,
                ImmutableList.of(actualIntermediateType),
                expectedIntermediateTypeParameters);
        assertFalse(expectedIntermediateTypeParameters.isEmpty());
        assertEquals(resolvedIntermediateType, expectedIntermediateTypeParameters.get(0));
    }

    @Test
    public void testResolveIntermediateTypeNonGenericDecimalArgType()
    {
        TypeSignature actualIntermediateType = parseTypeSignature("test(decimal(15, 2))");
        TypeSignature expectedIntermediateType = parseTypeSignature("test(decimal(15, 2))");
        TypeSignature resolvedIntermediateType = resolveIntermediateType(
                actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);
    }

    @Test
    public void testResolveIntermediateTypeGenericMapArgType()
    {
        TypeSignature actualIntermediateType = parseTypeSignature("map(T, decimal(15, 2))");
        TypeSignature expectedIntermediateType = parseTypeSignature("map(integer, decimal(15, 2))");
        TypeSignature resolvedIntermediateType = resolveIntermediateType(
                actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);
    }

    @Test
    public void testResolveIntermediateTypeGenericMapArgTypeMapParam()
    {
        TypeSignature actualIntermediateType = parseTypeSignature("map(map(array(K), V), decimal(15, 2))");
        TypeSignature expectedIntermediateType = parseTypeSignature("map(map(array(double), decimal(15, 2)), decimal(15, 2))");
        TypeSignature resolvedIntermediateType = resolveIntermediateType(
                actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);
    }

    @Test
    public void testResolveIntermediateTypeGenericRowTypeMapParam()
    {
        TypeSignature actualIntermediateType = parseTypeSignature("row(map(map(array(K), V), decimal(15, 2)))");
        TypeSignature expectedIntermediateType = parseTypeSignature("row(map(map(array(double), varchar(50)), decimal(15, 2)))");
        TypeSignature resolvedIntermediateType = resolveIntermediateType(
                actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);
    }

    @Test
    public void testResolveIntermediateTypeNonGenericRowTypeMapParam()
    {
        TypeSignature actualIntermediateType = parseTypeSignature("row(map(map(array(double), varchar(50)), decimal(15, 2)))");
        TypeSignature expectedIntermediateType = parseTypeSignature("row(map(map(array(double), varchar(50)), decimal(15, 2)))");
        TypeSignature resolvedIntermediateType = resolveIntermediateType(
                actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);
    }

    @Test
    public void testResolveIntermediateTypeGenericFunctionType()
    {
        TypeSignature actualIntermediateType = parseTypeSignature("S");
        TypeSignature expectedIntermediateType = parseTypeSignature("array(bigint)");
        List<TypeSignature> actualIntermediateTypeParams =
                parseTypeSignature("test(T, S, function(S, T, S), function(S, S, S))").getTypeOrNamedTypeParametersAsTypeSignatures();
        List<TypeSignature> expectedIntermediateTypeParams =
                parseTypeSignature(
                        "test(integer, array(bigint), function(array(bigint), integer, array(bigint)), " +
                                "function(array(bigint), array(bigint), array(bigint)))").getTypeOrNamedTypeParametersAsTypeSignatures();
        TypeSignature resolvedIntermediateType = resolveIntermediateType(
                actualIntermediateType,
                actualIntermediateTypeParams,
                expectedIntermediateTypeParams);
        assertEquals(resolvedIntermediateType, expectedIntermediateType);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "Parameters size: 7 and argumentTypes size: 6 do not match !")
    public void testFailingResolveIntermediateTypeExtraParams()
    {
        TypeSignature actualIntermediateType =
                parseTypeSignature("row(array(T),boolean,double,integer,E,array(T),array(integer))");
        TypeSignature expectedIntermediateType =
                parseTypeSignature("row(array(bigint),boolean,double,integer,varchar(100),array(bigint))");
        TypeSignature resolvedIntermediateType =
                resolveIntermediateType(actualIntermediateType,
                        actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                        expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "Parameters size: 6 and argumentTypes size: 7 do not match !")
    public void testFailingResolveIntermediateTypeExtraArgs()
    {
        TypeSignature actualIntermediateType =
                parseTypeSignature("row(array(T),boolean,double,integer,E,array(T))");
        TypeSignature expectedIntermediateType =
                parseTypeSignature("row(array(bigint),boolean,double,integer,varchar(100),array(bigint), array(integer))");
        TypeSignature resolvedIntermediateType =
                resolveIntermediateType(actualIntermediateType,
                        actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                        expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);
    }

    @Test
    public void testResolveIntermediateTypeGenericEnumParametricType()
    {
        TypeSignature actualIntermediateType =
                parseTypeSignature("test(T)");
        TypeSignature test = new TypeSignature(
                StandardTypes.MAP,
                TypeSignatureParameter.of((new VarcharEnumType(new VarcharEnumType.VarcharEnumMap("test.enum.my_enum", ImmutableMap.of("k", "v)))"))).getTypeSignature())),
                TypeSignatureParameter.of(new BigintEnumType(new BigintEnumType.LongEnumMap("test.enum.my_enum_2", ImmutableMap.of("k", 1L))).getTypeSignature()));
        TypeSignature expectedIntermediateType = new TypeSignature(
                "test",
                TypeSignatureParameter.of(test));
        TypeSignature resolvedIntermediateType =
                resolveIntermediateType(actualIntermediateType,
                        actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                        expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);
    }

    @Test
    public void testResolveIntermediateTypeNonGenericEnumParametricType()
    {
        TypeSignature actualIntermediateType = new
                TypeSignature("test",
                TypeSignatureParameter.of(
                        new TypeSignature(
                                StandardTypes.MAP,
                                TypeSignatureParameter.of((new VarcharEnumType(new VarcharEnumType.VarcharEnumMap("test.enum.my_enum", ImmutableMap.of("k", "v)))"))).getTypeSignature())),
                                TypeSignatureParameter.of(new BigintEnumType(new BigintEnumType.LongEnumMap("test.enum.my_enum_2", ImmutableMap.of("k", 1L))).getTypeSignature()))));

        TypeSignature expectedIntermediateType = new
                TypeSignature("test",
                TypeSignatureParameter.of(
                        new TypeSignature(
                                StandardTypes.MAP,
                                TypeSignatureParameter.of((new VarcharEnumType(new VarcharEnumType.VarcharEnumMap("test.enum.my_enum", ImmutableMap.of("k", "v)))"))).getTypeSignature())),
                                TypeSignatureParameter.of(new BigintEnumType(new BigintEnumType.LongEnumMap("test.enum.my_enum_2", ImmutableMap.of("k", 1L))).getTypeSignature()))));

        TypeSignature resolvedIntermediateType =
                resolveIntermediateType(actualIntermediateType,
                        actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                        expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);
    }

    @Test
    public void testResolveIntermediateTypeNonGenericRowEnumParametricType()
    {
        TypeSignature actualIntermediateType = new
                TypeSignature("row",
                TypeSignatureParameter.of(new NamedTypeSignature(Optional.empty(), parseTypeSignature("double"))),
                TypeSignatureParameter.of(
                        new NamedTypeSignature(
                                Optional.empty(),
                                new TypeSignature(
                                        StandardTypes.MAP,
                                        TypeSignatureParameter.of((new VarcharEnumType(new VarcharEnumType.VarcharEnumMap("test.enum.my_enum", ImmutableMap.of("k", "v)))"))).getTypeSignature())),
                                        TypeSignatureParameter.of(new BigintEnumType(new BigintEnumType.LongEnumMap("test.enum.my_enum_2", ImmutableMap.of("k", 1L))).getTypeSignature())))));

        TypeSignature expectedIntermediateType = new
                TypeSignature("row",
                TypeSignatureParameter.of(new NamedTypeSignature(Optional.empty(), parseTypeSignature("double"))),
                TypeSignatureParameter.of(
                        new NamedTypeSignature(
                                Optional.empty(),
                                new TypeSignature(
                                        StandardTypes.MAP,
                                        TypeSignatureParameter.of((new VarcharEnumType(new VarcharEnumType.VarcharEnumMap("test.enum.my_enum", ImmutableMap.of("k", "v)))"))).getTypeSignature())),
                                        TypeSignatureParameter.of(new BigintEnumType(new BigintEnumType.LongEnumMap("test.enum.my_enum_2", ImmutableMap.of("k", 1L))).getTypeSignature())))));

        TypeSignature resolvedIntermediateType =
                resolveIntermediateType(actualIntermediateType,
                        actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                        expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);
    }

    @Test
    public void testResolveIntermediateTypeGenericRowEnumParametricType()
    {
        TypeSignature actualIntermediateType = new
                TypeSignature("row",
                TypeSignatureParameter.of(new NamedTypeSignature(Optional.empty(), parseTypeSignature("K"))),
                TypeSignatureParameter.of(
                        new NamedTypeSignature(
                                Optional.empty(),
                                new TypeSignature(
                                        StandardTypes.MAP,
                                        TypeSignatureParameter.of((new VarcharEnumType(new VarcharEnumType.VarcharEnumMap("test.enum.my_enum", ImmutableMap.of("k", "v)))"))).getTypeSignature())),
                                        TypeSignatureParameter.of(parseTypeSignature("T"))))));

        TypeSignature expectedIntermediateType = new
                TypeSignature("row",
                TypeSignatureParameter.of(new NamedTypeSignature(Optional.empty(), parseTypeSignature("array(double)"))),
                TypeSignatureParameter.of(
                        new NamedTypeSignature(
                                Optional.empty(),
                                new TypeSignature(
                                        StandardTypes.MAP,
                                        TypeSignatureParameter.of((new VarcharEnumType(new VarcharEnumType.VarcharEnumMap("test.enum.my_enum", ImmutableMap.of("k", "v)))"))).getTypeSignature())),
                                        TypeSignatureParameter.of(new BigintEnumType(new BigintEnumType.LongEnumMap("test.enum.my_enum_2", ImmutableMap.of("k", 1L))).getTypeSignature())))));

        TypeSignature resolvedIntermediateType =
                resolveIntermediateType(actualIntermediateType,
                        actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                        expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);
    }

    @Test
    public void testResolveIntermediateTypeNonGenericDistinctParametricType()
    {
        TypeSignature actualIntermediateType = new
                TypeSignature("test",
                TypeSignatureParameter.of(parseTypeSignature("bigint")),
                TypeSignatureParameter.of(
                        createTypeSignatureWithDistinctParameterKind("test.dt.int00", Optional.empty(), "integer")));

        TypeSignature expectedIntermediateType = new
                TypeSignature("test",
                TypeSignatureParameter.of(parseTypeSignature("bigint")),
                TypeSignatureParameter.of(
                        createTypeSignatureWithDistinctParameterKind("test.dt.int00", Optional.empty(), "integer")));
        TypeSignature resolvedIntermediateType =
                resolveIntermediateType(actualIntermediateType,
                        actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                        expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);
    }

    @Test
    public void testResolveIntermediateTypeGenericDistinctParametricType()
    {
        TypeSignature actualIntermediateType = new
                TypeSignature("test",
                TypeSignatureParameter.of(parseTypeSignature("K")),
                TypeSignatureParameter.of(parseTypeSignature("V")));

        TypeSignature expectedIntermediateType = new
                TypeSignature("test",
                TypeSignatureParameter.of(parseTypeSignature("array(double)")),
                TypeSignatureParameter.of(
                        createTypeSignatureWithDistinctParameterKind("test.dt.int00", Optional.empty(), "bigint")));
        TypeSignature resolvedIntermediateType =
                resolveIntermediateType(actualIntermediateType,
                        actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                        expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);
    }

    @Test
    public void testResolveIntermediateTypeNonGenericRowDistinctParametricType()
    {
        TypeSignature actualIntermediateType = new
                TypeSignature("row",
                TypeSignatureParameter.of(new NamedTypeSignature(Optional.empty(), parseTypeSignature("K"))),
                TypeSignatureParameter.of(
                        new NamedTypeSignature(Optional.empty(), createTypeSignatureWithDistinctParameterKind("test.dt.int00", Optional.empty(), "integer"))));

        TypeSignature expectedIntermediateType = new
                TypeSignature("row",
                TypeSignatureParameter.of(new NamedTypeSignature(Optional.empty(), parseTypeSignature("array(double)"))),
                TypeSignatureParameter.of(
                        new NamedTypeSignature(Optional.empty(), createTypeSignatureWithDistinctParameterKind("test.dt.int00", Optional.empty(), "integer"))));
        TypeSignature resolvedIntermediateType =
                resolveIntermediateType(actualIntermediateType,
                        actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                        expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);
    }

    @Test
    public void testResolveIntermediateTypeGenericRowDistinctParametricType()
    {
        TypeSignature actualIntermediateType = new
                TypeSignature("row",
                TypeSignatureParameter.of(new NamedTypeSignature(Optional.empty(), parseTypeSignature("K"))),
                TypeSignatureParameter.of(new NamedTypeSignature(Optional.empty(), parseTypeSignature("V"))));

        TypeSignature expectedIntermediateType = new
                TypeSignature("row",
                TypeSignatureParameter.of(new NamedTypeSignature(Optional.empty(), parseTypeSignature("array(double)"))),
                TypeSignatureParameter.of(
                        new NamedTypeSignature(Optional.empty(), createTypeSignatureWithDistinctParameterKind("test.dt.int00", Optional.empty(), "integer"))));
        TypeSignature resolvedIntermediateType =
                resolveIntermediateType(actualIntermediateType,
                        actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                        expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);
    }

    private static TypeSignature createTypeSignatureWithDistinctParameterKind(String name, Optional<String> parent, String baseType)
    {
        return new TypeSignature(
                new DistinctTypeInfo(
                        QualifiedObjectName.valueOf(name),
                        parseTypeSignature(baseType),
                        parent.map(QualifiedObjectName::valueOf),
                        true));
    }
}
