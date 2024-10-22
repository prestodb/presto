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

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.TypeSignatureUtils.getTypeSignatureMap;
import static com.facebook.presto.common.type.TypeSignatureUtils.resolveIntermediateType;
import static org.testng.Assert.assertEquals;

public class TestTypeSignatureUtils
{
    @Test
    public void testGetTypeSignatureMap()
    {
        //test 1
        TypeSignature actualTypeSignature = parseTypeSignature("T");
        TypeSignature expectedTypeSignature = parseTypeSignature("bigint");
        Map<TypeSignature, TypeSignature> actualTypeSignatureMap = getTypeSignatureMap(ImmutableList.of(actualTypeSignature), ImmutableList.of(expectedTypeSignature));
        Map<TypeSignature, TypeSignature> expectedTypeSignatureMap = new HashMap<>();
        expectedTypeSignatureMap.put(actualTypeSignature, expectedTypeSignature);
        assertEquals(actualTypeSignatureMap, expectedTypeSignatureMap);

        //test 2
        actualTypeSignature = parseTypeSignature("T");
        expectedTypeSignature = parseTypeSignature("decimal(15, 2)");
        actualTypeSignatureMap = getTypeSignatureMap(ImmutableList.of(actualTypeSignature), ImmutableList.of(expectedTypeSignature));
        expectedTypeSignatureMap.clear();
        expectedTypeSignatureMap.put(actualTypeSignature, expectedTypeSignature);
        assertEquals(actualTypeSignatureMap, expectedTypeSignatureMap);

        // test 3
        actualTypeSignature = parseTypeSignature("T");
        expectedTypeSignature = parseTypeSignature("varchar(15)");
        actualTypeSignatureMap = getTypeSignatureMap(ImmutableList.of(actualTypeSignature), ImmutableList.of(expectedTypeSignature));
        expectedTypeSignatureMap.clear();
        expectedTypeSignatureMap.put(actualTypeSignature, expectedTypeSignature);
        assertEquals(actualTypeSignatureMap, expectedTypeSignatureMap);

        // test 4
        actualTypeSignature = parseTypeSignature("T");
        expectedTypeSignature = parseTypeSignature("map(array(varchar(15)), decimal(15, 2))");
        actualTypeSignatureMap = getTypeSignatureMap(ImmutableList.of(actualTypeSignature), ImmutableList.of(expectedTypeSignature));
        expectedTypeSignatureMap.clear();
        expectedTypeSignatureMap.put(actualTypeSignature, expectedTypeSignature);
        assertEquals(actualTypeSignatureMap, expectedTypeSignatureMap);

        // test 5
        actualTypeSignature = parseTypeSignature("T");
        expectedTypeSignature = parseTypeSignature("row(array(double))");
        actualTypeSignatureMap = getTypeSignatureMap(ImmutableList.of(actualTypeSignature), ImmutableList.of(expectedTypeSignature));
        expectedTypeSignatureMap.clear();
        expectedTypeSignatureMap.put(actualTypeSignature, expectedTypeSignature);
        assertEquals(actualTypeSignatureMap, expectedTypeSignatureMap);

        // test 6
        actualTypeSignature = parseTypeSignature("T");
        expectedTypeSignature = parseTypeSignature("function(array(double), array(integer), map(integer, bigint))");
        actualTypeSignatureMap = getTypeSignatureMap(ImmutableList.of(actualTypeSignature), ImmutableList.of(expectedTypeSignature));
        expectedTypeSignatureMap.clear();
        expectedTypeSignatureMap.put(actualTypeSignature, expectedTypeSignature);
        assertEquals(actualTypeSignatureMap, expectedTypeSignatureMap);

        // test 7
        actualTypeSignature = parseTypeSignature("double");
        expectedTypeSignature = parseTypeSignature("double");
        actualTypeSignatureMap = getTypeSignatureMap(ImmutableList.of(actualTypeSignature), ImmutableList.of(expectedTypeSignature));
        expectedTypeSignatureMap.clear();
        expectedTypeSignatureMap.put(actualTypeSignature, expectedTypeSignature);
        assertEquals(actualTypeSignatureMap, expectedTypeSignatureMap);

        //test 8
        actualTypeSignature = parseTypeSignature("decimal(15, 2)");
        expectedTypeSignature = parseTypeSignature("decimal(15, 2)");
        actualTypeSignatureMap = getTypeSignatureMap(ImmutableList.of(actualTypeSignature), ImmutableList.of(expectedTypeSignature));
        expectedTypeSignatureMap.clear();
        expectedTypeSignatureMap.put(actualTypeSignature, expectedTypeSignature);
        assertEquals(actualTypeSignatureMap, expectedTypeSignatureMap);

        // test 9
        actualTypeSignature = parseTypeSignature("varchar(15)");
        expectedTypeSignature = parseTypeSignature("varchar(15)");
        actualTypeSignatureMap = getTypeSignatureMap(ImmutableList.of(actualTypeSignature), ImmutableList.of(expectedTypeSignature));
        expectedTypeSignatureMap.clear();
        expectedTypeSignatureMap.put(actualTypeSignature, expectedTypeSignature);
        assertEquals(actualTypeSignatureMap, expectedTypeSignatureMap);

        // test 10
        actualTypeSignature = parseTypeSignature("map(array(varchar(15)), decimal(15, 2))");
        expectedTypeSignature = parseTypeSignature("map(array(varchar(15)), decimal(15, 2))");
        actualTypeSignatureMap = getTypeSignatureMap(
                actualTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures());
        expectedTypeSignatureMap.clear();
        expectedTypeSignatureMap.put(parseTypeSignature("varchar(15)"), parseTypeSignature("varchar(15)"));
        expectedTypeSignatureMap.put(parseTypeSignature("decimal(15, 2)"), parseTypeSignature("decimal(15, 2)"));
        assertEquals(actualTypeSignatureMap, expectedTypeSignatureMap);

        // test 11
        actualTypeSignature = parseTypeSignature("row(array(double))");
        expectedTypeSignature = parseTypeSignature("row(array(double))");
        actualTypeSignatureMap = getTypeSignatureMap(
                actualTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures());
        expectedTypeSignatureMap.clear();
        expectedTypeSignatureMap.put(parseTypeSignature("double"), parseTypeSignature("double"));
        assertEquals(actualTypeSignatureMap, expectedTypeSignatureMap);

        // test 12
        actualTypeSignature = parseTypeSignature("function(array(double), array(integer), map(integer, bigint))");
        expectedTypeSignature = parseTypeSignature("function(array(double), array(integer), map(integer, bigint))");
        actualTypeSignatureMap = getTypeSignatureMap(
                actualTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures());
        expectedTypeSignatureMap.clear();
        expectedTypeSignatureMap.put(parseTypeSignature("double"), parseTypeSignature("double"));
        expectedTypeSignatureMap.put(parseTypeSignature("bigint"), parseTypeSignature("bigint"));
        expectedTypeSignatureMap.put(parseTypeSignature("integer"), parseTypeSignature("integer"));
        assertEquals(actualTypeSignatureMap, expectedTypeSignatureMap);

        // test 13
        actualTypeSignature = parseTypeSignature("test(bigint)");
        expectedTypeSignature = parseTypeSignature("test(bigint)");
        actualTypeSignatureMap =
                getTypeSignatureMap(actualTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures(), expectedTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures());
        expectedTypeSignatureMap.clear();
        expectedTypeSignatureMap.put(parseTypeSignature("bigint"), parseTypeSignature("bigint"));
        assertEquals(actualTypeSignatureMap, expectedTypeSignatureMap);

        // test 14
        actualTypeSignature = parseTypeSignature("test(T)");
        expectedTypeSignature = parseTypeSignature("test(bigint)");
        actualTypeSignatureMap = getTypeSignatureMap(actualTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures(), expectedTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures());
        expectedTypeSignatureMap.clear();
        expectedTypeSignatureMap.put(parseTypeSignature("T"), parseTypeSignature("bigint"));
        assertEquals(actualTypeSignatureMap, expectedTypeSignatureMap);

        // test 15
        actualTypeSignature = parseTypeSignature("test(array(T))");
        expectedTypeSignature = parseTypeSignature("test(array(double))");
        actualTypeSignatureMap = getTypeSignatureMap(actualTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures(), expectedTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures());
        expectedTypeSignatureMap.clear();
        expectedTypeSignatureMap.put(parseTypeSignature("T"), parseTypeSignature("double"));
        assertEquals(actualTypeSignatureMap, expectedTypeSignatureMap);

        // test 16
        actualTypeSignature = parseTypeSignature("test(T)");
        expectedTypeSignature = parseTypeSignature("test(array(double))");
        actualTypeSignatureMap = getTypeSignatureMap(actualTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures(), expectedTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures());
        expectedTypeSignatureMap.clear();
        expectedTypeSignatureMap.put(parseTypeSignature("T"), parseTypeSignature("array(double)"));
        assertEquals(actualTypeSignatureMap, expectedTypeSignatureMap);

        // test 17
        actualTypeSignature = parseTypeSignature("test(T)");
        expectedTypeSignature = parseTypeSignature("test(varchar(35))");
        actualTypeSignatureMap = getTypeSignatureMap(actualTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures(), expectedTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures());
        expectedTypeSignatureMap.clear();
        expectedTypeSignatureMap.put(parseTypeSignature("T"), parseTypeSignature("varchar(35)"));
        assertEquals(actualTypeSignatureMap, expectedTypeSignatureMap);

        // test 18
        actualTypeSignature = parseTypeSignature("test(K, V, T)");
        expectedTypeSignature = parseTypeSignature("test(varchar(35), bigint, array(double))");
        actualTypeSignatureMap = getTypeSignatureMap(actualTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures(), expectedTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures());
        expectedTypeSignatureMap.clear();
        expectedTypeSignatureMap.put(parseTypeSignature("K"), parseTypeSignature("varchar(35)"));
        expectedTypeSignatureMap.put(parseTypeSignature("V"), parseTypeSignature("bigint"));
        expectedTypeSignatureMap.put(parseTypeSignature("T"), parseTypeSignature("array(double)"));
        assertEquals(actualTypeSignatureMap, expectedTypeSignatureMap);

        // test 19
        actualTypeSignature = parseTypeSignature("test(row(K, V, T))");
        expectedTypeSignature = parseTypeSignature("test(row(varchar(35), bigint, array(double)))");
        actualTypeSignatureMap = getTypeSignatureMap(actualTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures(), expectedTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures());
        expectedTypeSignatureMap.clear();
        expectedTypeSignatureMap.put(parseTypeSignature("K"), parseTypeSignature("varchar(35)"));
        expectedTypeSignatureMap.put(parseTypeSignature("V"), parseTypeSignature("bigint"));
        expectedTypeSignatureMap.put(parseTypeSignature("T"), parseTypeSignature("array(double)"));
        assertEquals(actualTypeSignatureMap, expectedTypeSignatureMap);

        //test 20
        actualTypeSignature = parseTypeSignature("test(row(varchar(50), decimal(38, 2)))");
        expectedTypeSignature = parseTypeSignature("test(row(varchar(50), decimal(38, 2)))");
        actualTypeSignatureMap = getTypeSignatureMap(actualTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures());
        expectedTypeSignatureMap.clear();
        expectedTypeSignatureMap.put(parseTypeSignature("varchar(50)"), parseTypeSignature("varchar(50)"));
        expectedTypeSignatureMap.put(parseTypeSignature("decimal(38, 2)"), parseTypeSignature("decimal(38, 2)"));
        assertEquals(actualTypeSignatureMap, expectedTypeSignatureMap);

        //test 21
        actualTypeSignature = parseTypeSignature("test(row(K, V))");
        expectedTypeSignature = parseTypeSignature("test(row(varchar(50), decimal(38, 2)))");
        actualTypeSignatureMap = getTypeSignatureMap(actualTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures());
        expectedTypeSignatureMap.clear();
        expectedTypeSignatureMap.put(parseTypeSignature("K"), parseTypeSignature("varchar(50)"));
        expectedTypeSignatureMap.put(parseTypeSignature("V"), parseTypeSignature("decimal(38, 2)"));
        assertEquals(actualTypeSignatureMap, expectedTypeSignatureMap);

        //test 22
        actualTypeSignature = parseTypeSignature("test(row(K, decimal(38, 2)))");
        expectedTypeSignature = parseTypeSignature("test(row(varchar(50), decimal(38, 2)))");
        actualTypeSignatureMap = getTypeSignatureMap(actualTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures());
        expectedTypeSignatureMap.clear();
        expectedTypeSignatureMap.put(parseTypeSignature("K"), parseTypeSignature("varchar(50)"));
        expectedTypeSignatureMap.put(parseTypeSignature("decimal(38, 2)"), parseTypeSignature("decimal(38, 2)"));
        assertEquals(actualTypeSignatureMap, expectedTypeSignatureMap);

        //test 23
        actualTypeSignature = parseTypeSignature("test(row(K, decimal(38, 2)))");
        expectedTypeSignature = parseTypeSignature("test(row(map(varchar(50), decimal(38,2)), decimal(38, 2)))");
        actualTypeSignatureMap = getTypeSignatureMap(actualTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures());
        expectedTypeSignatureMap.clear();
        expectedTypeSignatureMap.put(parseTypeSignature("K"), parseTypeSignature("map(varchar(50), decimal(38,2))"));
        expectedTypeSignatureMap.put(parseTypeSignature("decimal(38, 2)"), parseTypeSignature("decimal(38, 2)"));
        assertEquals(actualTypeSignatureMap, expectedTypeSignatureMap);

        //test 24
        actualTypeSignature = parseTypeSignature("test(row(map(array(T), K), decimal(15, 2)))");
        expectedTypeSignature = parseTypeSignature("test(row(map(array(array(double)), decimal(38,2)), decimal(15, 2)))");
        actualTypeSignatureMap = getTypeSignatureMap(actualTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures());
        expectedTypeSignatureMap.clear();
        expectedTypeSignatureMap.put(parseTypeSignature("T"), parseTypeSignature("array(double)"));
        expectedTypeSignatureMap.put(parseTypeSignature("K"), parseTypeSignature("decimal(38, 2)"));
        expectedTypeSignatureMap.put(parseTypeSignature("decimal(15, 2)"), parseTypeSignature("decimal(15, 2)"));
        assertEquals(actualTypeSignatureMap, expectedTypeSignatureMap);

        // test 25 - test varchar parameters
        actualTypeSignature = new TypeSignature(
                "decimal",
                TypeSignatureParameter.of("i4"),
                TypeSignatureParameter.of("i5"));
        expectedTypeSignature = parseTypeSignature("decimal(15, 2)");
        actualTypeSignatureMap =
                getTypeSignatureMap(ImmutableList.of(actualTypeSignature), ImmutableList.of(expectedTypeSignature));
        expectedTypeSignatureMap.clear();
        expectedTypeSignatureMap.put(actualTypeSignature, parseTypeSignature("decimal(15, 2)"));
        assertEquals(actualTypeSignatureMap, expectedTypeSignatureMap);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "Parameters size: 1 and argumentTypes size: 2 do not match !")
    public void testFailingGetTypeSignatureMapExtraArgs()
    {
        TypeSignature actualTypeSignature = parseTypeSignature("test(k)");
        TypeSignature expectedTypeSignature = parseTypeSignature("test(array(double), bigint)");
        getTypeSignatureMap(actualTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures(), expectedTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures());
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "Parameters size: 2 and argumentTypes size: 1 do not match !")
    public void testFailingGetTypeSignatureMapExtraParams()
    {
        TypeSignature actualTypeSignature = parseTypeSignature("test(K, V)");
        TypeSignature expectedTypeSignature = parseTypeSignature("test(array(double))");
        getTypeSignatureMap(actualTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures(), expectedTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures());
    }

    @Test
    public void testResolveIntermediateType()
    {
        // test 1
        TypeSignature actualIntermediateType = parseTypeSignature("bigint");
        TypeSignature expectedIntermediateType = parseTypeSignature("bigint");
        TypeSignature resolvedIntermediateType = resolveIntermediateType(
                actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);

        // test 2 - when intermediateType is already resolved, but still have different types in args types
        actualIntermediateType = parseTypeSignature("bigint");
        resolvedIntermediateType =
                resolveIntermediateType(
                        actualIntermediateType,
                        ImmutableList.of(parseTypeSignature("double")),
                        ImmutableList.of(parseTypeSignature("double")));
        assertEquals(resolvedIntermediateType, actualIntermediateType);

        // test 2
        actualIntermediateType = parseTypeSignature("T");
        expectedIntermediateType = parseTypeSignature("bigint");
        resolvedIntermediateType = resolveIntermediateType(
                actualIntermediateType,
                ImmutableList.of(actualIntermediateType),
                ImmutableList.of(expectedIntermediateType));
        assertEquals(resolvedIntermediateType, expectedIntermediateType);

        // test 3
        actualIntermediateType = parseTypeSignature("test(T)");
        expectedIntermediateType = parseTypeSignature("test(bigint)");
        resolvedIntermediateType = resolveIntermediateType(
                actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);

        // test 4
        actualIntermediateType = parseTypeSignature("test(array(T),boolean,double,integer,E,array(T))");
        expectedIntermediateType = parseTypeSignature("test(array(bigint),boolean,double,integer,varchar(100), array(bigint))");
        resolvedIntermediateType = resolveIntermediateType(actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);

        // test 6
        actualIntermediateType = parseTypeSignature("row(bigint)");
        expectedIntermediateType = parseTypeSignature("row(bigint)");
        resolvedIntermediateType = resolveIntermediateType(actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);

        // test 7
        actualIntermediateType = parseTypeSignature("row(T)");
        expectedIntermediateType = parseTypeSignature("row(bigint)");
        resolvedIntermediateType = resolveIntermediateType(actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);

        // test 8
        actualIntermediateType = parseTypeSignature("test(row(T))");
        expectedIntermediateType = parseTypeSignature("test(row(bigint))");
        resolvedIntermediateType = resolveIntermediateType(actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);

        // test 9
        actualIntermediateType = parseTypeSignature("test(row(array(T),boolean,double,integer,E,array(T)))");
        expectedIntermediateType = parseTypeSignature("test(row(array(bigint),boolean,double,integer,varchar(100), array(bigint)))");
        resolvedIntermediateType = resolveIntermediateType(actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);

        // test 10
        actualIntermediateType = parseTypeSignature("row(array(T),boolean,double,integer,E,array(T),array(integer))");
        expectedIntermediateType = parseTypeSignature("row(array(bigint),boolean,double,integer,varchar(100),array(bigint),array(integer))");
        resolvedIntermediateType = resolveIntermediateType(actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);

        // test 10
        actualIntermediateType = parseTypeSignature("decimal(15, 2)");
        expectedIntermediateType = parseTypeSignature("test(decimal(15, 2))");
        List<TypeSignature> expectedIntermediateTypeParameters = expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures();
        resolvedIntermediateType = resolveIntermediateType(
                actualIntermediateType,
                ImmutableList.of(actualIntermediateType),
                expectedIntermediateTypeParameters);
        assertEquals(resolvedIntermediateType, expectedIntermediateTypeParameters.get(0));

        actualIntermediateType = parseTypeSignature("test(decimal(15, 2))");
        expectedIntermediateType = parseTypeSignature("test(decimal(15, 2))");
        resolvedIntermediateType = resolveIntermediateType(actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);

        actualIntermediateType = parseTypeSignature("map(T, decimal(15, 2))");
        expectedIntermediateType = parseTypeSignature("map(integer, decimal(15, 2))");
        resolvedIntermediateType = resolveIntermediateType(actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);

        actualIntermediateType = parseTypeSignature("map(map(array(K), V), decimal(15, 2))");
        expectedIntermediateType = parseTypeSignature("map(map(array(double), decimal(15, 2)), decimal(15, 2))");
        resolvedIntermediateType = resolveIntermediateType(actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);

        actualIntermediateType = parseTypeSignature("row(map(map(array(K), V), decimal(15, 2)))");
        expectedIntermediateType = parseTypeSignature("row(map(map(array(double), varchar(50)), decimal(15, 2)))");
        resolvedIntermediateType = resolveIntermediateType(actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);

        actualIntermediateType = parseTypeSignature("row(map(K, V))");
        expectedIntermediateType = parseTypeSignature("row(map(map(array(double), varchar(50)), decimal(15, 2)))");
        resolvedIntermediateType = resolveIntermediateType(actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);

        actualIntermediateType = parseTypeSignature("row(map(map(array(double), varchar(50)), decimal(15, 2)))");
        expectedIntermediateType = parseTypeSignature("row(map(map(array(double), varchar(50)), decimal(15, 2)))");
        resolvedIntermediateType = resolveIntermediateType(actualIntermediateType,
                actualIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures(),
                expectedIntermediateType.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(resolvedIntermediateType, expectedIntermediateType);

        actualIntermediateType = parseTypeSignature("S");
        expectedIntermediateType = parseTypeSignature("array(bigint)");
        List<TypeSignature> actualIntermediateTypeParams =
                parseTypeSignature("test(T, S, function(S, T, S), function(S, S, S))").getTypeOrNamedTypeParametersAsTypeSignatures();
        List<TypeSignature> expectedIntermediateTypeParams =
                parseTypeSignature(
                        "test(integer, array(bigint), function(array(bigint), integer, array(bigint)), " +
                                "function(array(bigint), array(bigint), array(bigint)))").getTypeOrNamedTypeParametersAsTypeSignatures();
        resolvedIntermediateType = resolveIntermediateType(
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
}
