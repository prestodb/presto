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
package com.facebook.presto.metadata;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.metadata.FunctionType.SCALAR;
import static com.facebook.presto.metadata.OperatorType.ADD;
import static com.facebook.presto.metadata.Signature.comparableWithVariadicBound;
import static com.facebook.presto.metadata.TestScalarPolymorphicParametricFunction.TestMethods.VARCHAR_TO_BIGINT_RETURN_VALUE;
import static com.facebook.presto.metadata.TestScalarPolymorphicParametricFunction.TestMethods.VARCHAR_TO_VARCHAR_RETURN_VALUE;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static java.util.Collections.emptyMap;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestScalarPolymorphicParametricFunction
{
    private static final TypeRegistry TYPE_REGISTRY = new TypeRegistry();
    private static final FunctionRegistry REGISTRY = new FunctionRegistry(TYPE_REGISTRY, new BlockEncodingManager(TYPE_REGISTRY), true);
    private static final Signature SIGNATURE = Signature.builder()
            .name("foo")
            .type(SCALAR)
            .returnType("bigint")
            .argumentTypes("varchar(x)")
            .build();
    private static final int INPUT_VARCHAR_LENGTH = 10;
    private static final String INPUT_VARCHAR_SIGNATURE = "varchar(" + INPUT_VARCHAR_LENGTH + ")";
    private static final TypeSignature INPUT_VARCHAR_TYPE = parseTypeSignature(INPUT_VARCHAR_SIGNATURE);
    private static final List<TypeSignature> PARAMETER_TYPES = ImmutableList.of(INPUT_VARCHAR_TYPE);
    private static final Slice INPUT_SLICE = Slices.allocate(INPUT_VARCHAR_LENGTH);

    @Test
    public void testSelectsMethodBasedOnArgumentTypes()
            throws Throwable
    {
        ParametricFunction function = ParametricFunction.builder(TestMethods.class)
                .signature(SIGNATURE)
                .methods("bigintToBigintReturnExtraParameter")
                .methods("varcharToBigintReturnExtraParameter")
                .extraParameters(context -> ImmutableList.of(context.getLiteral("x")))
                .build();

        FunctionInfo functionInfo = function.specialize(emptyMap(), PARAMETER_TYPES, TYPE_REGISTRY, REGISTRY);

        assertEquals(functionInfo.getSignature(), new Signature("foo", SCALAR, "bigint", INPUT_VARCHAR_SIGNATURE));
        assertEquals(functionInfo.resolveCalculatedTypes(PARAMETER_TYPES).getSignature(), SIGNATURE.resolveCalculatedTypes(PARAMETER_TYPES));
        assertEquals(functionInfo.getMethodHandle().invoke(INPUT_SLICE), (long) INPUT_VARCHAR_LENGTH);
        assertFalse(functionInfo.isHidden());
    }

    @Test
    public void testSelectsMethodBasedOnReturnType()
            throws Throwable
    {
        ParametricFunction function = ParametricFunction.builder(TestMethods.class)
                .signature(SIGNATURE)
                .methods("varcharToVarcharCreateSliceWithExtraParameterLength")
                .methods("varcharToBigintReturnExtraParameter")
                .extraParameters(context -> ImmutableList.of(42))
                .build();

        FunctionInfo functionInfo = function.specialize(emptyMap(), PARAMETER_TYPES, TYPE_REGISTRY, REGISTRY);

        assertEquals(functionInfo.resolveCalculatedTypes(PARAMETER_TYPES).getSignature(), SIGNATURE.resolveCalculatedTypes(PARAMETER_TYPES));
        assertEquals(functionInfo.getMethodHandle().invoke(INPUT_SLICE), VARCHAR_TO_BIGINT_RETURN_VALUE);
    }

    @Test
    public void testSelectsFirstMethodBasedOnPredicate()
            throws Throwable
    {
        ParametricFunction function = ParametricFunction.builder(TestMethods.class)
                .signature(SIGNATURE)
                .methods("varcharToBigint")
                .predicate(context -> true)
                .methods("varcharToBigintReturnExtraParameter")
                .build();

        FunctionInfo functionInfo = function.specialize(emptyMap(), PARAMETER_TYPES, TYPE_REGISTRY, REGISTRY);

        assertEquals(functionInfo.resolveCalculatedTypes(PARAMETER_TYPES).getSignature(), SIGNATURE.resolveCalculatedTypes(PARAMETER_TYPES));
        assertEquals(functionInfo.getMethodHandle().invoke(INPUT_SLICE), VARCHAR_TO_BIGINT_RETURN_VALUE);
    }

    @Test
    public void testSelectsSecondMethodBasedOnPredicate()
            throws Throwable
    {
        ParametricFunction function = ParametricFunction.builder(TestMethods.class)
                .signature(SIGNATURE)
                .methods("varcharToBigintReturnExtraParameter")
                .predicate(context -> false)
                .methods("varcharToBigint")
                .build();

        FunctionInfo functionInfo = function.specialize(emptyMap(), PARAMETER_TYPES, TYPE_REGISTRY, REGISTRY);

        assertEquals(functionInfo.resolveCalculatedTypes(PARAMETER_TYPES).getSignature(), SIGNATURE.resolveCalculatedTypes(PARAMETER_TYPES));
        assertEquals(functionInfo.getMethodHandle().invoke(INPUT_SLICE), VARCHAR_TO_BIGINT_RETURN_VALUE);
    }

    @Test
    public void testSameLiteralInArgumentsAndReturnValue()
            throws Throwable
    {
        Signature signature = Signature.builder()
                .name("foo")
                .type(SCALAR)
                .returnType("varchar(x)")
                .argumentTypes("varchar(x)")
                .build();

        ParametricFunction function = ParametricFunction.builder(TestMethods.class)
                .signature(signature)
                .methods("varcharToVarchar")
                .build();

        FunctionInfo functionInfo = function.specialize(emptyMap(), PARAMETER_TYPES, TYPE_REGISTRY, REGISTRY);
        Slice slice = (Slice) functionInfo.getMethodHandle().invoke(INPUT_SLICE);
        assertEquals(slice, VARCHAR_TO_VARCHAR_RETURN_VALUE);
        assertEquals(functionInfo.resolveCalculatedTypes(PARAMETER_TYPES).getReturnType(), INPUT_VARCHAR_TYPE);
    }

    @Test
    public void testTypeParameters()
            throws Throwable
    {
        Signature signature = Signature.builder()
                .name("foo")
                .type(SCALAR)
                .typeParameters(comparableWithVariadicBound("V", VARCHAR))
                .returnType("V")
                .argumentTypes("V")
                .build();

        ParametricFunction function = ParametricFunction.builder(TestMethods.class)
                .signature(signature)
                .methods("varcharToVarchar")
                .build();

        FunctionInfo functionInfo = function.specialize(ImmutableMap.of("V", TYPE_REGISTRY.getType(INPUT_VARCHAR_TYPE)), PARAMETER_TYPES, TYPE_REGISTRY, REGISTRY);
        assertEquals(functionInfo.getSignature(), new Signature("foo", SCALAR, INPUT_VARCHAR_SIGNATURE, INPUT_VARCHAR_SIGNATURE));

        Slice slice = (Slice) functionInfo.getMethodHandle().invoke(INPUT_SLICE);
        assertEquals(slice, VARCHAR_TO_VARCHAR_RETURN_VALUE);
    }

    @Test
    public void testSetsHiddenToTrueForOperators()
    {
        Signature signature = Signature.builder()
                .operatorType(ADD)
                .type(SCALAR)
                .returnType("varchar(x)")
                .argumentTypes("varchar(x)")
                .build();

        ParametricFunction function = ParametricFunction.builder(TestMethods.class)
                .signature(signature)
                .methods("varcharToVarchar")
                .build();

        FunctionInfo functionInfo = function.specialize(emptyMap(), PARAMETER_TYPES, TYPE_REGISTRY, REGISTRY);
        assertTrue(functionInfo.isHidden());
    }

    @Test(expectedExceptions = {IllegalStateException.class},
            expectedExceptionsMessageRegExp = "method foo was not found in class com.facebook.presto.metadata.TestScalarPolymorphicParametricFunction\\$TestMethods")
    public void testFailIfNotAllMethodsPresent()
    {
        ParametricFunction.builder(TestMethods.class)
                .signature(SIGNATURE)
                .methods("bigintToBigintReturnExtraParameter")
                .methods("foo")
                .build();
    }

    @Test(expectedExceptions = {IllegalStateException.class},
            expectedExceptionsMessageRegExp = "no methods are selected \\(call methods\\(\\) first\\)")
    public void testFailNoMethodsAreSelectedWhenExtraParametersFunctionIsSet()
    {
        ParametricFunction.builder(TestMethods.class)
                .signature(SIGNATURE)
                .extraParameters(context -> ImmutableList.of(context.getLiteral("x")))
                .build();
    }

    @Test(expectedExceptions = {IllegalStateException.class},
            expectedExceptionsMessageRegExp = "two matching methods \\(varcharToBigintReturnFirstExtraParameter and varcharToBigintReturnExtraParameter\\) for parameter types \\[varchar\\(10\\)\\]")
    public void testFailIfTwoMethodsWithSameArguments()
    {
        ParametricFunction function = ParametricFunction.builder(TestMethods.class)
                .signature(SIGNATURE)
                .methods("varcharToBigintReturnFirstExtraParameter")
                .methods("varcharToBigintReturnExtraParameter")
                .build();

        function.specialize(emptyMap(), PARAMETER_TYPES, TYPE_REGISTRY, REGISTRY);
    }

    @Test(expectedExceptions = {IllegalStateException.class},
            expectedExceptionsMessageRegExp = "two matching methods \\(varcharToBigintReturnFirstExtraParameter and varcharToBigintReturnExtraParameter\\) for parameter types \\[varchar\\(10\\)\\]")
    public void testFailIfTwoMethodsWithPredicatesWithSameArguments()
    {
        ParametricFunction function = ParametricFunction.builder(TestMethods.class)
                .signature(SIGNATURE)
                .methods("varcharToBigintReturnFirstExtraParameter")
                .predicate(context -> true)
                .methods("varcharToBigintReturnExtraParameter")
                .predicate(context -> true)
                .build();

        function.specialize(emptyMap(), PARAMETER_TYPES, TYPE_REGISTRY, REGISTRY);
    }

    public static class TestMethods
    {
        static final Slice VARCHAR_TO_VARCHAR_RETURN_VALUE = Slices.utf8Slice("hello world");
        static final long VARCHAR_TO_BIGINT_RETURN_VALUE = 42L;

        public static Slice varcharToVarchar(Slice varchar)
        {
            return VARCHAR_TO_VARCHAR_RETURN_VALUE;
        }

        public static long varcharToBigint(Slice varchar)
        {
            return VARCHAR_TO_BIGINT_RETURN_VALUE;
        }

        public static long varcharToBigintReturnExtraParameter(Slice varchar, long extraParameter)
        {
            return extraParameter;
        }

        public static long bigintToBigintReturnExtraParameter(long bigint, int extraParameter)
        {
            return bigint;
        }

        public static long varcharToBigintReturnFirstExtraParameter(Slice varchar, long extraParameter1, int extraParameter2)
        {
            return extraParameter1;
        }

        public static Slice varcharToVarcharCreateSliceWithExtraParameterLength(Slice string, int extraParameter)
        {
            return Slices.allocate(extraParameter);
        }
    }
}
