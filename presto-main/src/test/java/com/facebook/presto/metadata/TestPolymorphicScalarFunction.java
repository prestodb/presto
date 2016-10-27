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
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.Signature.comparableWithVariadicBound;
import static com.facebook.presto.metadata.TestPolymorphicScalarFunction.TestMethods.VARCHAR_TO_BIGINT_RETURN_VALUE;
import static com.facebook.presto.metadata.TestPolymorphicScalarFunction.TestMethods.VARCHAR_TO_VARCHAR_RETURN_VALUE;
import static com.facebook.presto.spi.function.OperatorType.ADD;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static org.testng.Assert.assertEquals;

public class TestPolymorphicScalarFunction
{
    private static final TypeRegistry TYPE_REGISTRY = new TypeRegistry();
    private static final FunctionRegistry REGISTRY = new FunctionRegistry(TYPE_REGISTRY, new BlockEncodingManager(TYPE_REGISTRY), new FeaturesConfig());
    private static final Signature SIGNATURE = Signature.builder()
            .name("foo")
            .kind(SCALAR)
            .returnType(parseTypeSignature(StandardTypes.BIGINT))
            .argumentTypes(parseTypeSignature("varchar(x)", ImmutableSet.of("x")))
            .build();
    private static final long INPUT_VARCHAR_LENGTH = 10;
    private static final String INPUT_VARCHAR_SIGNATURE = "varchar(" + INPUT_VARCHAR_LENGTH + ")";
    private static final TypeSignature INPUT_VARCHAR_TYPE = parseTypeSignature(INPUT_VARCHAR_SIGNATURE);
    private static final Slice INPUT_SLICE = Slices.allocate(Ints.checkedCast(INPUT_VARCHAR_LENGTH));
    private static final BoundVariables BOUND_VARIABLES = new BoundVariables(
            ImmutableMap.of("V", TYPE_REGISTRY.getType(INPUT_VARCHAR_TYPE)),
            ImmutableMap.of("x", INPUT_VARCHAR_LENGTH)
    );

    @Test
    public void testSelectsMethodBasedOnArgumentTypes()
            throws Throwable
    {
        SqlScalarFunction function = SqlScalarFunction.builder(TestMethods.class)
                .signature(SIGNATURE)
                .implementation(b -> b.methods("bigintToBigintReturnExtraParameter"))
                .implementation(b -> b
                        .methods("varcharToBigintReturnExtraParameter")
                        .withExtraParameters(context -> ImmutableList.of(context.getLiteral("x")))
                )
                .build();

        ScalarFunctionImplementation functionImplementation = function.specialize(BOUND_VARIABLES, 1, TYPE_REGISTRY, REGISTRY);
        assertEquals(functionImplementation.getMethodHandle().invoke(INPUT_SLICE), INPUT_VARCHAR_LENGTH);
    }

    @Test
    public void testSelectsMethodBasedOnReturnType()
            throws Throwable
    {
        SqlScalarFunction function = SqlScalarFunction.builder(TestMethods.class)
                .signature(SIGNATURE)
                .implementation(b -> b.methods("varcharToVarcharCreateSliceWithExtraParameterLength"))
                .implementation(b -> b
                        .methods("varcharToBigintReturnExtraParameter")
                        .withExtraParameters(context -> ImmutableList.of(42))
                )
                .build();

        ScalarFunctionImplementation functionImplementation = function.specialize(BOUND_VARIABLES, 1, TYPE_REGISTRY, REGISTRY);

        assertEquals(functionImplementation.getMethodHandle().invoke(INPUT_SLICE), VARCHAR_TO_BIGINT_RETURN_VALUE);
    }

    @Test
    public void testSelectsFirstMethodBasedOnPredicate()
            throws Throwable
    {
        SqlScalarFunction function = SqlScalarFunction.builder(TestMethods.class)
                .signature(SIGNATURE)
                .implementation(b -> b
                        .methods("varcharToBigint")
                        .withPredicate(context -> true)
                )
                .implementation(b -> b.methods("varcharToBigintReturnExtraParameter"))
                .build();

        ScalarFunctionImplementation functionImplementation = function.specialize(BOUND_VARIABLES, 1, TYPE_REGISTRY, REGISTRY);

        assertEquals(functionImplementation.getMethodHandle().invoke(INPUT_SLICE), VARCHAR_TO_BIGINT_RETURN_VALUE);
    }

    @Test
    public void testSelectsSecondMethodBasedOnPredicate()
            throws Throwable
    {
        SqlScalarFunction function = SqlScalarFunction.builder(TestMethods.class)
                .signature(SIGNATURE)
                .implementation(b -> b
                        .methods("varcharToBigintReturnExtraParameter")
                        .withPredicate(context -> false)
                )
                .implementation(b -> b.methods("varcharToBigint"))
                .build();

        ScalarFunctionImplementation functionImplementation = function.specialize(BOUND_VARIABLES, 1, TYPE_REGISTRY, REGISTRY);

        assertEquals(functionImplementation.getMethodHandle().invoke(INPUT_SLICE), VARCHAR_TO_BIGINT_RETURN_VALUE);
    }

    @Test
    public void testSameLiteralInArgumentsAndReturnValue()
            throws Throwable
    {
        Signature signature = Signature.builder()
                .name("foo")
                .kind(SCALAR)
                .returnType(parseTypeSignature("varchar(x)", ImmutableSet.of("x")))
                .argumentTypes(parseTypeSignature("varchar(x)", ImmutableSet.of("x")))
                .build();

        SqlScalarFunction function = SqlScalarFunction.builder(TestMethods.class)
                .signature(signature)
                .implementation(b -> b.methods("varcharToVarchar"))
                .build();

        ScalarFunctionImplementation functionImplementation = function.specialize(BOUND_VARIABLES, 1, TYPE_REGISTRY, REGISTRY);
        Slice slice = (Slice) functionImplementation.getMethodHandle().invoke(INPUT_SLICE);
        assertEquals(slice, VARCHAR_TO_VARCHAR_RETURN_VALUE);
    }

    @Test
    public void testTypeParameters()
            throws Throwable
    {
        Signature signature = Signature.builder()
                .name("foo")
                .kind(SCALAR)
                .typeVariableConstraints(comparableWithVariadicBound("V", VARCHAR))
                .returnType(parseTypeSignature("V"))
                .argumentTypes(parseTypeSignature("V"))
                .build();

        SqlScalarFunction function = SqlScalarFunction.builder(TestMethods.class)
                .signature(signature)
                .implementation(b -> b.methods("varcharToVarchar"))
                .build();

        ScalarFunctionImplementation functionImplementation = function.specialize(BOUND_VARIABLES, 1, TYPE_REGISTRY, REGISTRY);
        Slice slice = (Slice) functionImplementation.getMethodHandle().invoke(INPUT_SLICE);
        assertEquals(slice, VARCHAR_TO_VARCHAR_RETURN_VALUE);
    }

    @Test
    public void testSetsHiddenToTrueForOperators()
    {
        Signature signature = Signature.builder()
                .operatorType(ADD)
                .kind(SCALAR)
                .returnType(parseTypeSignature("varchar(x)", ImmutableSet.of("x")))
                .argumentTypes(parseTypeSignature("varchar(x)", ImmutableSet.of("x")))
                .build();

        SqlScalarFunction function = SqlScalarFunction.builder(TestMethods.class)
                .signature(signature)
                .implementation(b -> b.methods("varcharToVarchar"))
                .build();

        ScalarFunctionImplementation functionImplementation = function.specialize(BOUND_VARIABLES, 1, TYPE_REGISTRY, REGISTRY);
    }

    @Test(expectedExceptions = {IllegalStateException.class},
            expectedExceptionsMessageRegExp = "method foo was not found in class com.facebook.presto.metadata.TestPolymorphicScalarFunction\\$TestMethods")
    public void testFailIfNotAllMethodsPresent()
    {
        SqlScalarFunction.builder(TestMethods.class)
                .signature(SIGNATURE)
                .implementation(b -> b.methods("bigintToBigintReturnExtraParameter"))
                .implementation(b -> b.methods("foo"))
                .build();
    }

    @Test(expectedExceptions = {IllegalStateException.class},
            expectedExceptionsMessageRegExp = "methods must be selected first")
    public void testFailNoMethodsAreSelectedWhenExtraParametersFunctionIsSet()
    {
        SqlScalarFunction.builder(TestMethods.class)
                .signature(SIGNATURE)
                .implementation(b -> b
                        .withExtraParameters(context -> ImmutableList.of(42))
                )
                .build();
    }

    @Test(expectedExceptions = {IllegalStateException.class},
            expectedExceptionsMessageRegExp = "two matching methods \\(varcharToBigintReturnFirstExtraParameter and varcharToBigintReturnExtraParameter\\) for parameter types \\[varchar\\(10\\)\\]")
    public void testFailIfTwoMethodsWithSameArguments()
    {
        SqlScalarFunction function = SqlScalarFunction.builder(TestMethods.class)
                .signature(SIGNATURE)
                .implementation(b -> b.methods("varcharToBigintReturnFirstExtraParameter"))
                .implementation(b -> b.methods("varcharToBigintReturnExtraParameter"))
                .build();

        function.specialize(BOUND_VARIABLES, 1, TYPE_REGISTRY, REGISTRY);
    }

    @Test(expectedExceptions = {IllegalStateException.class},
            expectedExceptionsMessageRegExp = "two matching methods \\(varcharToBigintReturnFirstExtraParameter and varcharToBigintReturnExtraParameter\\) for parameter types \\[varchar\\(10\\)\\]")
    public void testFailIfTwoMethodsWithPredicatesWithSameArguments()
    {
        SqlScalarFunction function = SqlScalarFunction.builder(TestMethods.class)
                .signature(SIGNATURE)
                .implementation(b -> b
                        .methods("varcharToBigintReturnFirstExtraParameter")
                        .withPredicate(context -> true)
                )
                .implementation(b -> b
                        .methods("varcharToBigintReturnExtraParameter")
                        .withPredicate(context -> true)
                )
                .build();

        function.specialize(BOUND_VARIABLES, 1, TYPE_REGISTRY, REGISTRY);
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
