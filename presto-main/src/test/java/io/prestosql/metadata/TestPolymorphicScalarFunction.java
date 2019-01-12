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
package io.prestosql.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.block.BlockEncodingManager;
import io.prestosql.operator.scalar.ScalarFunctionImplementation;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.LongArrayBlock;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.type.TypeRegistry;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Optional;

import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.metadata.Signature.comparableWithVariadicBound;
import static io.prestosql.metadata.TestPolymorphicScalarFunction.TestMethods.VARCHAR_TO_BIGINT_RETURN_VALUE;
import static io.prestosql.metadata.TestPolymorphicScalarFunction.TestMethods.VARCHAR_TO_VARCHAR_RETURN_VALUE;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.BLOCK_AND_POSITION;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.USE_NULL_FLAG;
import static io.prestosql.spi.function.OperatorType.ADD;
import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.prestosql.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.prestosql.spi.type.StandardTypes.BOOLEAN;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static java.lang.Math.toIntExact;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

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
    private static final Slice INPUT_SLICE = Slices.allocate(toIntExact(INPUT_VARCHAR_LENGTH));
    private static final BoundVariables BOUND_VARIABLES = new BoundVariables(
            ImmutableMap.of("V", TYPE_REGISTRY.getType(INPUT_VARCHAR_TYPE)),
            ImmutableMap.of("x", INPUT_VARCHAR_LENGTH));

    private static final TypeSignature DECIMAL_SIGNATURE = parseTypeSignature("decimal(a_precision, a_scale)", ImmutableSet.of("a_precision", "a_scale"));
    private static final BoundVariables LONG_DECIMAL_BOUND_VARIABLES = new BoundVariables(
            ImmutableMap.of(),
            ImmutableMap.of("a_precision", MAX_SHORT_PRECISION + 1L, "a_scale", 2L));
    private static final BoundVariables SHORT_DECIMAL_BOUND_VARIABLES = new BoundVariables(
            ImmutableMap.of(),
            ImmutableMap.of("a_precision", (long) MAX_SHORT_PRECISION, "a_scale", 2L));

    @Test
    public void testSelectsMultipleChoiceWithBlockPosition()
            throws Throwable
    {
        Signature signature = Signature.builder()
                .kind(SCALAR)
                .operatorType(IS_DISTINCT_FROM)
                .argumentTypes(DECIMAL_SIGNATURE, DECIMAL_SIGNATURE)
                .returnType(parseTypeSignature(BOOLEAN))
                .build();

        SqlScalarFunction function = SqlScalarFunction.builder(TestMethods.class)
                .signature(signature)
                .deterministic(true)
                .choice(choice -> choice
                        .argumentProperties(
                                valueTypeArgumentProperty(USE_NULL_FLAG),
                                valueTypeArgumentProperty(USE_NULL_FLAG))
                        .implementation(methodsGroup -> methodsGroup
                                .methods("shortShort", "longLong")))
                .choice(choice -> choice
                        .argumentProperties(
                                valueTypeArgumentProperty(BLOCK_AND_POSITION),
                                valueTypeArgumentProperty(BLOCK_AND_POSITION))
                        .implementation(methodsGroup -> methodsGroup
                                .methodWithExplicitJavaTypes("blockPositionLongLong",
                                        asList(Optional.of(Slice.class), Optional.of(Slice.class)))
                                .methodWithExplicitJavaTypes("blockPositionShortShort",
                                        asList(Optional.of(long.class), Optional.of(long.class)))))
                .build();

        ScalarFunctionImplementation functionImplementation = function.specialize(SHORT_DECIMAL_BOUND_VARIABLES, 2, TYPE_REGISTRY, REGISTRY);

        assertEquals(functionImplementation.getAllChoices().size(), 2);
        assertEquals(functionImplementation.getAllChoices().get(0).getArgumentProperties(), Collections.nCopies(2, valueTypeArgumentProperty(USE_NULL_FLAG)));
        assertEquals(functionImplementation.getAllChoices().get(1).getArgumentProperties(), Collections.nCopies(2, valueTypeArgumentProperty(BLOCK_AND_POSITION)));
        Block block1 = new LongArrayBlock(0, Optional.empty(), new long[0]);
        Block block2 = new LongArrayBlock(0, Optional.empty(), new long[0]);
        assertFalse((boolean) functionImplementation.getAllChoices().get(1).getMethodHandle().invoke(block1, 0, block2, 0));
        functionImplementation = function.specialize(LONG_DECIMAL_BOUND_VARIABLES, 2, TYPE_REGISTRY, REGISTRY);
        assertTrue((boolean) functionImplementation.getAllChoices().get(1).getMethodHandle().invoke(block1, 0, block2, 0));
    }

    @Test
    public void testSelectsMethodBasedOnArgumentTypes()
            throws Throwable
    {
        SqlScalarFunction function = SqlScalarFunction.builder(TestMethods.class)
                .signature(SIGNATURE)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup.methods("bigintToBigintReturnExtraParameter"))
                        .implementation(methodsGroup -> methodsGroup
                                .methods("varcharToBigintReturnExtraParameter")
                                .withExtraParameters(context -> ImmutableList.of(context.getLiteral("x")))))
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
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup.methods("varcharToVarcharCreateSliceWithExtraParameterLength"))
                        .implementation(methodsGroup -> methodsGroup
                                .methods("varcharToBigintReturnExtraParameter")
                                .withExtraParameters(context -> ImmutableList.of(42))))
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
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup.methods("varcharToVarchar")))
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
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup.methods("varcharToVarchar")))
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
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup.methods("varcharToVarchar")))
                .build();

        ScalarFunctionImplementation functionImplementation = function.specialize(BOUND_VARIABLES, 1, TYPE_REGISTRY, REGISTRY);
    }

    @Test(expectedExceptions = {IllegalStateException.class},
            expectedExceptionsMessageRegExp = "method foo was not found in class io.prestosql.metadata.TestPolymorphicScalarFunction\\$TestMethods")
    public void testFailIfNotAllMethodsPresent()
    {
        SqlScalarFunction.builder(TestMethods.class)
                .signature(SIGNATURE)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup.methods("bigintToBigintReturnExtraParameter"))
                        .implementation(methodsGroup -> methodsGroup.methods("foo")))
                .build();
    }

    @Test(expectedExceptions = {IllegalStateException.class},
            expectedExceptionsMessageRegExp = "methods must be selected first")
    public void testFailNoMethodsAreSelectedWhenExtraParametersFunctionIsSet()
    {
        SqlScalarFunction.builder(TestMethods.class)
                .signature(SIGNATURE)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup
                                .withExtraParameters(context -> ImmutableList.of(42))))
                .build();
    }

    @Test(expectedExceptions = {IllegalStateException.class},
            expectedExceptionsMessageRegExp = "two matching methods \\(varcharToBigintReturnFirstExtraParameter and varcharToBigintReturnExtraParameter\\) for parameter types \\[varchar\\(10\\)\\]")
    public void testFailIfTwoMethodsWithSameArguments()
    {
        SqlScalarFunction function = SqlScalarFunction.builder(TestMethods.class)
                .signature(SIGNATURE)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup.methods("varcharToBigintReturnFirstExtraParameter"))
                        .implementation(methodsGroup -> methodsGroup.methods("varcharToBigintReturnExtraParameter")))
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

        public static boolean blockPositionLongLong(Block left, int leftPosition, Block right, int rightPosition)
        {
            return true;
        }

        public static boolean blockPositionShortShort(Block left, int leftPosition, Block right, int rightPosition)
        {
            return false;
        }

        public static boolean shortShort(long left, boolean leftNull, long right, boolean rightNull)
        {
            return false;
        }

        public static boolean longLong(Slice left, boolean leftNull, Slice right, boolean rightNull)
        {
            return false;
        }
    }
}
