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
import com.facebook.presto.operator.scalar.CustomFunctions;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeVariableConstraint;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandles;
import java.util.List;

import static com.facebook.presto.metadata.OperatorSignatureUtils.mangleOperatorName;
import static com.facebook.presto.metadata.OperatorSignatureUtils.unmangleOperator;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.function.OperatorType.CAST;
import static com.facebook.presto.spi.function.OperatorType.SATURATED_FLOOR_CAST;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static com.facebook.presto.sql.planner.LiteralEncoder.getMagicLiteralFunctionSignature;
import static com.facebook.presto.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Lists.transform;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestStaticFunctionNamespace
{
    @Test
    public void testIdentityCast()
    {
        TypeRegistry typeManager = new TypeRegistry();
        StaticFunctionNamespace staticFunctionNamespace = createStaticFunctionNamespace(typeManager);
        FunctionHandle exactOperator = staticFunctionNamespace.lookupCast(CastType.CAST, HYPER_LOG_LOG.getTypeSignature(), HYPER_LOG_LOG.getTypeSignature());
        assertEquals(exactOperator, new StaticFunctionHandle(new Signature(mangleOperatorName(CAST.name()), SCALAR, HYPER_LOG_LOG.getTypeSignature(), HYPER_LOG_LOG.getTypeSignature())));
    }

    @Test
    public void testExactMatchBeforeCoercion()
    {
        TypeRegistry typeManager = new TypeRegistry();
        StaticFunctionNamespace staticFunctionNamespace = createStaticFunctionNamespace(typeManager);
        boolean foundOperator = false;
        for (SqlFunction function : staticFunctionNamespace.listOperators()) {
            OperatorType operatorType = unmangleOperator(function.getSignature().getName());
            if (operatorType == CAST || operatorType == SATURATED_FLOOR_CAST) {
                continue;
            }
            if (!function.getSignature().getTypeVariableConstraints().isEmpty()) {
                continue;
            }
            if (function.getSignature().getArgumentTypes().stream().anyMatch(TypeSignature::isCalculated)) {
                continue;
            }
            StaticFunctionHandle exactOperator = (StaticFunctionHandle) staticFunctionNamespace.resolveOperator(operatorType, fromTypeSignatures(function.getSignature().getArgumentTypes()));
            assertEquals(exactOperator.getSignature(), function.getSignature());
            foundOperator = true;
        }
        assertTrue(foundOperator);
    }

    @Test
    public void testMagicLiteralFunction()
    {
        Signature signature = getMagicLiteralFunctionSignature(TIMESTAMP_WITH_TIME_ZONE);
        assertEquals(signature.getName(), "$literal$timestamp with time zone");
        assertEquals(signature.getArgumentTypes(), ImmutableList.of(parseTypeSignature(StandardTypes.BIGINT)));
        assertEquals(signature.getReturnType().getBase(), StandardTypes.TIMESTAMP_WITH_TIME_ZONE);

        TypeRegistry typeManager = new TypeRegistry();
        StaticFunctionNamespace staticFunctionNamespace = createStaticFunctionNamespace(typeManager);
        StaticFunctionHandle functionHandle = (StaticFunctionHandle) staticFunctionNamespace.resolveFunction(QualifiedName.of(signature.getName()), fromTypeSignatures(signature.getArgumentTypes()));
        assertEquals(staticFunctionNamespace.getFunctionMetadata(functionHandle).getArgumentTypes(), ImmutableList.of(parseTypeSignature(StandardTypes.BIGINT)));
        assertEquals(signature.getReturnType().getBase(), StandardTypes.TIMESTAMP_WITH_TIME_ZONE);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "\\QFunction already registered: custom_add(bigint,bigint):bigint\\E")
    public void testDuplicateFunctions()
    {
        List<SqlFunction> functions = new FunctionListBuilder()
                .scalars(CustomFunctions.class)
                .getFunctions()
                .stream()
                .filter(input -> input.getSignature().getName().equals("custom_add"))
                .collect(toImmutableList());

        TypeRegistry typeManager = new TypeRegistry();
        StaticFunctionNamespace staticFunctionNamespace = createStaticFunctionNamespace(typeManager);
        staticFunctionNamespace.addFunctions(functions);
        staticFunctionNamespace.addFunctions(functions);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "'sum' is both an aggregation and a scalar function")
    public void testConflictingScalarAggregation()
    {
        List<SqlFunction> functions = new FunctionListBuilder()
                .scalars(ScalarSum.class)
                .getFunctions();

        TypeRegistry typeManager = new TypeRegistry();
        StaticFunctionNamespace staticFunctionNamespace = createStaticFunctionNamespace(typeManager);
        staticFunctionNamespace.addFunctions(functions);
    }

    @Test
    public void testListingHiddenFunctions()
    {
        TypeRegistry typeManager = new TypeRegistry();
        StaticFunctionNamespace staticFunctionNamespace = createStaticFunctionNamespace(typeManager);
        List<SqlFunction> functions = staticFunctionNamespace.listFunctions();
        List<String> names = transform(functions, input -> input.getSignature().getName());

        assertTrue(names.contains("length"), "Expected function names " + names + " to contain 'length'");
        assertTrue(names.contains("stddev"), "Expected function names " + names + " to contain 'stddev'");
        assertTrue(names.contains("rank"), "Expected function names " + names + " to contain 'rank'");
        assertFalse(names.contains("like"), "Expected function names " + names + " not to contain 'like'");
        assertFalse(names.contains("$internal$sum_data_size_for_stats"), "Expected function names " + names + " not to contain '$internal$sum_data_size_for_stats'");
        assertFalse(names.contains("$internal$max_data_size_for_stats"), "Expected function names " + names + " not to contain '$internal$max_data_size_for_stats'");
    }

    @Test
    public void testOperatorTypes()
    {
        TypeRegistry typeManager = new TypeRegistry();
        FunctionManager functionManager = new FunctionManager(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig());
        FunctionResolution functionResolution = new FunctionResolution(functionManager);

        assertTrue(functionManager.getFunctionMetadata(functionResolution.arithmeticFunction(ADD, BIGINT, BIGINT)).getOperatorType().map(OperatorType::isArithmeticOperator).orElse(false));
        assertFalse(functionManager.getFunctionMetadata(functionResolution.arithmeticFunction(ADD, BIGINT, BIGINT)).getOperatorType().map(OperatorType::isComparisonOperator).orElse(true));
        assertTrue(functionManager.getFunctionMetadata(functionResolution.comparisonFunction(GREATER_THAN, BIGINT, BIGINT)).getOperatorType().map(OperatorType::isComparisonOperator).orElse(false));
        assertFalse(functionManager.getFunctionMetadata(functionResolution.comparisonFunction(GREATER_THAN, BIGINT, BIGINT)).getOperatorType().map(OperatorType::isArithmeticOperator).orElse(true));
        assertFalse(functionManager.getFunctionMetadata(functionResolution.notFunction()).getOperatorType().isPresent());
    }

    @Test
    public void testResolveFunctionByExactMatch()
    {
        assertThatResolveFunction()
                .among(functionSignature("bigint", "bigint"))
                .forParameters("bigint", "bigint")
                .returns(functionSignature("bigint", "bigint"));
    }

    @Test
    public void testResolveTypeParametrizedFunction()
    {
        assertThatResolveFunction()
                .among(functionSignature(ImmutableList.of("T", "T"), "boolean", ImmutableList.of(typeVariable("T"))))
                .forParameters("bigint", "bigint")
                .returns(functionSignature("bigint", "bigint"));
    }

    @Test
    public void testResolveFunctionWithCoercion()
    {
        assertThatResolveFunction()
                .among(
                        functionSignature("decimal(p,s)", "double"),
                        functionSignature("decimal(p,s)", "decimal(p,s)"),
                        functionSignature("double", "double"))
                .forParameters("bigint", "bigint")
                .returns(functionSignature("decimal(19,0)", "decimal(19,0)"));
    }

    @Test
    public void testAmbiguousCallWithNoCoercion()
    {
        assertThatResolveFunction()
                .among(
                        functionSignature("decimal(p,s)", "decimal(p,s)"),
                        functionSignature(ImmutableList.of("T", "T"), "boolean", ImmutableList.of(typeVariable("T"))))
                .forParameters("decimal(3,1)", "decimal(3,1)")
                .returns(functionSignature("decimal(3,1)", "decimal(3,1)"));
    }

    @Test
    public void testAmbiguousCallWithCoercion()
    {
        assertThatResolveFunction()
                .among(
                        functionSignature("decimal(p,s)", "double"),
                        functionSignature("double", "decimal(p,s)"))
                .forParameters("bigint", "bigint")
                .failsWithMessage("Could not choose a best candidate operator. Explicit type casts must be added.");
    }

    @Test
    public void testResolveFunctionWithCoercionInTypes()
    {
        assertThatResolveFunction()
                .among(
                        functionSignature("array(decimal(p,s))", "array(double)"),
                        functionSignature("array(decimal(p,s))", "array(decimal(p,s))"),
                        functionSignature("array(double)", "array(double)"))
                .forParameters("array(bigint)", "array(bigint)")
                .returns(functionSignature("array(decimal(19,0))", "array(decimal(19,0))"));
    }

    @Test
    public void testResolveFunctionWithVariableArity()
    {
        assertThatResolveFunction()
                .among(
                        functionSignature("double", "double", "double"),
                        functionSignature("decimal(p,s)").setVariableArity(true))
                .forParameters("bigint", "bigint", "bigint")
                .returns(functionSignature("decimal(19,0)", "decimal(19,0)", "decimal(19,0)"));

        assertThatResolveFunction()
                .among(
                        functionSignature("double", "double", "double"),
                        functionSignature("bigint").setVariableArity(true))
                .forParameters("bigint", "bigint", "bigint")
                .returns(functionSignature("bigint", "bigint", "bigint"));
    }

    @Test
    public void testResolveFunctionWithVariadicBound()
    {
        assertThatResolveFunction()
                .among(
                        functionSignature("bigint", "bigint", "bigint"),
                        functionSignature(
                                ImmutableList.of("T1", "T2", "T3"),
                                "boolean",
                                ImmutableList.of(Signature.withVariadicBound("T1", "decimal"),
                                        Signature.withVariadicBound("T2", "decimal"),
                                        Signature.withVariadicBound("T3", "decimal"))))
                .forParameters("unknown", "bigint", "bigint")
                .returns(functionSignature("bigint", "bigint", "bigint"));
    }

    @Test
    public void testResolveFunctionForUnknown()
    {
        assertThatResolveFunction()
                .among(
                        functionSignature("bigint"))
                .forParameters("unknown")
                .returns(functionSignature("bigint"));

        // when coercion between the types exist, and the most specific function can be determined with the main algorithm
        assertThatResolveFunction()
                .among(
                        functionSignature("bigint"),
                        functionSignature("integer"))
                .forParameters("unknown")
                .returns(functionSignature("integer"));

        // function that requires only unknown coercion must be preferred
        assertThatResolveFunction()
                .among(
                        functionSignature("bigint", "bigint"),
                        functionSignature("integer", "integer"))
                .forParameters("unknown", "bigint")
                .returns(functionSignature("bigint", "bigint"));

        // when coercion between the types doesn't exist, but the return type is the same, so the random function must be chosen
        assertThatResolveFunction()
                .among(
                        functionSignature(ImmutableList.of("JoniRegExp"), "boolean"),
                        functionSignature(ImmutableList.of("integer"), "boolean"))
                .forParameters("unknown")
                // any function can be selected, but to make it deterministic we sort function signatures alphabetically
                .returns(functionSignature("integer"));

        // when the return type is different
        assertThatResolveFunction()
                .among(
                        functionSignature(ImmutableList.of("JoniRegExp"), "JoniRegExp"),
                        functionSignature(ImmutableList.of("integer"), "integer"))
                .forParameters("unknown")
                .failsWithMessage("Could not choose a best candidate operator. Explicit type casts must be added.");
    }

    private StaticFunctionNamespace createStaticFunctionNamespace(TypeRegistry typeManager)
    {
        BlockEncodingManager blockEncodingManager = new BlockEncodingManager(typeManager);
        FeaturesConfig featuresConfig = new FeaturesConfig();
        FunctionManager functionManager = new FunctionManager(typeManager, blockEncodingManager, featuresConfig);
        return new StaticFunctionNamespace(typeManager, blockEncodingManager, featuresConfig, functionManager);
    }

    private SignatureBuilder functionSignature(String... argumentTypes)
    {
        return functionSignature(ImmutableList.copyOf(argumentTypes), "boolean");
    }

    private static SignatureBuilder functionSignature(List<String> arguments, String returnType)
    {
        return functionSignature(arguments, returnType, ImmutableList.of());
    }

    private static SignatureBuilder functionSignature(List<String> arguments, String returnType, List<TypeVariableConstraint> typeVariableConstraints)
    {
        ImmutableSet<String> literalParameters = ImmutableSet.of("p", "s", "p1", "s1", "p2", "s2", "p3", "s3");
        List<TypeSignature> argumentSignatures = arguments.stream()
                .map((signature) -> TypeSignature.parseTypeSignature(signature, literalParameters))
                .collect(toImmutableList());
        return new SignatureBuilder()
                .returnType(TypeSignature.parseTypeSignature(returnType, literalParameters))
                .argumentTypes(argumentSignatures)
                .typeVariableConstraints(typeVariableConstraints)
                .kind(SCALAR);
    }

    private static ResolveFunctionAssertion assertThatResolveFunction()
    {
        return new ResolveFunctionAssertion();
    }

    private static class ResolveFunctionAssertion
    {
        private static final String TEST_FUNCTION_NAME = "TEST_FUNCTION_NAME";

        private final TypeRegistry typeRegistry = new TypeRegistry();
        private final BlockEncodingSerde blockEncoding = new BlockEncodingManager(typeRegistry);

        private List<SignatureBuilder> functionSignatures = ImmutableList.of();
        private List<TypeSignature> parameterTypes = ImmutableList.of();

        public ResolveFunctionAssertion among(SignatureBuilder... functionSignatures)
        {
            this.functionSignatures = ImmutableList.copyOf(functionSignatures);
            return this;
        }

        public ResolveFunctionAssertion forParameters(String... parameters)
        {
            this.parameterTypes = parseTypeSignatures(parameters);
            return this;
        }

        public ResolveFunctionAssertion returns(SignatureBuilder functionSignature)
        {
            FunctionHandle expectedFunction = new StaticFunctionHandle(functionSignature.name(TEST_FUNCTION_NAME).build());
            FunctionHandle actualFunction = resolveFunctionHandle();
            assertEquals(expectedFunction, actualFunction);
            return this;
        }

        public ResolveFunctionAssertion failsWithMessage(String... messages)
        {
            try {
                resolveFunctionHandle();
                fail("didn't fail as expected");
            }
            catch (RuntimeException e) {
                String actualMessage = e.getMessage();
                for (String expectedMessage : messages) {
                    if (!actualMessage.contains(expectedMessage)) {
                        fail(format("%s doesn't contain %s", actualMessage, expectedMessage));
                    }
                }
            }
            return this;
        }

        private FunctionHandle resolveFunctionHandle()
        {
            FeaturesConfig featuresConfig = new FeaturesConfig();
            FunctionManager functionManager = new FunctionManager(typeRegistry, blockEncoding, featuresConfig);
            StaticFunctionNamespace staticFunctionNamespace = new StaticFunctionNamespace(typeRegistry, blockEncoding, featuresConfig, functionManager);
            staticFunctionNamespace.addFunctions(createFunctionsFromSignatures());
            return staticFunctionNamespace.resolveFunction(QualifiedName.of(TEST_FUNCTION_NAME), fromTypeSignatures(parameterTypes));
        }

        private List<SqlFunction> createFunctionsFromSignatures()
        {
            ImmutableList.Builder<SqlFunction> functions = ImmutableList.builder();
            for (SignatureBuilder functionSignature : functionSignatures) {
                Signature signature = functionSignature.name(TEST_FUNCTION_NAME).build();
                functions.add(new SqlScalarFunction(signature)
                {
                    @Override
                    public ScalarFunctionImplementation specialize(
                            BoundVariables boundVariables,
                            int arity,
                            TypeManager typeManager,
                            FunctionManager functionManager)
                    {
                        return new ScalarFunctionImplementation(
                                false,
                                nCopies(arity, valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                                MethodHandles.identity(Void.class));
                    }

                    @Override
                    public boolean isHidden()
                    {
                        return false;
                    }

                    @Override
                    public boolean isDeterministic()
                    {
                        return false;
                    }

                    @Override
                    public String getDescription()
                    {
                        return "testing function that does nothing";
                    }
                });
            }
            return functions.build();
        }

        private static List<TypeSignature> parseTypeSignatures(String... signatures)
        {
            return ImmutableList.copyOf(signatures)
                    .stream()
                    .map(TypeSignature::parseTypeSignature)
                    .collect(toList());
        }
    }

    public static final class ScalarSum
    {
        private ScalarSum() {}

        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        public static long sum(@SqlType(StandardTypes.BIGINT) long a, @SqlType(StandardTypes.BIGINT) long b)
        {
            return a + b;
        }
    }
}
