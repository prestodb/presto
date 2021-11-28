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

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation;
import com.facebook.presto.operator.scalar.CustomFunctions;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeVariableConstraint;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.SystemSessionProperties.EXPERIMENTAL_FUNCTIONS_ENABLED;
import static com.facebook.presto.common.function.OperatorType.CAST;
import static com.facebook.presto.common.function.OperatorType.SATURATED_FLOOR_CAST;
import static com.facebook.presto.common.function.OperatorType.tryGetOperatorType;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.metadata.FunctionAndTypeManager.qualifyObjectName;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.function.FunctionVersion.notVersioned;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static com.facebook.presto.sql.planner.LiteralEncoder.getMagicLiteralFunctionSignature;
import static com.facebook.presto.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Lists.transform;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestFunctionAndTypeManager
{
    @Test
    public void testIdentityCast()
    {
        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
        FunctionHandle exactOperator = functionAndTypeManager.lookupCast(CastType.CAST, HYPER_LOG_LOG.getTypeSignature(), HYPER_LOG_LOG.getTypeSignature());
        assertEquals(exactOperator, new BuiltInFunctionHandle(new Signature(CAST.getFunctionName(), SCALAR, HYPER_LOG_LOG.getTypeSignature(), HYPER_LOG_LOG.getTypeSignature())));
    }

    @Test
    public void testExactMatchBeforeCoercion()
    {
        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
        boolean foundOperator = false;
        for (SqlFunction function : functionAndTypeManager.listOperators()) {
            OperatorType operatorType = tryGetOperatorType(function.getSignature().getName()).get();
            if (operatorType == CAST || operatorType == SATURATED_FLOOR_CAST) {
                continue;
            }
            if (!function.getSignature().getTypeVariableConstraints().isEmpty()) {
                continue;
            }
            if (function.getSignature().getArgumentTypes().stream().anyMatch(TypeSignature::isCalculated)) {
                continue;
            }
            BuiltInFunctionHandle exactOperator = (BuiltInFunctionHandle) functionAndTypeManager.resolveOperator(operatorType, fromTypeSignatures(function.getSignature().getArgumentTypes()));
            assertEquals(exactOperator.getSignature(), function.getSignature());
            foundOperator = true;
        }
        assertTrue(foundOperator);
    }

    @Test
    public void testMagicLiteralFunction()
    {
        Signature signature = getMagicLiteralFunctionSignature(TIMESTAMP_WITH_TIME_ZONE);
        assertEquals(signature.getNameSuffix(), "$literal$timestamp with time zone");
        assertEquals(signature.getArgumentTypes(), ImmutableList.of(parseTypeSignature(StandardTypes.BIGINT)));
        assertEquals(signature.getReturnType().getBase(), StandardTypes.TIMESTAMP_WITH_TIME_ZONE);

        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
        BuiltInFunctionHandle functionHandle = (BuiltInFunctionHandle) functionAndTypeManager.resolveFunction(
                Optional.empty(),
                TEST_SESSION.getTransactionId(),
                signature.getName(),
                fromTypeSignatures(signature.getArgumentTypes()));
        assertEquals(functionAndTypeManager.getFunctionMetadata(functionHandle).getArgumentTypes(), ImmutableList.of(parseTypeSignature(StandardTypes.BIGINT)));
        assertEquals(signature.getReturnType().getBase(), StandardTypes.TIMESTAMP_WITH_TIME_ZONE);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "\\QFunction already registered: presto.default.custom_add(bigint,bigint):bigint\\E")
    public void testDuplicateFunctions()
    {
        List<SqlFunction> functions = new FunctionListBuilder()
                .scalars(CustomFunctions.class)
                .getFunctions()
                .stream()
                .filter(input -> input.getSignature().getNameSuffix().equals("custom_add"))
                .collect(toImmutableList());

        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
        functionAndTypeManager.registerBuiltInFunctions(functions);
        functionAndTypeManager.registerBuiltInFunctions(functions);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "'presto.default.sum' is both an aggregation and a scalar function")
    public void testConflictingScalarAggregation()
    {
        List<SqlFunction> functions = new FunctionListBuilder()
                .scalars(ScalarSum.class)
                .getFunctions();

        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
        functionAndTypeManager.registerBuiltInFunctions(functions);
    }

    @Test
    public void testListingVisibilityBetaFunctionsDisabled()
    {
        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
        List<SqlFunction> functions = functionAndTypeManager.listFunctions(TEST_SESSION, Optional.empty(), Optional.empty());
        List<String> names = transform(functions, input -> input.getSignature().getNameSuffix());

        assertTrue(names.contains("length"), "Expected function names " + names + " to contain 'length'");
        assertTrue(names.contains("stddev"), "Expected function names " + names + " to contain 'stddev'");
        assertTrue(names.contains("rank"), "Expected function names " + names + " to contain 'rank'");
        assertFalse(names.contains("tdigest_agg"), "Expected function names " + names + " not to contain 'tdigest_agg'");
        assertFalse(names.contains("quantiles_at_values"), "Expected function names " + names + " not to contain 'quantiles_at_values'");
        assertFalse(names.contains("like"), "Expected function names " + names + " not to contain 'like'");
        assertFalse(names.contains("$internal$sum_data_size_for_stats"), "Expected function names " + names + " not to contain '$internal$sum_data_size_for_stats'");
        assertFalse(names.contains("$internal$max_data_size_for_stats"), "Expected function names " + names + " not to contain '$internal$max_data_size_for_stats'");
    }

    @Test
    public void testListingVisibilityBetaFunctionsEnabled()
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(EXPERIMENTAL_FUNCTIONS_ENABLED, "true")
                .build();
        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
        List<SqlFunction> functions = functionAndTypeManager.listFunctions(session, Optional.empty(), Optional.empty());
        List<String> names = transform(functions, input -> input.getSignature().getNameSuffix());

        assertTrue(names.contains("length"), "Expected function names " + names + " to contain 'length'");
        assertTrue(names.contains("stddev"), "Expected function names " + names + " to contain 'stddev'");
        assertTrue(names.contains("rank"), "Expected function names " + names + " to contain 'rank'");
        assertTrue(names.contains("tdigest_agg"), "Expected function names " + names + " to contain 'tdigest_agg'");
        assertTrue(names.contains("quantiles_at_values"), "Expected function names " + names + " to contain 'tdigest_agg'");
        assertFalse(names.contains("like"), "Expected function names " + names + " not to contain 'like'");
        assertFalse(names.contains("$internal$sum_data_size_for_stats"), "Expected function names " + names + " not to contain '$internal$sum_data_size_for_stats'");
        assertFalse(names.contains("$internal$max_data_size_for_stats"), "Expected function names " + names + " not to contain '$internal$max_data_size_for_stats'");
    }

    @Test
    public void testOperatorTypes()
    {
        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
        FunctionResolution functionResolution = new FunctionResolution(functionAndTypeManager);

        assertTrue(functionAndTypeManager.getFunctionMetadata(functionResolution.arithmeticFunction(ADD, BIGINT, BIGINT)).getOperatorType().map(OperatorType::isArithmeticOperator).orElse(false));
        assertFalse(functionAndTypeManager.getFunctionMetadata(functionResolution.arithmeticFunction(ADD, BIGINT, BIGINT)).getOperatorType().map(OperatorType::isComparisonOperator).orElse(true));
        assertTrue(functionAndTypeManager.getFunctionMetadata(functionResolution.comparisonFunction(GREATER_THAN, BIGINT, BIGINT)).getOperatorType().map(OperatorType::isComparisonOperator).orElse(false));
        assertFalse(functionAndTypeManager.getFunctionMetadata(functionResolution.comparisonFunction(GREATER_THAN, BIGINT, BIGINT)).getOperatorType().map(OperatorType::isArithmeticOperator).orElse(true));
        assertFalse(functionAndTypeManager.getFunctionMetadata(functionResolution.notFunction()).getOperatorType().isPresent());
    }

    @Test
    public void testSessionFunctions()
    {
        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();

        SqlFunctionId bigintSignature = new SqlFunctionId(QualifiedObjectName.valueOf("presto.default.foo"), ImmutableList.of(parseTypeSignature("bigint")));
        SqlInvokedFunction bigintFunction = new SqlInvokedFunction(
                bigintSignature.getFunctionName(),
                ImmutableList.of(new Parameter("x", parseTypeSignature("bigint"))),
                parseTypeSignature("bigint"),
                "",
                RoutineCharacteristics.builder().build(),
                "",
                notVersioned());

        SqlFunctionId varcharSignature = new SqlFunctionId(QualifiedObjectName.valueOf("presto.default.foo"), ImmutableList.of(parseTypeSignature("varchar")));
        SqlInvokedFunction varcharFunction = new SqlInvokedFunction(
                bigintSignature.getFunctionName(),
                ImmutableList.of(new Parameter("x", parseTypeSignature("varchar"))),
                parseTypeSignature("varchar"),
                "",
                RoutineCharacteristics.builder().build(),
                "",
                notVersioned());

        Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions = ImmutableMap.of(bigintSignature, bigintFunction, varcharSignature, varcharFunction);

        assertEquals(
                functionAndTypeManager.resolveFunction(
                        Optional.of(sessionFunctions),
                        Optional.empty(),
                        bigintSignature.getFunctionName(),
                        ImmutableList.of(new TypeSignatureProvider(parseTypeSignature("bigint")))),
                new SessionFunctionHandle(bigintFunction));
        assertEquals(
                functionAndTypeManager.resolveFunction(
                        Optional.of(sessionFunctions),
                        Optional.empty(),
                        varcharSignature.getFunctionName(),
                        ImmutableList.of(new TypeSignatureProvider(parseTypeSignature("varchar")))),
                new SessionFunctionHandle(varcharFunction));
        assertEquals(
                functionAndTypeManager.resolveFunction(
                        Optional.of(sessionFunctions),
                        Optional.empty(),
                        bigintSignature.getFunctionName(),
                        ImmutableList.of(new TypeSignatureProvider(parseTypeSignature("int")))),
                new SessionFunctionHandle(bigintFunction));
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
                .map((signature) -> parseTypeSignature(signature, literalParameters))
                .collect(toImmutableList());
        return new SignatureBuilder()
                .returnType(parseTypeSignature(returnType, literalParameters))
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
            FunctionHandle expectedFunction = new BuiltInFunctionHandle(functionSignature.name(TEST_FUNCTION_NAME).build());
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
            FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
            functionAndTypeManager.registerBuiltInFunctions(createFunctionsFromSignatures());
            return functionAndTypeManager.resolveFunction(
                    Optional.empty(),
                    TEST_SESSION.getTransactionId(),
                    qualifyObjectName(QualifiedName.of(TEST_FUNCTION_NAME)),
                    fromTypeSignatures(parameterTypes));
        }

        private List<BuiltInFunction> createFunctionsFromSignatures()
        {
            ImmutableList.Builder<BuiltInFunction> functions = ImmutableList.builder();
            for (SignatureBuilder functionSignature : functionSignatures) {
                Signature signature = functionSignature.name(TEST_FUNCTION_NAME).build();
                functions.add(new SqlScalarFunction(signature)
                {
                    @Override
                    public BuiltInScalarFunctionImplementation specialize(
                            BoundVariables boundVariables,
                            int arity,
                            FunctionAndTypeManager functionAndTypeManager)
                    {
                        return new BuiltInScalarFunctionImplementation(
                                false,
                                nCopies(arity, valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                                MethodHandles.identity(Void.class));
                    }

                    @Override
                    public SqlFunctionVisibility getVisibility()
                    {
                        return PUBLIC;
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
