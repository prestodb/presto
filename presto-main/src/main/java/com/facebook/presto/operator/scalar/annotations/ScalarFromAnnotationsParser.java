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
package com.facebook.presto.operator.scalar.annotations;

import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.ParametricImplementationsGroup;
import com.facebook.presto.operator.annotations.FunctionsParserHelper;
import com.facebook.presto.operator.scalar.ParametricScalar;
import com.facebook.presto.operator.scalar.ScalarHeader;
import com.facebook.presto.operator.scalar.annotations.ParametricScalarImplementation.SpecializedSignature;
import com.facebook.presto.spi.function.CodegenScalarFunction;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarFunctionConstantStats;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.ScalarPropagateSourceStats;
import com.facebook.presto.spi.function.ScalarStatsHeader;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlInvokedScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.operator.scalar.annotations.OperatorValidator.validateOperator;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class ScalarFromAnnotationsParser
{
    private ScalarFromAnnotationsParser() {}

    public static List<SqlScalarFunction> parseFunctionDefinition(Class<?> clazz)
    {
        ImmutableList.Builder<SqlScalarFunction> builder = ImmutableList.builder();
        for (ScalarHeaderAndMethods scalar : findScalarsInFunctionDefinitionClass(clazz)) {
            builder.add(parseParametricScalar(scalar, FunctionsParserHelper.findConstructor(clazz)));
        }
        return builder.build();
    }

    public static List<SqlScalarFunction> parseFunctionDefinitions(Class<?> clazz)
    {
        ImmutableList.Builder<SqlScalarFunction> builder = ImmutableList.builder();
        for (ScalarHeaderAndMethods methods : findScalarsInFunctionSetClass(clazz)) {
            // Non-static function only makes sense in classes annotated @ScalarFunction.
            builder.add(parseParametricScalar(methods, Optional.empty()));
        }
        return builder.build();
    }

    private static List<ScalarHeaderAndMethods> findScalarsInFunctionDefinitionClass(Class<?> annotated)
    {
        ImmutableList.Builder<ScalarHeaderAndMethods> builder = ImmutableList.builder();
        List<ScalarImplementationHeader> classHeaders = ScalarImplementationHeader.fromAnnotatedElement(annotated);
        checkArgument(!classHeaders.isEmpty(), "Class [%s] that defines function must be annotated with @ScalarFunction or @ScalarOperator", annotated.getName());

        for (ScalarImplementationHeader header : classHeaders) {
            Set<Method> methods = FunctionsParserHelper.findPublicMethods(annotated, SqlType.class, ScalarFunction.class, ScalarOperator.class);
            checkCondition(!methods.isEmpty(), FUNCTION_IMPLEMENTATION_ERROR, "Parametric class [%s] does not have any annotated methods", annotated.getName());
            for (Method method : methods) {
                checkArgument(method.getAnnotation(ScalarFunction.class) == null, "Parametric class method [%s] is annotated with @ScalarFunction", method);
                checkArgument(method.getAnnotation(ScalarOperator.class) == null, "Parametric class method [%s] is annotated with @ScalarOperator", method);
            }
            builder.add(new ScalarHeaderAndMethods(header, methods));
        }

        return builder.build();
    }

    private static List<ScalarHeaderAndMethods> findScalarsInFunctionSetClass(Class<?> annotated)
    {
        ImmutableList.Builder<ScalarHeaderAndMethods> builder = ImmutableList.builder();
        for (Method method : FunctionsParserHelper.findPublicMethods(
                annotated,
                ImmutableSet.of(SqlType.class, ScalarFunction.class, ScalarOperator.class),
                ImmutableSet.of(SqlInvokedScalarFunction.class, CodegenScalarFunction.class))) {
            checkCondition((method.getAnnotation(ScalarFunction.class) != null) || (method.getAnnotation(ScalarOperator.class) != null),
                    FUNCTION_IMPLEMENTATION_ERROR, "Method [%s] annotated with @SqlType is missing @ScalarFunction or @ScalarOperator", method);
            for (ScalarImplementationHeader header : ScalarImplementationHeader.fromAnnotatedElement(method)) {
                builder.add(new ScalarHeaderAndMethods(header, ImmutableSet.of(method)));
            }
        }
        return builder.build();
    }

    private static Optional<ScalarStatsHeader> getScalarStatsHeader(Method annotated)
    {
        Optional<ScalarStatsHeader> scalarStatsHeader;
        ScalarFunctionConstantStats statsCalculator = annotated.getAnnotation(ScalarFunctionConstantStats.class);
        List<java.lang.reflect.Parameter> params = Arrays.stream(annotated.getParameters()).filter(x -> x.getAnnotation(SqlType.class) != null).collect(Collectors.toList());
        // Map of (function argument position index) -> (ScalarPropagateSourceStats annotation)
        Map<Integer, ScalarPropagateSourceStats> argumentStatsResolver = new HashMap<>();

        IntStream.range(0, params.size())
                .filter(x -> params.get(x).getAnnotation(ScalarPropagateSourceStats.class) != null)
                .forEachOrdered(x -> argumentStatsResolver.put(x, params.get(x).getAnnotation(ScalarPropagateSourceStats.class)));

        scalarStatsHeader = Optional.ofNullable(statsCalculator).map(x -> new ScalarStatsHeader(x, argumentStatsResolver));
        if (!argumentStatsResolver.isEmpty() && !scalarStatsHeader.isPresent()) {
            scalarStatsHeader = Optional.of(new ScalarStatsHeader(argumentStatsResolver));
        }
        return scalarStatsHeader;
    }

    private static SqlScalarFunction parseParametricScalar(ScalarHeaderAndMethods scalar, Optional<Constructor<?>> constructor)
    {
        ScalarImplementationHeader header = scalar.getHeader();

        Map<SpecializedSignature, ParametricScalarImplementation.Builder> signatures = new HashMap<>();
        Map<Signature, ScalarStatsHeader> signatureVsStatsHeader = new HashMap<>();
        for (Method method : scalar.getMethods()) {
            ParametricScalarImplementation implementation = ParametricScalarImplementation.Parser.parseImplementation(header, method, constructor);
            if (!signatures.containsKey(implementation.getSpecializedSignature())) {
                ParametricScalarImplementation.Builder builder = new ParametricScalarImplementation.Builder(
                        implementation.getSignature(),
                        implementation.getArgumentNativeContainerTypes(),
                        implementation.getSpecializedTypeParameters(),
                        implementation.getReturnNativeContainerType());
                signatures.put(implementation.getSpecializedSignature(), builder);
                builder.addChoices(implementation);
            }
            else {
                ParametricScalarImplementation.Builder builder = signatures.get(implementation.getSpecializedSignature());
                builder.addChoices(implementation);
            }
            Optional<ScalarStatsHeader> scalarStatsHeader = getScalarStatsHeader(method);
            scalarStatsHeader.ifPresent(statsHeader -> signatureVsStatsHeader.put(implementation.getSignature().canonicalization(), statsHeader));
        }

        ParametricImplementationsGroup.Builder<ParametricScalarImplementation> implementationsBuilder = ParametricImplementationsGroup.builder();
        for (ParametricScalarImplementation.Builder implementation : signatures.values()) {
            implementationsBuilder.addImplementation(implementation.build());
        }
        ParametricImplementationsGroup<ParametricScalarImplementation> implementations = implementationsBuilder.build();
        Signature scalarSignature = implementations.getSignature();

        header.getOperatorType().ifPresent(operatorType ->
                validateOperator(operatorType, scalarSignature.getReturnType(), scalarSignature.getArgumentTypes()));

        ScalarHeader scalarHeader = header.getHeader();
        ScalarHeader headerWithStats =
                new ScalarHeader(scalarHeader.getDescription(), scalarHeader.getVisibility(), scalarHeader.isDeterministic(), scalarHeader.isCalledOnNullInput(), signatureVsStatsHeader);
        return new ParametricScalar(scalarSignature, headerWithStats, implementations);
    }

    private static class ScalarHeaderAndMethods
    {
        private final ScalarImplementationHeader header;
        private final Set<Method> methods;

        public ScalarHeaderAndMethods(ScalarImplementationHeader header, Set<Method> methods)
        {
            this.header = requireNonNull(header);
            this.methods = requireNonNull(methods);
        }

        public ScalarImplementationHeader getHeader()
        {
            return header;
        }

        public Set<Method> getMethods()
        {
            return methods;
        }
    }
}
