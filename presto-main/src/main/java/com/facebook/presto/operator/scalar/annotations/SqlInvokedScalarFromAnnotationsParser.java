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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.CodegenScalarFunction;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.function.SqlInvokedScalarFunction;
import com.facebook.presto.spi.function.SqlParameter;
import com.facebook.presto.spi.function.SqlParameters;
import com.facebook.presto.spi.function.SqlType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.operator.annotations.FunctionsParserHelper.findPublicStaticMethods;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static com.facebook.presto.spi.function.FunctionVersion.notVersioned;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Determinism.DETERMINISTIC;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Determinism.NOT_DETERMINISTIC;
import static com.facebook.presto.spi.function.RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT;
import static com.facebook.presto.spi.function.RoutineCharacteristics.NullCallClause.RETURNS_NULL_ON_NULL_INPUT;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Arrays.stream;

public final class SqlInvokedScalarFromAnnotationsParser
{
    private SqlInvokedScalarFromAnnotationsParser() {}

    public static List<SqlInvokedFunction> parseFunctionDefinition(Class<?> clazz)
    {
        checkArgument(clazz.isAnnotationPresent(SqlInvokedScalarFunction.class), "Class is not annotated with SqlInvokedScalarFunction: %s", clazz.getName());

        SqlInvokedScalarFunction header = clazz.getAnnotation(SqlInvokedScalarFunction.class);
        Optional<String> description = Optional.ofNullable(clazz.getAnnotation(Description.class)).map(Description::value);

        return findScalarsInFunctionDefinitionClass(clazz).stream()
                .map(method -> createSqlInvokedFunctions(method, Optional.of(header), description))
                .flatMap(List::stream)
                .collect(toImmutableList());
    }

    public static List<SqlInvokedFunction> parseFunctionDefinitions(Class<?> clazz)
    {
        return findScalarsInFunctionSetClass(clazz).stream()
                .map(method -> createSqlInvokedFunctions(method, Optional.empty(), Optional.empty()))
                .flatMap(List::stream)
                .collect(toImmutableList());
    }

    private static List<Method> findScalarsInFunctionDefinitionClass(Class<?> clazz)
    {
        Set<Method> methods = findPublicStaticMethods(
                clazz,
                ImmutableSet.of(SqlInvokedScalarFunction.class, SqlType.class, SqlParameter.class, SqlParameters.class, Description.class),
                ImmutableSet.of(ScalarFunction.class, ScalarOperator.class));
        for (Method method : methods) {
            checkCondition(
                    !method.isAnnotationPresent(SqlInvokedScalarFunction.class) && !method.isAnnotationPresent(Description.class),
                    FUNCTION_IMPLEMENTATION_ERROR,
                    "Function-defining method [%s] cannot have @SqlInvokedScalarFunction",
                    method);
            checkCondition(
                    method.isAnnotationPresent(SqlType.class),
                    FUNCTION_IMPLEMENTATION_ERROR,
                    "Function-defining method [%s] is missing @SqlType",
                    method);
            checkReturnString(method);
        }

        return ImmutableList.copyOf(methods);
    }

    private static List<Method> findScalarsInFunctionSetClass(Class<?> clazz)
    {
        Set<Method> methods = findPublicStaticMethods(
                clazz,
                ImmutableSet.of(SqlInvokedScalarFunction.class, SqlType.class, SqlParameter.class, SqlParameters.class, Description.class),
                ImmutableSet.of(ScalarFunction.class, ScalarOperator.class, CodegenScalarFunction.class));
        for (Method method : methods) {
            checkCondition(
                    method.isAnnotationPresent(SqlInvokedScalarFunction.class) && method.isAnnotationPresent(SqlType.class),
                    FUNCTION_IMPLEMENTATION_ERROR,
                    "Function-defining method [%s] is missing @SqlInvokedScalarFunction or @SqlType",
                    method);
            checkReturnString(method);
        }
        return ImmutableList.copyOf(methods);
    }

    private static List<SqlInvokedFunction> createSqlInvokedFunctions(Method method, Optional<SqlInvokedScalarFunction> header, Optional<String> description)
    {
        SqlInvokedScalarFunction functionHeader = header.orElse(method.getAnnotation(SqlInvokedScalarFunction.class));
        String functionDescription = description.orElse(method.isAnnotationPresent(Description.class) ? method.getAnnotation(Description.class).value() : "");
        TypeSignature returnType = parseTypeSignature(method.getAnnotation(SqlType.class).value());

        // Parameter
        checkCondition(
                !method.isAnnotationPresent(SqlParameter.class) || !method.isAnnotationPresent(SqlParameters.class),
                FUNCTION_IMPLEMENTATION_ERROR,
                "Function-defining method [%s] is annotated with both @SqlParameter and @SqlParameters",
                method);
        List<Parameter> parameters;
        if (method.isAnnotationPresent(SqlParameter.class)) {
            parameters = ImmutableList.of(getParameterFromAnnotation(method.getAnnotation(SqlParameter.class)));
        }
        else if (method.isAnnotationPresent(SqlParameters.class)) {
            parameters = stream(method.getAnnotation(SqlParameters.class).value())
                    .map(SqlInvokedScalarFromAnnotationsParser::getParameterFromAnnotation)
                    .collect(toImmutableList());
        }
        else {
            parameters = ImmutableList.of();
        }

        // Routine characteristics
        RoutineCharacteristics routineCharacteristics = RoutineCharacteristics.builder()
                .setLanguage(RoutineCharacteristics.Language.SQL)
                .setDeterminism(functionHeader.deterministic() ? DETERMINISTIC : NOT_DETERMINISTIC)
                .setNullCallClause(functionHeader.calledOnNullInput() ? CALLED_ON_NULL_INPUT : RETURNS_NULL_ON_NULL_INPUT)
                .build();

        String body;
        try {
            body = (String) method.invoke(null);
        }
        catch (ReflectiveOperationException e) {
            throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, format("Failed to get function body for method [%s]", method), e);
        }

        return Stream.concat(Stream.of(functionHeader.value()), stream(functionHeader.alias()))
                .map(name -> new SqlInvokedFunction(
                        QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, name),
                        parameters,
                        returnType,
                        functionDescription,
                        routineCharacteristics,
                        body,
                        notVersioned()))
                .collect(toImmutableList());
    }

    private static Parameter getParameterFromAnnotation(SqlParameter sqlParameter)
    {
        return new Parameter(sqlParameter.name(), parseTypeSignature(sqlParameter.type()));
    }

    private static void checkReturnString(Method method)
    {
        checkCondition(method.getReturnType().equals(String.class), FUNCTION_IMPLEMENTATION_ERROR, "Function-defining method [%s] must return String");
    }
}
