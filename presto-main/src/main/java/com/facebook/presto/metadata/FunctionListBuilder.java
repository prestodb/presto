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

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.aggregation.GenericAggregationFunctionFactory;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.operator.scalar.JsonPath;
import com.facebook.presto.operator.scalar.ReflectionParametricScalar;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.operator.scalar.ScalarOperator;
import com.facebook.presto.operator.window.ReflectionWindowFunctionSupplier;
import com.facebook.presto.operator.window.SqlWindowFunction;
import com.facebook.presto.operator.window.ValueWindowFunction;
import com.facebook.presto.operator.window.WindowFunction;
import com.facebook.presto.operator.window.WindowFunctionSupplier;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.type.LiteralParameters;
import com.facebook.presto.type.SqlType;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.primitives.Primitives;
import io.airlift.joni.Regex;

import javax.annotation.Nullable;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.FunctionKind.WINDOW;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class FunctionListBuilder
{
    private static final Set<Class<?>> NON_NULLABLE_ARGUMENT_TYPES = ImmutableSet.of(
            long.class,
            double.class,
            boolean.class,
            Regex.class,
            JsonPath.class);

    private final List<SqlFunction> functions = new ArrayList<>();
    private final TypeManager typeManager;

    public FunctionListBuilder(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    public FunctionListBuilder window(String name, Type returnType, List<? extends Type> argumentTypes, Class<? extends WindowFunction> functionClass)
    {
        WindowFunctionSupplier windowFunctionSupplier = new ReflectionWindowFunctionSupplier<>(
                new Signature(name, WINDOW, returnType.getTypeSignature(), Lists.transform(ImmutableList.copyOf(argumentTypes), Type::getTypeSignature)),
                functionClass);

        functions.add(new SqlWindowFunction(windowFunctionSupplier));
        return this;
    }

    public FunctionListBuilder window(String name, Class<? extends ValueWindowFunction> clazz, String typeVariable, String... argumentTypes)
    {
        Signature signature = new Signature(
                name,
                WINDOW,
                ImmutableList.of(typeParameter(typeVariable)),
                typeVariable,
                ImmutableList.copyOf(argumentTypes),
                false,
                ImmutableSet.of());
        functions.add(new SqlWindowFunction(new ReflectionWindowFunctionSupplier<>(signature, clazz)));
        return this;
    }

    public FunctionListBuilder aggregate(List<InternalAggregationFunction> functions)
    {
        for (InternalAggregationFunction function : functions) {
            aggregate(function);
        }
        return this;
    }

    public FunctionListBuilder aggregate(InternalAggregationFunction function)
    {
        String name = function.name();
        name = name.toLowerCase(ENGLISH);

        String description = getDescription(function.getClass());
        functions.add(SqlAggregationFunction.create(name, description, function));
        return this;
    }

    public FunctionListBuilder aggregate(Class<?> aggregationDefinition)
    {
        functions.addAll(GenericAggregationFunctionFactory.fromAggregationDefinition(aggregationDefinition, typeManager).listFunctions());
        return this;
    }

    public FunctionListBuilder scalar(
            Signature signature,
            MethodHandle function,
            Optional<MethodHandle> instanceFactory,
            boolean deterministic,
            String description,
            boolean hidden,
            boolean nullable,
            List<Boolean> nullableArguments,
            Set<String> literalParameters)
    {
        functions.add(SqlScalarFunction.create(
                signature,
                description,
                hidden,
                function,
                instanceFactory,
                deterministic,
                nullable,
                nullableArguments,
                literalParameters));
        return this;
    }

    private FunctionListBuilder operator(
            OperatorType operatorType,
            TypeSignature returnType,
            List<TypeSignature> argumentTypes,
            MethodHandle function,
            Optional<MethodHandle> instanceFactory,
            boolean nullable,
            List<Boolean> nullableArguments,
            Set<String> literalParameters)
    {
        operatorType.validateSignature(returnType, argumentTypes);
        functions.add(SqlOperator.create(
                operatorType,
                argumentTypes,
                returnType,
                function,
                instanceFactory,
                nullable,
                nullableArguments,
                literalParameters));
        return this;
    }

    public FunctionListBuilder scalar(Class<?> clazz)
    {
        ScalarFunction scalarAnnotation = clazz.getAnnotation(ScalarFunction.class);
        ScalarOperator operatorAnnotation = clazz.getAnnotation(ScalarOperator.class);
        if (scalarAnnotation != null || operatorAnnotation != null) {
            functions.add(ReflectionParametricScalar.parseDefinition(clazz));
            return this;
        }
        try {
            boolean foundOne = false;
            for (Method method : clazz.getMethods()) {
                foundOne = processScalarFunction(method) || foundOne;
                foundOne = processScalarOperator(method) || foundOne;
            }
            checkArgument(foundOne, "Expected class %s to be annotated with @%s, or contain at least one method annotated with @%s", clazz.getName(), ScalarFunction.class.getSimpleName(), ScalarFunction.class.getSimpleName());
        }
        catch (IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
        return this;
    }

    public FunctionListBuilder functions(SqlFunction... sqlFunctions)
    {
        for (SqlFunction sqlFunction : sqlFunctions) {
            function(sqlFunction);
        }
        return this;
    }

    public FunctionListBuilder function(SqlFunction sqlFunction)
    {
        requireNonNull(sqlFunction, "parametricFunction is null");
        functions.add(sqlFunction);
        return this;
    }

    private boolean processScalarFunction(Method method)
            throws IllegalAccessException
    {
        ScalarFunction scalarFunction = method.getAnnotation(ScalarFunction.class);
        if (scalarFunction == null) {
            return false;
        }
        checkValidMethod(method);
        Optional<MethodHandle> instanceFactory = getInstanceFactory(method);
        MethodHandle methodHandle = lookup().unreflect(method);
        String name = scalarFunction.value();
        if (name.isEmpty()) {
            name = camelToSnake(method.getName());
        }
        SqlType returnTypeAnnotation = method.getAnnotation(SqlType.class);
        checkArgument(returnTypeAnnotation != null, "Method %s return type does not have a @SqlType annotation", method);
        LiteralParameters literalParametersAnnotation = method.getAnnotation(LiteralParameters.class);
        Set<String> literalParameters = ImmutableSet.of();
        if (literalParametersAnnotation != null) {
            literalParameters = ImmutableSet.copyOf(literalParametersAnnotation.value());
        }

        Signature signature = new Signature(
                name.toLowerCase(ENGLISH),
                SCALAR,
                parseTypeSignature(returnTypeAnnotation.value(), literalParameters),
                parameterTypeSignatures(method, literalParameters));

        verifyMethodSignature(method, signature.getReturnType(), signature.getArgumentTypes(), typeManager);

        List<Boolean> nullableArguments = getNullableArguments(method);

        scalar(
                signature,
                methodHandle,
                instanceFactory,
                scalarFunction.deterministic(),
                getDescription(method),
                scalarFunction.hidden(),
                method.isAnnotationPresent(Nullable.class),
                nullableArguments,
                literalParameters);
        for (String alias : scalarFunction.alias()) {
            scalar(signature.withAlias(alias.toLowerCase(ENGLISH)),
                    methodHandle,
                    instanceFactory,
                    scalarFunction.deterministic(),
                    getDescription(method),
                    scalarFunction.hidden(),
                    method.isAnnotationPresent(Nullable.class),
                    nullableArguments,
                    literalParameters);
        }
        return true;
    }

    private static Optional<MethodHandle> getInstanceFactory(Method method)
            throws IllegalAccessException
    {
        Optional<MethodHandle> instanceFactory = Optional.empty();
        if (!Modifier.isStatic(method.getModifiers())) {
            try {
                instanceFactory = Optional.of(lookup().unreflectConstructor(method.getDeclaringClass().getConstructor()));
            }
            catch (NoSuchMethodException e) {
                throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, format("%s is non-static, but its declaring class is missing a default constructor", method));
            }
        }
        return instanceFactory;
    }

    private static Type type(TypeManager typeManager, SqlType explicitType)
    {
        Type type = typeManager.getType(parseTypeSignature(explicitType.value()));
        requireNonNull(type, format("No type found for '%s'", explicitType.value()));
        return type;
    }

    private static List<TypeSignature> parameterTypeSignatures(Method method, Set<String> literalParameters)
    {
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();

        ImmutableList.Builder<TypeSignature> types = ImmutableList.builder();
        for (int i = 0; i < method.getParameterTypes().length; i++) {
            Class<?> clazz = method.getParameterTypes()[i];
            // skip session parameters
            if (clazz == ConnectorSession.class) {
                continue;
            }

            // find the explicit type annotation if present
            SqlType explicitType = null;
            for (Annotation annotation : parameterAnnotations[i]) {
                if (annotation instanceof SqlType) {
                    explicitType = (SqlType) annotation;
                    break;
                }
            }
            checkArgument(explicitType != null, "Method %s argument %s does not have a @SqlType annotation", method, i);
            types.add(parseTypeSignature(explicitType.value(), literalParameters));
        }
        return types.build();
    }

    private static void verifyMethodSignature(Method method, TypeSignature returnTypeName, List<TypeSignature> argumentTypeNames, TypeManager typeManager)
    {
        // todo figure out how to validate java type for calculated SQL type
        if (!returnTypeName.isCalculated()) {
            Type returnType = typeManager.getType(returnTypeName);
            requireNonNull(returnType, "returnType is null");
            checkArgument(Primitives.unwrap(method.getReturnType()) == returnType.getJavaType(),
                    "Expected method %s return type to be %s (%s)",
                    method,
                    returnType.getJavaType().getName(),
                    returnType);
        }

        // skip Session argument
        Class<?>[] parameterTypes = method.getParameterTypes();
        Annotation[][] annotations = method.getParameterAnnotations();
        if (parameterTypes.length > 0 && parameterTypes[0] == ConnectorSession.class) {
            parameterTypes = Arrays.copyOfRange(parameterTypes, 1, parameterTypes.length);
            annotations = Arrays.copyOfRange(annotations, 1, annotations.length);
        }

        for (int i = 0; i < argumentTypeNames.size(); i++) {
            TypeSignature expectedTypeName = argumentTypeNames.get(i);
            if (expectedTypeName.isCalculated()) {
                continue;
            }
            Type expectedType = typeManager.getType(expectedTypeName);
            Class<?> actualType = parameterTypes[i];
            boolean nullable = Arrays.asList(annotations[i]).stream().anyMatch(Nullable.class::isInstance);
            // Only allow boxing for functions that need to see nulls
            if (Primitives.isWrapperType(actualType)) {
                checkArgument(nullable, "Method %s has parameter with type %s that is missing @Nullable", method, actualType);
            }
            if (nullable) {
                checkArgument(!NON_NULLABLE_ARGUMENT_TYPES.contains(actualType), "Method %s has parameter type %s, but @Nullable is not supported on this type", method, actualType);
            }
            checkArgument(Primitives.unwrap(actualType) == expectedType.getJavaType(),
                    "Expected method %s parameter %s type to be %s (%s)",
                    method,
                    i,
                    expectedType.getJavaType().getName(),
                    expectedType);
        }
    }

    private static List<Boolean> getNullableArguments(Method method)
    {
        List<Boolean> nullableArguments = new ArrayList<>();
        for (Annotation[] annotations : method.getParameterAnnotations()) {
            boolean nullable = false;
            boolean foundSqlType = false;
            for (Annotation annotation : annotations) {
                if (annotation instanceof Nullable) {
                    nullable = true;
                }
                if (annotation instanceof SqlType) {
                    foundSqlType = true;
                }
            }
            // Check that this is a real argument. For example, some functions take ConnectorSession which isn't a SqlType
            if (foundSqlType) {
                nullableArguments.add(nullable);
            }
        }
        return nullableArguments;
    }

    private boolean processScalarOperator(Method method)
            throws IllegalAccessException
    {
        ScalarOperator scalarOperator = method.getAnnotation(ScalarOperator.class);
        if (scalarOperator == null) {
            return false;
        }
        checkValidMethod(method);
        Optional<MethodHandle>  instanceFactory = getInstanceFactory(method);
        MethodHandle methodHandle = lookup().unreflect(method);
        OperatorType operatorType = scalarOperator.value();

        LiteralParameters literalParametersAnnotation = method.getAnnotation(LiteralParameters.class);
        Set<String> literalParameters = ImmutableSet.of();
        if (literalParametersAnnotation != null) {
            literalParameters = ImmutableSet.copyOf(literalParametersAnnotation.value());
        }

        List<TypeSignature> argumentTypes = parameterTypeSignatures(method, literalParameters);
        TypeSignature returnTypeSignature;

        if (operatorType == OperatorType.HASH_CODE) {
            // todo hack for hashCode... should be int
            returnTypeSignature = BIGINT.getTypeSignature();
        }
        else {
            SqlType explicitType = method.getAnnotation(SqlType.class);
            checkArgument(explicitType != null, "Method %s return type does not have a @SqlType annotation", method);
            returnTypeSignature = parseTypeSignature(explicitType.value(), literalParameters);
            verifyMethodSignature(method, returnTypeSignature, argumentTypes, typeManager);
        }

        List<Boolean> nullableArguments = getNullableArguments(method);

        operator(
                operatorType,
                returnTypeSignature,
                argumentTypes,
                methodHandle,
                instanceFactory,
                method.isAnnotationPresent(Nullable.class),
                nullableArguments,
                literalParameters);
        return true;
    }

    private static String getDescription(AnnotatedElement annotatedElement)
    {
        Description description = annotatedElement.getAnnotation(Description.class);
        return (description == null) ? null : description.value();
    }

    private static String camelToSnake(String name)
    {
        return LOWER_CAMEL.to(LOWER_UNDERSCORE, name);
    }

    private static void checkValidMethod(Method method)
    {
        String message = "@ScalarFunction method %s is not valid: ";

        if (method.getAnnotation(Nullable.class) != null) {
            checkArgument(!method.getReturnType().isPrimitive(), message + "annotated with @Nullable but has primitive return type", method);
        }
        else {
            checkArgument(!Primitives.isWrapperType(method.getReturnType()), "not annotated with @Nullable but has boxed primitive return type", method);
        }
    }

    public List<SqlFunction> getFunctions()
    {
        return ImmutableList.copyOf(functions);
    }
}
