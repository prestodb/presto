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
import com.facebook.presto.operator.scalar.Literal;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.operator.scalar.ScalarOperator;
import com.facebook.presto.operator.window.ReflectionWindowFunctionSupplier;
import com.facebook.presto.operator.window.WindowFunction;
import com.facebook.presto.operator.window.WindowFunctionSupplier;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.type.SqlType;
import com.facebook.presto.util.ImmutableCollectors;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.primitives.Primitives;
import io.airlift.joni.Regex;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.metadata.FunctionRegistry.operatorInfo;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Locale.ENGLISH;

public class FunctionListBuilder
{
    private static final Set<Class<?>> NULLABLE_ARGUMENT_TYPES = ImmutableSet.<Class<?>>of(Boolean.class, Long.class, Double.class, Slice.class);

    private static final Set<Class<?>> SUPPORTED_TYPES = ImmutableSet.of(
            long.class,
            Long.class,
            double.class,
            Double.class,
            Slice.class,
            boolean.class,
            Boolean.class,
            Regex.class,
            JsonPath.class);

    private static final Set<Class<?>> SUPPORTED_RETURN_TYPES = ImmutableSet.of(
            long.class,
            double.class,
            Slice.class,
            boolean.class,
            int.class,
            Regex.class,
            JsonPath.class);

    private final List<ParametricFunction> functions = new ArrayList<>();
    private final TypeManager typeManager;

    public FunctionListBuilder(TypeManager typeManager)
    {
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
    }

    public FunctionListBuilder window(String name, Type returnType, List<? extends Type> argumentTypes, Class<? extends WindowFunction> functionClass)
    {
        WindowFunctionSupplier windowFunctionSupplier = new ReflectionWindowFunctionSupplier<>(
                new Signature(name, returnType.getTypeSignature(), Lists.transform(ImmutableList.copyOf(argumentTypes), Type::getTypeSignature)),
                functionClass);

        functions.add(new FunctionInfo(windowFunctionSupplier.getSignature(), windowFunctionSupplier.getDescription(), windowFunctionSupplier));
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
        Signature signature = new Signature(name, function.getFinalType().getTypeSignature(), Lists.transform(ImmutableList.copyOf(function.getParameterTypes()), Type::getTypeSignature));
        functions.add(new FunctionInfo(signature, description, function));
        return this;
    }

    public FunctionListBuilder aggregate(Class<?> aggregationDefinition)
    {
        functions.addAll(GenericAggregationFunctionFactory.fromAggregationDefinition(aggregationDefinition, typeManager).listFunctions());
        return this;
    }

    public FunctionListBuilder scalar(Signature signature, MethodHandle function, boolean deterministic, String description, boolean hidden, boolean nullable, List<Boolean> nullableArguments)
    {
        functions.add(new FunctionInfo(signature, description, hidden, function, deterministic, nullable, nullableArguments));
        return this;
    }

    private FunctionListBuilder operator(OperatorType operatorType, Type returnType, List<Type> parameterTypes, List<LiteralMapping> literalMappings, MethodHandle function, boolean nullable, List<Boolean> nullableArguments)
    {
        FunctionInfo operatorInfo = operatorInfo(operatorType, returnType.getTypeSignature(), Lists.transform(parameterTypes, Type::getTypeSignature), literalMappings, function, nullable, nullableArguments);
        functions.add(operatorInfo);
        return this;
    }

    public FunctionListBuilder scalar(Class<?> clazz)
    {
        try {
            boolean foundOne = false;
            for (Method method : clazz.getMethods()) {
                foundOne = processScalarFunction(method) || foundOne;
                foundOne = processScalarOperator(method) || foundOne;
            }
            checkArgument(foundOne, "Expected class %s to contain at least one method annotated with @%s", clazz.getName(), ScalarFunction.class.getSimpleName());
        }
        catch (IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
        return this;
    }

    public FunctionListBuilder functions(ParametricFunction... parametricFunctions)
    {
        for (ParametricFunction parametricFunction : parametricFunctions) {
            function(parametricFunction);
        }
        return this;
    }

    public FunctionListBuilder function(ParametricFunction parametricFunction)
    {
        checkNotNull(parametricFunction, "parametricFunction is null");
        functions.add(parametricFunction);
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
        MethodHandle methodHandle = lookup().unreflect(method);
        String name = scalarFunction.value();
        if (name.isEmpty()) {
            name = camelToSnake(method.getName());
        }
        SqlType returnTypeAnnotation = method.getAnnotation(SqlType.class);
        checkArgument(returnTypeAnnotation != null, "Method %s return type does not have a @SqlType annotation", method);
        Signature signature = new Signature(name.toLowerCase(ENGLISH), parseTypeSignature(returnTypeAnnotation.value()), parameterTypeSignatures(method), parameterLiteralMappings(method));

        verifyMethodSignature(method, signature.getReturnType(), signature.getArgumentTypes(), signature.getLiteralMappings(), typeManager);

        List<Boolean> nullableArguments = getNullableArguments(method);

        scalar(signature, methodHandle, scalarFunction.deterministic(), getDescription(method), scalarFunction.hidden(), method.isAnnotationPresent(Nullable.class), nullableArguments);
        for (String alias : scalarFunction.alias()) {
            scalar(signature.withAlias(alias.toLowerCase(ENGLISH)), methodHandle, scalarFunction.deterministic(), getDescription(method), scalarFunction.hidden(), method.isAnnotationPresent(Nullable.class), nullableArguments);
        }
        return true;
    }

    private static Type type(TypeManager typeManager, SqlType explicitType)
    {
        Type type = typeManager.getType(parseTypeSignature(explicitType.value()));
        checkNotNull(type, "No type found for '%s'", explicitType.value());
        return type;
    }

    private static List<LiteralMapping> parameterLiteralMappings(Method method)
    {
        ImmutableList.Builder<LiteralMapping> literalMappings = ImmutableList.builder();
        parameterAnnotations(method, ImmutableList.builder(), literalMappings);
        return literalMappings.build();
    }

    private static List<Type> parameterTypes(TypeManager typeManager, Method method)
    {
        return parameterSqlTypeAnnotations(method).stream()
                .map(sqlTypeAnnotation -> type(typeManager, sqlTypeAnnotation))
                .collect(ImmutableCollectors.toImmutableList());
    }

    private static List<TypeSignature> parameterTypeSignatures(Method method)
    {
        return parameterSqlTypeAnnotations(method).stream()
                .map(sqlTypeAnnotation -> parseTypeSignature(sqlTypeAnnotation.value()))
                .collect(ImmutableCollectors.toImmutableList());
    }

    private static List<SqlType> parameterSqlTypeAnnotations(Method method)
    {
        ImmutableList.Builder<SqlType> sqlTypeAnnotations = ImmutableList.builder();
        parameterAnnotations(method, sqlTypeAnnotations, ImmutableList.builder());
        return sqlTypeAnnotations.build();
    }

    private static void parameterAnnotations(Method method,
            ImmutableList.Builder<SqlType> sqlTypeAnnotations,
            ImmutableList.Builder<LiteralMapping> literalMappings)
    {
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();

        for (int i = 0; i < method.getParameterTypes().length; i++) {
            Class<?> clazz = method.getParameterTypes()[i];
            // skip session parameters
            if (clazz == ConnectorSession.class) {
                continue;
            }

            List<SqlType> parameterSqlTypeAnnotations = filterElementsOfType(parameterAnnotations[i], SqlType.class);
            List<Literal> parameterLiteralAnnotations = filterElementsOfType(parameterAnnotations[i], Literal.class);

            checkArgument(parameterSqlTypeAnnotations.isEmpty() ^ parameterLiteralAnnotations.isEmpty(),
                    "Method %s argument %s must have either @SqlType or @Literal annotation", method, i);
            checkArgument(parameterSqlTypeAnnotations.size() <= 1,
                    "Method %s argument %s must contain at most one @SqlType annotation", method, i);
            checkArgument(parameterLiteralAnnotations.size() <= 1,
                    "Method %s argument %s must contain at most one @Literal annotation", method, i);

            if (!parameterSqlTypeAnnotations.isEmpty()) {
                sqlTypeAnnotations.add(getOnlyElement(parameterSqlTypeAnnotations));
            }
            else {
                Literal parameterLiteralAnnotation = getOnlyElement(parameterLiteralAnnotations);
                literalMappings.add(new LiteralMapping(parameterLiteralAnnotation.value(), i));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <T, E> List<T> filterElementsOfType(E[] elements, Class<T> type)
    {
        ImmutableList.Builder<T> result = ImmutableList.builder();

        for (E element : elements) {
            if (type.isInstance(element)) {
                result.add((T) element);
            }
        }

        return result.build();
    }

    private static void verifyMethodSignature(Method method, TypeSignature returnTypeName, List<TypeSignature> argumentTypeNames, List<LiteralMapping> literalMappings, TypeManager typeManager)
    {
        // todo figure out how to validate java type for calculated SQL type
        if (!returnTypeName.isCalculated()) {
            Type returnType = typeManager.getType(returnTypeName);
            checkNotNull(returnType, "returnType is null");
            checkArgument(Primitives.unwrap(method.getReturnType()) == returnType.getJavaType(),
                    "Expected method %s return type to be %s (%s)",
                    method,
                    returnType.getJavaType().getName(),
                    returnType);
        }

        Set<Integer> literalParameterIndexes = literalMappings.stream()
                .map(LiteralMapping::getFunctionParameterIndex)
                .collect(toImmutableSet());

        Class<?>[] parameterTypes = method.getParameterTypes();
        Annotation[][] annotations = method.getParameterAnnotations();

        int argumentIndex = 0;
        for (int parameterIndex = 0; parameterIndex < parameterTypes.length; parameterIndex++) {
            if (shouldSkipParameter(literalParameterIndexes, parameterTypes[parameterIndex], parameterIndex)) {
                continue;
            }

            TypeSignature expectedTypeName = argumentTypeNames.get(argumentIndex++);
            if (expectedTypeName.isCalculated()) {
                continue;
            }
            Type expectedType = typeManager.getType(expectedTypeName);
            Class<?> actualType = parameterTypes[parameterIndex];
            boolean nullable = !FluentIterable.from(Arrays.asList(annotations[parameterIndex])).filter(Nullable.class).isEmpty();
            // Only allow boxing for functions that need to see nulls
            if (Primitives.isWrapperType(actualType)) {
                checkArgument(nullable, "Method %s has parameter with type %s that is missing @Nullable", method, actualType);
            }
            if (nullable) {
                checkArgument(NULLABLE_ARGUMENT_TYPES.contains(actualType), "Method %s has parameter type %s, but @Nullable is not supported on this type", method, actualType);
            }
            checkArgument(Primitives.unwrap(actualType) == expectedType.getJavaType(),
                    "Expected method %s parameter %s type to be %s (%s)",
                    method,
                    parameterIndex,
                    expectedType.getJavaType().getName(),
                    expectedType);
        }
    }

    private static boolean shouldSkipParameter(Set<Integer> literalParameterIndexes, Class<?> parameterType, int parameterIndex)
    {
        // skip Session argument
        if (parameterType == ConnectorSession.class && parameterIndex == 0) {
            return true;
        }

        if (literalParameterIndexes.contains(parameterIndex)) {
            return true;
        }

        return false;
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
        MethodHandle methodHandle = lookup().unreflect(method);
        OperatorType operatorType = scalarOperator.value();

        List<Type> parameterTypes = parameterTypes(typeManager, method);
        List<LiteralMapping> literalMappings = parameterLiteralMappings(method);

        Type returnType;
        if (operatorType == OperatorType.HASH_CODE) {
            // todo hack for hashCode... should be int
            returnType = BIGINT;
        }
        else {
            SqlType explicitType = method.getAnnotation(SqlType.class);
            checkArgument(explicitType != null, "Method %s return type does not have a @SqlType annotation", method);
            returnType = type(typeManager, explicitType);

            verifyMethodSignature(method, returnType.getTypeSignature(), Lists.transform(parameterTypes, Type::getTypeSignature), literalMappings, typeManager);
        }

        List<Boolean> nullableArguments = getNullableArguments(method);

        operator(operatorType, returnType, parameterTypes, literalMappings, methodHandle, method.isAnnotationPresent(Nullable.class), nullableArguments);
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

        checkArgument(Modifier.isStatic(method.getModifiers()), message + "must be static", method);

        checkArgument(SUPPORTED_RETURN_TYPES.contains(Primitives.unwrap(method.getReturnType())), message + "return type not supported", method);
        if (method.getAnnotation(Nullable.class) != null) {
            checkArgument(!method.getReturnType().isPrimitive(), message + "annotated with @Nullable but has primitive return type", method);
        }
        else {
            checkArgument(!Primitives.isWrapperType(method.getReturnType()), "not annotated with @Nullable but has boxed primitive return type", method);
        }

        for (Class<?> type : getParameterTypes(method.getParameterTypes())) {
            checkArgument(SUPPORTED_TYPES.contains(type), message + "parameter type [%s] not supported", method, type.getName());
        }
    }

    private static List<Class<?>> getParameterTypes(Class<?>... types)
    {
        ImmutableList<Class<?>> parameterTypes = ImmutableList.copyOf(types);
        if (!parameterTypes.isEmpty() && parameterTypes.get(0) == ConnectorSession.class) {
            parameterTypes = parameterTypes.subList(1, parameterTypes.size());
        }
        return parameterTypes;
    }

    public List<ParametricFunction> getFunctions()
    {
        return ImmutableList.copyOf(functions);
    }
}
