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

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.LongVariableConstraint;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TypeVariableConstraint;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionDependency;
import com.facebook.presto.spi.function.IsNull;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.function.TypeParameterSpecialization;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.type.Constraint;
import com.facebook.presto.type.LiteralParameter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Primitives;

import javax.annotation.Nullable;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.metadata.Signature.internalScalarFunction;
import static com.facebook.presto.metadata.Signature.orderableTypeParameter;
import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.metadata.SignatureBinder.applyBoundVariables;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static com.facebook.presto.spi.function.OperatorType.BETWEEN;
import static com.facebook.presto.spi.function.OperatorType.CAST;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodHandles.permuteArguments;
import static java.lang.reflect.Modifier.isStatic;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class ScalarImplementation
{
    private final Signature signature;
    private final boolean nullable;
    private final List<Boolean> nullableArguments;
    private final List<Boolean> nullFlags;
    private final MethodHandle methodHandle;
    private final List<ImplementationDependency> dependencies;
    private final Optional<MethodHandle> constructor;
    private final List<ImplementationDependency> constructorDependencies;
    private final List<Class<?>> argumentNativeContainerTypes;
    private final Map<String, Class<?>> specializedTypeParameters;

    public ScalarImplementation(
            Signature signature,
            boolean nullable,
            List<Boolean> nullableArguments,
            List<Boolean> nullFlags,
            MethodHandle methodHandle,
            List<ImplementationDependency> dependencies,
            Optional<MethodHandle> constructor,
            List<ImplementationDependency> constructorDependencies,
            List<Class<?>> argumentNativeContainerTypes,
            Map<String, Class<?>> specializedTypeParameters)
    {
        this.signature = requireNonNull(signature, "signature is null");
        this.nullable = nullable;
        this.nullableArguments = ImmutableList.copyOf(requireNonNull(nullableArguments, "nullableArguments is null"));
        this.nullFlags = ImmutableList.copyOf(requireNonNull(nullFlags, "nullFlags is null"));
        this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");
        this.dependencies = ImmutableList.copyOf(requireNonNull(dependencies, "dependencies is null"));
        this.constructor = requireNonNull(constructor, "constructor is null");
        this.constructorDependencies = ImmutableList.copyOf(requireNonNull(constructorDependencies, "constructorDependencies is null"));
        this.argumentNativeContainerTypes = ImmutableList.copyOf(requireNonNull(argumentNativeContainerTypes, "argumentNativeContainerTypes is null"));
        this.specializedTypeParameters = ImmutableMap.copyOf(requireNonNull(specializedTypeParameters, "specializedTypeParameters is null"));
    }

    public Optional<MethodHandleAndConstructor> specialize(Signature boundSignature, BoundVariables boundVariables, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        for (Map.Entry<String, Class<?>> entry : specializedTypeParameters.entrySet()) {
            if (!entry.getValue().isAssignableFrom(boundVariables.getTypeVariable(entry.getKey()).getJavaType())) {
                return Optional.empty();
            }
        }
        Class<?> returnContainerType = getNullAwareReturnType(typeManager.getType(boundSignature.getReturnType()).getJavaType(), nullable);
        if (!returnContainerType.equals(methodHandle.type().returnType())) {
            return Optional.empty();
        }
        for (int i = 0; i < boundSignature.getArgumentTypes().size(); i++) {
            Class<?> argumentType = typeManager.getType(boundSignature.getArgumentTypes().get(i)).getJavaType();
            boolean nullableParameter = isParameterNullable(argumentType, nullableArguments.get(i), nullFlags.get(i));
            Class<?> argumentContainerType = getNullAwareContainerType(argumentType, nullableParameter);
            if (!argumentNativeContainerTypes.get(i).isAssignableFrom(argumentContainerType)) {
                return Optional.empty();
            }
        }
        MethodHandle methodHandle = this.methodHandle;
        for (ImplementationDependency dependency : dependencies) {
            methodHandle = MethodHandles.insertArguments(methodHandle, 0, dependency.resolve(boundVariables, typeManager, functionRegistry));
        }
        MethodHandle constructor = null;
        if (this.constructor.isPresent()) {
            constructor = this.constructor.get();
            for (ImplementationDependency dependency : constructorDependencies) {
                constructor = MethodHandles.insertArguments(constructor, 0, dependency.resolve(boundVariables, typeManager, functionRegistry));
            }
        }
        return Optional.of(new MethodHandleAndConstructor(methodHandle, Optional.ofNullable(constructor)));
    }

    private static Class<?> getNullAwareReturnType(Class<?> clazz, boolean nullable)
    {
        if (nullable) {
            return Primitives.wrap(clazz);
        }
        return clazz;
    }

    private static Class<?> getNullAwareContainerType(Class<?> clazz, boolean nullable)
    {
        if (clazz == void.class) {
            return Primitives.wrap(clazz);
        }
        if (nullable) {
            return Primitives.wrap(clazz);
        }
        return clazz;
    }

    private static boolean isParameterNullable(Class<?> type, boolean nullableArgument, boolean nullFlag)
    {
        if (!nullableArgument) {
            return false;
        }
        // void must be nullable even if the null flag is present
        if (type == void.class) {
            return true;
        }
        if (nullFlag) {
            return !type.isPrimitive();
        }
        return true;
    }

    public boolean hasSpecializedTypeParameters()
    {
        return !specializedTypeParameters.isEmpty();
    }

    public Signature getSignature()
    {
        return signature;
    }

    public boolean isNullable()
    {
        return nullable;
    }

    public List<Boolean> getNullableArguments()
    {
        return nullableArguments;
    }

    public List<Boolean> getNullFlags()
    {
        return nullFlags;
    }

    public MethodHandle getMethodHandle()
    {
        return methodHandle;
    }

    public List<ImplementationDependency> getDependencies()
    {
        return dependencies;
    }

    public static final class MethodHandleAndConstructor
    {
        private final MethodHandle methodHandle;
        private final Optional<MethodHandle> constructor;

        public MethodHandleAndConstructor(MethodHandle methodHandle, Optional<MethodHandle> constructor)
        {
            this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");
            this.constructor = requireNonNull(constructor, "constructor is null");
        }

        public MethodHandle getMethodHandle()
        {
            return methodHandle;
        }

        public Optional<MethodHandle> getConstructor()
        {
            return constructor;
        }
    }

    private interface ImplementationDependency
    {
        Object resolve(BoundVariables boundVariables, TypeManager typeManager, FunctionRegistry functionRegistry);
    }

    private static final class FunctionImplementationDependency
            extends ScalarImplementationDependency
    {
        private FunctionImplementationDependency(String name, TypeSignature returnType, List<TypeSignature> argumentTypes)
        {
            super(internalScalarFunction(name, returnType, argumentTypes));
        }
    }

    private static final class OperatorImplementationDependency
            extends ScalarImplementationDependency
    {
        private final OperatorType operator;

        private OperatorImplementationDependency(OperatorType operator, TypeSignature returnType, List<TypeSignature> argumentTypes)
        {
            super(internalOperator(operator, returnType, argumentTypes));
            this.operator = requireNonNull(operator, "operator is null");
        }

        public OperatorType getOperator()
        {
            return operator;
        }
    }

    private abstract static class ScalarImplementationDependency
            implements ImplementationDependency
    {
        private final Signature signature;

        private ScalarImplementationDependency(Signature signature)
        {
            this.signature = requireNonNull(signature, "signature is null");
        }

        public Signature getSignature()
        {
            return signature;
        }

        @Override
        public MethodHandle resolve(BoundVariables boundVariables, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            Signature signature = applyBoundVariables(this.signature, boundVariables, this.signature.getArgumentTypes().size());
            return functionRegistry.getScalarFunctionImplementation(signature).getMethodHandle();
        }
    }

    private static final class TypeImplementationDependency
            implements ImplementationDependency
    {
        private final TypeSignature signature;

        private TypeImplementationDependency(String signature)
        {
            this.signature = parseTypeSignature(requireNonNull(signature, "signature is null"));
        }

        @Override
        public Type resolve(BoundVariables boundVariables, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            return typeManager.getType(applyBoundVariables(signature, boundVariables));
        }
    }

    private static final class LiteralImplementationDependency
            implements ImplementationDependency
    {
        private final String literalName;

        private LiteralImplementationDependency(String literalName)
        {
            this.literalName = requireNonNull(literalName, "literalName is null");
        }

        @Override
        public Long resolve(BoundVariables boundVariables, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            return boundVariables.getLongVariable(literalName);
        }
    }

    public static final class Parser
    {
        private static final Set<OperatorType> COMPARABLE_TYPE_OPERATORS = ImmutableSet.of(EQUAL, NOT_EQUAL, HASH_CODE);
        private static final Set<OperatorType> ORDERABLE_TYPE_OPERATORS = ImmutableSet.of(LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, BETWEEN);

        private final String functionName;
        private final boolean nullable;
        private final List<Boolean> nullableArguments = new ArrayList<>();
        private final List<Boolean> nullFlags = new ArrayList<>();
        private final TypeSignature returnType;
        private final List<TypeSignature> argumentTypes = new ArrayList<>();
        private final List<Class<?>> argumentNativeContainerTypes = new ArrayList<>();
        private final MethodHandle methodHandle;
        private final List<ImplementationDependency> dependencies = new ArrayList<>();
        private final LinkedHashSet<TypeParameter> typeParameters = new LinkedHashSet<>();
        private final Set<String> literalParameters = new HashSet<>();
        private final Map<String, Class<?>> specializedTypeParameters;
        private final Optional<MethodHandle> constructorMethodHandle;
        private final List<ImplementationDependency> constructorDependencies = new ArrayList<>();
        private final List<LongVariableConstraint> longVariableConstraints = new ArrayList<>();

        private Parser(String functionName, Method method, Map<Set<TypeParameter>, Constructor<?>> constructors)
        {
            this.functionName = requireNonNull(functionName, "functionName is null");
            this.nullable = method.getAnnotation(SqlNullable.class) != null;
            checkArgument(nullable || !containsLegacyNullable(method.getAnnotations()), "Method [%s] is annotated with @Nullable but not @SqlNullable", method);

            Stream.of(method.getAnnotationsByType(TypeParameter.class))
                    .forEach(typeParameters::add);

            LiteralParameters literalParametersAnnotation = method.getAnnotation(LiteralParameters.class);
            if (literalParametersAnnotation != null) {
                literalParameters.addAll(asList(literalParametersAnnotation.value()));
            }

            SqlType returnType = method.getAnnotation(SqlType.class);
            checkArgument(returnType != null, format("Method [%s] is missing @SqlType annotation", method));
            this.returnType = parseTypeSignature(returnType.value(), literalParameters);

            Class<?> actualReturnType = method.getReturnType();
            if (Primitives.isWrapperType(actualReturnType)) {
                checkArgument(nullable, "Method [%s] has wrapper return type %s but is missing @SqlNullable", method, actualReturnType.getSimpleName());
            }
            else if (actualReturnType.isPrimitive()) {
                checkArgument(!nullable, "Method [%s] annotated with @SqlNullable has primitive return type %s", method, actualReturnType.getSimpleName());
            }

            Stream.of(method.getAnnotationsByType(Constraint.class))
                    .map(annotation -> new LongVariableConstraint(annotation.variable(), annotation.expression()))
                    .forEach(longVariableConstraints::add);

            this.specializedTypeParameters = getDeclaredSpecializedTypeParameters(method);

            parseArguments(method);

            this.constructorMethodHandle = getConstructor(method, constructors);

            this.methodHandle = getMethodHandle(method);
        }

        private void parseArguments(Method method)
        {
            ImmutableSet<String> typeParameterNames = typeParameters.stream()
                    .map(TypeParameter::value)
                    .collect(toImmutableSet());
            for (int i = 0; i < method.getParameterCount(); i++) {
                Annotation[] annotations = method.getParameterAnnotations()[i];
                Class<?> parameterType = method.getParameterTypes()[i];
                // Skip injected parameters
                if (parameterType == ConnectorSession.class) {
                    continue;
                }
                if (containsMetaParameter(annotations)) {
                    checkArgument(annotations.length == 1, "Meta parameters may only have a single annotation [%s]", method);
                    checkArgument(argumentTypes.isEmpty(), "Meta parameter must come before parameters [%s]", method);
                    Annotation annotation = annotations[0];
                    if (annotation instanceof TypeParameter) {
                        checkArgument(typeParameters.contains(annotation), "Injected type parameters must be declared with @TypeParameter annotation on the method [%s]", method);
                    }
                    if (annotation instanceof LiteralParameter) {
                        checkArgument(literalParameters.contains(((LiteralParameter) annotation).value()), "Parameter injected by @LiteralParameter must be declared with @LiteralParameters on the method [%s]", method);
                    }
                    dependencies.add(parseDependency(annotation));
                }
                else {
                    checkArgument(!Stream.of(annotations).anyMatch(IsNull.class::isInstance), "Method [%s] has @IsNull parameter that does not follow a @SqlType parameter", method);

                    SqlType type = Stream.of(annotations)
                            .filter(SqlType.class::isInstance)
                            .map(SqlType.class::cast)
                            .findFirst()
                            .orElseThrow(() -> new IllegalArgumentException(format("Method [%s] is missing @SqlType annotation for parameter", method)));
                    boolean nullableArgument = Stream.of(annotations).anyMatch(SqlNullable.class::isInstance);
                    checkArgument(nullableArgument || !containsLegacyNullable(annotations), "Method [%s] has parameter annotated with @Nullable but not @SqlNullable", method);

                    boolean hasNullFlag = false;
                    if (method.getParameterCount() > (i + 1)) {
                        Annotation[] parameterAnnotations = method.getParameterAnnotations()[i + 1];
                        if (Stream.of(parameterAnnotations).anyMatch(IsNull.class::isInstance)) {
                            Class<?> isNullType = method.getParameterTypes()[i + 1];

                            checkArgument(Stream.of(parameterAnnotations).filter(Parser::isPrestoAnnotation).allMatch(IsNull.class::isInstance), "Method [%s] has @IsNull parameter that has other annotations", method);
                            checkArgument(isNullType == boolean.class, "Method [%s] has non-boolean parameter with @IsNull", method);
                            checkArgument((parameterType == Void.class) || !Primitives.isWrapperType(parameterType), "Method [%s] uses @IsNull following a parameter with boxed primitive type: %s", method, parameterType.getSimpleName());

                            nullableArgument = true;
                            hasNullFlag = true;
                        }
                    }

                    if (Primitives.isWrapperType(parameterType)) {
                        checkArgument(nullableArgument, "Method [%s] has parameter with wrapper type %s that is missing @SqlNullable", method, parameterType.getSimpleName());
                    }
                    else if (parameterType.isPrimitive() && !hasNullFlag) {
                        checkArgument(!nullableArgument, "Method [%s] has parameter with primitive type %s annotated with @SqlNullable", method, parameterType.getSimpleName());
                    }

                    if (typeParameterNames.contains(type.value()) && !(parameterType == Object.class && nullableArgument)) {
                        // Infer specialization on this type parameter. We don't do this for @SqlNullable Object because it could match a type like BIGINT
                        Class<?> specialization = specializedTypeParameters.get(type.value());
                        Class<?> nativeParameterType = Primitives.unwrap(parameterType);
                        checkArgument(specialization == null || specialization.equals(nativeParameterType), "Method [%s] type %s has conflicting specializations %s and %s", method, type.value(), specialization, nativeParameterType);
                        specializedTypeParameters.put(type.value(), nativeParameterType);
                    }
                    argumentNativeContainerTypes.add(parameterType);
                    argumentTypes.add(parseTypeSignature(type.value(), literalParameters));

                    if (hasNullFlag) {
                        // skip @IsNull parameter
                        i++;
                    }
                    nullableArguments.add(nullableArgument);
                    nullFlags.add(hasNullFlag);
                }
            }
        }

        // Find matching constructor, if this is an instance method, and populate constructorDependencies
        private Optional<MethodHandle> getConstructor(Method method, Map<Set<TypeParameter>, Constructor<?>> constructors)
        {
            if (isStatic(method.getModifiers())) {
                return Optional.empty();
            }

            Constructor<?> constructor = constructors.get(typeParameters);
            checkArgument(constructor != null, "Method [%s] is an instance method and requires a public constructor to be declared with %s type parameters", method, typeParameters);
            for (int i = 0; i < constructor.getParameterCount(); i++) {
                Annotation[] annotations = constructor.getParameterAnnotations()[i];
                checkArgument(containsMetaParameter(annotations), "Constructors may only have meta parameters [%s]", constructor);
                checkArgument(annotations.length == 1, "Meta parameters may only have a single annotation [%s]", constructor);
                Annotation annotation = annotations[0];
                if (annotation instanceof TypeParameter) {
                    checkArgument(typeParameters.contains(annotation), "Injected type parameters must be declared with @TypeParameter annotation on the constructor [%s]", constructor);
                }
                constructorDependencies.add(parseDependency(annotation));
            }
            try {
                return Optional.of(lookup().unreflectConstructor(constructor));
            }
            catch (IllegalAccessException e) {
                throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, e);
            }
        }

        private Map<String, Class<?>> getDeclaredSpecializedTypeParameters(Method method)
        {
            Map<String, Class<?>> specializedTypeParameters = new HashMap<>();
            TypeParameterSpecialization[] typeParameterSpecializations = method.getAnnotationsByType(TypeParameterSpecialization.class);
            ImmutableSet<String> typeParameterNames = typeParameters.stream()
                    .map(TypeParameter::value)
                    .collect(toImmutableSet());
            for (TypeParameterSpecialization specialization : typeParameterSpecializations) {
                checkArgument(typeParameterNames.contains(specialization.name()), "%s does not match any declared type parameters (%s) [%s]", specialization.name(), typeParameters, method);
                Class<?> existingSpecialization = specializedTypeParameters.get(specialization.name());
                checkArgument(existingSpecialization == null || existingSpecialization.equals(specialization.nativeContainerType()),
                        "%s has conflicting specializations %s and %s [%s]", specialization.name(), existingSpecialization, specialization.nativeContainerType(), method);
                specializedTypeParameters.put(specialization.name(), specialization.nativeContainerType());
            }
            return specializedTypeParameters;
        }

        private MethodHandle getMethodHandle(Method method)
        {
            MethodHandle methodHandle;
            try {
                methodHandle = lookup().unreflect(method);
            }
            catch (IllegalAccessException e) {
                throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, e);
            }
            if (!isStatic(method.getModifiers())) {
                // Re-arrange the parameters, so that the "this" parameter is after the meta parameters
                int[] permutedIndices = new int[methodHandle.type().parameterCount()];
                permutedIndices[0] = dependencies.size();
                MethodType newType = methodHandle.type().changeParameterType(dependencies.size(), methodHandle.type().parameterType(0));
                for (int i = 0; i < dependencies.size(); i++) {
                    permutedIndices[i + 1] = i;
                    newType = newType.changeParameterType(i, methodHandle.type().parameterType(i + 1));
                }
                for (int i = dependencies.size() + 1; i < permutedIndices.length; i++) {
                    permutedIndices[i] = i;
                }
                methodHandle = permuteArguments(methodHandle, newType, permutedIndices);
            }
            return methodHandle;
        }

        private static List<TypeVariableConstraint> createTypeVariableConstraints(Iterable<TypeParameter> typeParameters, List<ImplementationDependency> dependencies)
        {
            Set<String> orderableRequired = new HashSet<>();
            Set<String> comparableRequired = new HashSet<>();
            for (ImplementationDependency dependency : dependencies) {
                if (dependency instanceof OperatorImplementationDependency) {
                    OperatorType operator = ((OperatorImplementationDependency) dependency).getOperator();
                    if (operator == CAST) {
                        continue;
                    }
                    Set<String> argumentTypes = ((OperatorImplementationDependency) dependency).getSignature().getArgumentTypes().stream()
                            .map(TypeSignature::getBase)
                            .collect(toImmutableSet());
                    checkArgument(argumentTypes.size() == 1, "Operator dependency must only have arguments of a single type");
                    String argumentType = Iterables.getOnlyElement(argumentTypes);
                    if (COMPARABLE_TYPE_OPERATORS.contains(operator)) {
                        comparableRequired.add(argumentType);
                    }
                    if (ORDERABLE_TYPE_OPERATORS.contains(operator)) {
                        orderableRequired.add(argumentType);
                    }
                }
            }
            ImmutableList.Builder<TypeVariableConstraint> typeVariableConstraints = ImmutableList.builder();
            for (TypeParameter typeParameter : typeParameters) {
                String name = typeParameter.value();
                if (orderableRequired.contains(name)) {
                    typeVariableConstraints.add(orderableTypeParameter(name));
                }
                else if (comparableRequired.contains(name)) {
                    typeVariableConstraints.add(comparableTypeParameter(name));
                }
                else {
                    typeVariableConstraints.add(typeVariable(name));
                }
            }
            return typeVariableConstraints.build();
        }

        private ImplementationDependency parseDependency(Annotation annotation)
        {
            if (annotation instanceof TypeParameter) {
                return new TypeImplementationDependency(((TypeParameter) annotation).value());
            }
            if (annotation instanceof LiteralParameter) {
                return new LiteralImplementationDependency(((LiteralParameter) annotation).value());
            }
            if (annotation instanceof FunctionDependency) {
                FunctionDependency function = (FunctionDependency) annotation;
                return new FunctionImplementationDependency(
                        function.name(),
                        parseTypeSignature(function.returnType(), literalParameters),
                        Arrays.stream(function.argumentTypes())
                                .map(signature -> parseTypeSignature(signature, literalParameters))
                                .collect(toImmutableList()));
            }
            if (annotation instanceof OperatorDependency) {
                OperatorDependency operator = (OperatorDependency) annotation;
                return new OperatorImplementationDependency(
                        operator.operator(),
                        parseTypeSignature(operator.returnType(), literalParameters),
                        Arrays.stream(operator.argumentTypes())
                                .map(signature -> parseTypeSignature(signature, literalParameters))
                                .collect(toImmutableList()));
            }
            throw new IllegalArgumentException("Unsupported annotation " + annotation.getClass().getSimpleName());
        }

        private static boolean containsMetaParameter(Annotation[] annotations)
        {
            for (Annotation annotation : annotations) {
                if (isMetaParameter(annotation)) {
                    return true;
                }
            }
            return false;
        }

        private static boolean isMetaParameter(Annotation annotation)
        {
            return annotation instanceof TypeParameter ||
                    annotation instanceof LiteralParameter ||
                    annotation instanceof FunctionDependency ||
                    annotation instanceof OperatorDependency;
        }

        public ScalarImplementation get()
        {
            Signature signature = new Signature(
                    functionName,
                    SCALAR,
                    createTypeVariableConstraints(typeParameters, dependencies),
                    longVariableConstraints,
                    returnType,
                    argumentTypes,
                    false);
            return new ScalarImplementation(
                    signature,
                    nullable,
                    nullableArguments,
                    nullFlags,
                    methodHandle,
                    dependencies,
                    constructorMethodHandle,
                    constructorDependencies,
                    argumentNativeContainerTypes,
                    specializedTypeParameters);
        }

        public static ScalarImplementation parseImplementation(String functionName, Method method, Map<Set<TypeParameter>, Constructor<?>> constructors)
        {
            return new Parser(functionName, method, constructors).get();
        }

        private static boolean containsLegacyNullable(Annotation[] annotations)
        {
            return Arrays.stream(annotations)
                    .map(Annotation::annotationType)
                    .map(Class::getName)
                    .anyMatch(name -> name.equals(Nullable.class.getName()));
        }

        private static boolean isPrestoAnnotation(Annotation annotation)
        {
            return isMetaParameter(annotation) ||
                    annotation instanceof SqlType ||
                    annotation instanceof SqlNullable ||
                    annotation instanceof IsNull;
        }
    }
}
