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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.metadata.TypeParameterRequirement;
import com.facebook.presto.operator.Description;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.type.SqlType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Primitives;

import javax.annotation.Nullable;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
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
import static com.facebook.presto.metadata.FunctionRegistry.bindSignature;
import static com.facebook.presto.metadata.FunctionRegistry.mangleOperatorName;
import static com.facebook.presto.metadata.OperatorType.BETWEEN;
import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.NOT_EQUAL;
import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.metadata.Signature.orderableTypeParameter;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.spi.StandardErrorCode.AMBIGUOUS_FUNCTION_IMPLEMENTATION;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodHandles.permuteArguments;
import static java.lang.reflect.Modifier.isStatic;
import static java.util.Objects.requireNonNull;

public class ReflectionParametricScalar
        extends SqlScalarFunction
{
    private static final Set<OperatorType> COMPARABLE_TYPE_OPERATORS = ImmutableSet.of(EQUAL, NOT_EQUAL, HASH_CODE);
    private static final Set<OperatorType> ORDERABLE_TYPE_OPERATORS = ImmutableSet.of(LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, BETWEEN);

    // These three collections implement something similar to partial template specialization from C++, and allow more optimized implementations to be provided for specific types

    // These are implementations for concrete types (they have no unbound type parameters), and have the highest priority when picking an implementation
    private final Map<Signature, Implementation> exactImplementations;
    // These are implementations for when a type parameter binds to a specific native container type
    private final List<Implementation> specializedImplementations;
    // These are generic implementations
    private final List<Implementation> implementations;
    private final String description;
    private final boolean hidden;
    private final boolean deterministic;

    private ReflectionParametricScalar(
            Signature signature,
            String description,
            boolean hidden,
            Map<Signature, Implementation> exactImplementations,
            List<Implementation> specializedImplementations,
            List<Implementation> implementations,
            boolean deterministic)
    {
        super(signature.getName(),
                signature.getTypeParameterRequirements(),
                signature.getReturnType().toString(),
                signature.getArgumentTypes().stream()
                        .map(TypeSignature::toString)
                        .collect(toImmutableList()));
        this.description = description;
        this.hidden = hidden;
        this.exactImplementations = ImmutableMap.copyOf(requireNonNull(exactImplementations, "exactImplementations is null"));
        this.specializedImplementations = ImmutableList.copyOf(requireNonNull(specializedImplementations, "specializedImplementations is null"));
        this.implementations = ImmutableList.copyOf(requireNonNull(implementations, "implementations is null"));
        this.deterministic = deterministic;
    }

    @Override
    public boolean isHidden()
    {
        return hidden;
    }

    @Override
    public boolean isDeterministic()
    {
        return deterministic;
    }

    @Override
    public String getDescription()
    {
        return description;
    }

    @Override
    public ScalarFunctionImplementation specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Signature boundSignature = bindSignature(getSignature(), types, arity);
        if (exactImplementations.containsKey(boundSignature)) {
            Implementation implementation = exactImplementations.get(boundSignature);
            return new ScalarFunctionImplementation(implementation.isNullable(), implementation.getNullableArguments(), implementation.getMethodHandle(), isDeterministic());
        }

        ScalarFunctionImplementation selectedImplementation = null;
        for (Implementation implementation : specializedImplementations) {
            MethodHandleAndConstructor methodHandle = implementation.specialize(boundSignature, types, typeManager, functionRegistry);
            if (methodHandle != null) {
                checkCondition(selectedImplementation == null, AMBIGUOUS_FUNCTION_IMPLEMENTATION, "Ambiguous implementation for %s with bindings %s", getSignature(), types);
                selectedImplementation = new ScalarFunctionImplementation(implementation.isNullable(), implementation.getNullableArguments(), methodHandle.getMethodHandle(), methodHandle.getConstructor(), isDeterministic());
            }
        }
        if (selectedImplementation != null) {
            return selectedImplementation;
        }

        for (Implementation implementation : implementations) {
            MethodHandleAndConstructor methodHandle = implementation.specialize(boundSignature, types, typeManager, functionRegistry);
            if (methodHandle != null) {
                checkCondition(selectedImplementation == null, AMBIGUOUS_FUNCTION_IMPLEMENTATION, "Ambiguous implementation for %s with bindings %s", getSignature(), types);
                selectedImplementation = new ScalarFunctionImplementation(implementation.isNullable(), implementation.getNullableArguments(), methodHandle.getMethodHandle(), methodHandle.getConstructor(), isDeterministic());
            }
        }
        if (selectedImplementation != null) {
            return selectedImplementation;
        }

        throw new PrestoException(FUNCTION_IMPLEMENTATION_MISSING, format("Unsupported type parameters (%s) for %s", types, getSignature()));
    }

    public static SqlScalarFunction parseDefinition(Class<?> clazz)
    {
        ScalarFunction scalarAnnotation = clazz.getAnnotation(ScalarFunction.class);
        ScalarOperator operatorAnnotation = clazz.getAnnotation(ScalarOperator.class);
        Description descriptionAnnotation = clazz.getAnnotation(Description.class);
        checkArgument(scalarAnnotation != null || operatorAnnotation != null, "Missing parametric annotation");
        checkArgument(scalarAnnotation == null || operatorAnnotation == null, "%s annotated as both an operator and a function", clazz.getSimpleName());
        if (scalarAnnotation != null) {
            requireNonNull(descriptionAnnotation, format("%s missing @Description annotation", clazz.getSimpleName()));
        }
        String name;
        String description = descriptionAnnotation == null ? "" : descriptionAnnotation.value();
        boolean hidden;
        boolean deterministic;

        if (scalarAnnotation != null) {
            name = scalarAnnotation.value();
            hidden = scalarAnnotation.hidden();
            deterministic = scalarAnnotation.deterministic();
        }
        else {
            name = mangleOperatorName(operatorAnnotation.value());
            hidden = true;
            deterministic = true;
        }

        ImmutableMap.Builder<Signature, Implementation> exactImplementations = ImmutableMap.builder();
        ImmutableList.Builder<Implementation> specializedImplementations = ImmutableList.builder();
        ImmutableList.Builder<Implementation> implementations = ImmutableList.builder();
        Signature signature = null;

        Map<Set<TypeParameter>, Constructor<?>> constructors = findConstructors(clazz);

        for (Method method : findPublicMethodsWithAnnotation(clazz, SqlType.class)) {
            Implementation implementation = new ImplementationParser(name, method, constructors).get();
            if (implementation.getSignature().getTypeParameterRequirements().isEmpty()) {
                exactImplementations.put(implementation.getSignature(), implementation);
                continue;
            }
            if (signature == null) {
                signature = implementation.getSignature();
            }
            else {
                checkArgument(implementation.getSignature().equals(signature), "Implementations with type parameters must all have matching signatures. %s does not match %s", implementation.getSignature(), signature);
            }
            if (implementation.hasSpecializedTypeParameters()) {
                specializedImplementations.add(implementation);
            }
            else {
                implementations.add(implementation);
            }
        }

        requireNonNull(signature, format("No implementations found for %s", name));

        return new ReflectionParametricScalar(
                signature,
                description,
                hidden,
                exactImplementations.build(),
                specializedImplementations.build(),
                implementations.build(),
                deterministic);
    }

    private static List<Method> findPublicMethodsWithAnnotation(Class<?> clazz, Class<?> annotationClass)
    {
        ImmutableList.Builder<Method> methods = ImmutableList.builder();
        for (Method method : clazz.getMethods()) {
            for (Annotation annotation : method.getAnnotations()) {
                if (annotationClass.isInstance(annotation)) {
                    checkArgument(Modifier.isPublic(method.getModifiers()), "%s annotated with %s must be public", method.getName(), annotationClass.getSimpleName());
                    methods.add(method);
                }
            }
        }
        return methods.build();
    }

    private static Map<Set<TypeParameter>, Constructor<?>> findConstructors(Class<?> clazz)
    {
        ImmutableMap.Builder<Set<TypeParameter>, Constructor<?>> builder = ImmutableMap.builder();
        for (Constructor<?> constructor : clazz.getConstructors()) {
            Set<TypeParameter> typeParameters = new HashSet<>();
            Stream.of(constructor.getAnnotationsByType(TypeParameter.class))
                    .forEach(typeParameters::add);
            builder.put(typeParameters, constructor);
        }
        return builder.build();
    }

    private static final class ImplementationParser
    {
        private final String functionName;
        private final boolean nullable;
        private final List<Boolean> nullableArguments = new ArrayList<>();
        private final String returnType;
        private final List<String> argumentTypes = new ArrayList<>();
        private final List<Class<?>> argumentNativeContainerTypes = new ArrayList<>();
        private final MethodHandle methodHandle;
        private final List<ImplementationDependency> dependencies = new ArrayList<>();
        private final LinkedHashSet<TypeParameter> typeParameters = new LinkedHashSet<>();
        private final Map<String, Class<?>> specializedTypeParameters;
        private final Optional<MethodHandle> constructorMethodHandle;
        private final List<ImplementationDependency> constructorDependencies = new ArrayList<>();

        public ImplementationParser(String functionName, Method method, Map<Set<TypeParameter>, Constructor<?>> constructors)
        {
            this.functionName = requireNonNull(functionName, "functionName is null");
            this.nullable = method.getAnnotation(Nullable.class) != null;

            SqlType returnType = method.getAnnotation(SqlType.class);
            requireNonNull(returnType, format("%s is missing @SqlType annotation", method));
            this.returnType = returnType.value();

            Class<?> actualReturnType = method.getReturnType();
            if (Primitives.isWrapperType(actualReturnType)) {
                checkArgument(nullable, "Method %s has return value with type %s that is missing @Nullable", method, actualReturnType);
            }

            Stream.of(method.getAnnotationsByType(TypeParameter.class))
                    .forEach(typeParameters::add);

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
                    checkArgument(annotations.length == 1, "Meta parameters may only have a single annotation");
                    checkArgument(argumentTypes.isEmpty(), "Meta parameter must come before parameters");
                    Annotation annotation = annotations[0];
                    if (annotation instanceof TypeParameter) {
                        checkArgument(typeParameters.contains(annotation), "Injected type parameters must be declared with @TypeParameter annotation on the method");
                    }
                    dependencies.add(parseDependency(annotation));
                }
                else {
                    SqlType type = null;
                    boolean nullableArgument = false;
                    for (Annotation annotation : annotations) {
                        if (annotation instanceof SqlType) {
                            type = (SqlType) annotation;
                        }
                        if (annotation instanceof Nullable) {
                            nullableArgument = true;
                        }
                    }
                    requireNonNull(type, format("@SqlType annotation missing for argument to %s", method));
                    if (Primitives.isWrapperType(parameterType)) {
                        checkArgument(nullableArgument, "Method %s has parameter with type %s that is missing @Nullable", method, parameterType);
                    }
                    if (typeParameterNames.contains(type.value()) && !(parameterType == Object.class && nullableArgument)) {
                        // Infer specialization on this type parameter. We don't do this for @Nullable Object because it could match a type like BIGINT
                        Class<?> specialization = specializedTypeParameters.get(type.value());
                        Class<?> nativeParameterType = Primitives.unwrap(parameterType);
                        checkArgument(specialization == null || specialization.equals(nativeParameterType), "%s has conflicting specializations %s and %s", type.value(), specialization, nativeParameterType);
                        specializedTypeParameters.put(type.value(), nativeParameterType);
                    }
                    argumentNativeContainerTypes.add(parameterType);
                    argumentTypes.add(type.value());
                    nullableArguments.add(nullableArgument);
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
            requireNonNull(constructor, format("%s is an instance method and requires a public constructor to be declared with %s type parameters", method.getName(), typeParameters));
            for (int i = 0; i < constructor.getParameterCount(); i++) {
                Annotation[] annotations = constructor.getParameterAnnotations()[i];
                checkArgument(containsMetaParameter(annotations), "Constructors may only have meta parameters");
                checkArgument(annotations.length == 1, "Meta parameters may only have a single annotation");
                Annotation annotation = annotations[0];
                if (annotation instanceof TypeParameter) {
                    checkArgument(typeParameters.contains(annotation), "Injected type parameters must be declared with @TypeParameter annotation on the constructor");
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
                checkArgument(typeParameterNames.contains(specialization.name()), "%s does not match any declared type parameters (%s)", specialization.name(), typeParameters);
                Class<?> existingSpecialization = specializedTypeParameters.get(specialization.name());
                checkArgument(existingSpecialization == null || existingSpecialization.equals(specialization.nativeContainerType()), "%s has conflicting specializations %s and %s", specialization.name(), existingSpecialization, specialization.nativeContainerType());
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

        private static List<TypeParameterRequirement> createTypeParameters(Iterable<TypeParameter> typeParameters, List<ImplementationDependency> dependencies)
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
            ImmutableList.Builder<TypeParameterRequirement> typeParameterRequirements = ImmutableList.builder();
            for (TypeParameter typeParameter : typeParameters) {
                String name = typeParameter.value();
                if (orderableRequired.contains(name)) {
                    typeParameterRequirements.add(orderableTypeParameter(name));
                }
                else if (comparableRequired.contains(name)) {
                    typeParameterRequirements.add(comparableTypeParameter(name));
                }
                else {
                    typeParameterRequirements.add(typeParameter(name));
                }
            }
            return typeParameterRequirements.build();
        }

        private static ImplementationDependency parseDependency(Annotation annotation)
        {
            if (annotation instanceof TypeParameter) {
                return new TypeImplementationDependency(((TypeParameter) annotation).value());
            }
            if (annotation instanceof OperatorDependency) {
                OperatorDependency operator = (OperatorDependency) annotation;
                return new OperatorImplementationDependency(operator.operator(), operator.returnType(), Arrays.asList(operator.argumentTypes()));
            }
            throw new IllegalArgumentException("Unsupported annotation " + annotation.getClass().getSimpleName());
        }

        private static boolean containsMetaParameter(Annotation[] annotations)
        {
            for (Annotation annotation : annotations) {
                if (annotation instanceof TypeParameter) {
                    return true;
                }
                if (annotation instanceof OperatorDependency) {
                    return true;
                }
            }
            return false;
        }

        public Implementation get()
        {
            Signature signature = new Signature(functionName, SCALAR, createTypeParameters(typeParameters, dependencies), returnType, argumentTypes, false);
            return new Implementation(signature,
                    nullable,
                    nullableArguments,
                    methodHandle,
                    dependencies,
                    constructorMethodHandle,
                    constructorDependencies,
                    argumentNativeContainerTypes,
                    specializedTypeParameters);
        }
    }

    private static final class Implementation
    {
        private final Signature signature;
        private final boolean nullable;
        private final List<Boolean> nullableArguments;
        private final MethodHandle methodHandle;
        private final List<ImplementationDependency> dependencies;
        private final Optional<MethodHandle> constructor;
        private final List<ImplementationDependency> constructorDependencies;
        private final List<Class<?>> argumentNativeContainerTypes;
        private final Map<String, Class<?>> specializedTypeParameters;

        private Implementation(
                Signature signature,
                boolean nullable,
                List<Boolean> nullableArguments,
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
            this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");
            this.dependencies = ImmutableList.copyOf(requireNonNull(dependencies, "dependencies is null"));
            this.constructor = requireNonNull(constructor, "constructor is null");
            this.constructorDependencies = ImmutableList.copyOf(requireNonNull(constructorDependencies, "constructorDependencies is null"));
            this.argumentNativeContainerTypes = ImmutableList.copyOf(requireNonNull(argumentNativeContainerTypes, "argumentNativeContainerTypes is null"));
            this.specializedTypeParameters = ImmutableMap.copyOf(requireNonNull(specializedTypeParameters, "specializedTypeParameters is null"));
        }

        public MethodHandleAndConstructor specialize(Signature boundSignature, Map<String, Type> boundTypeParameters, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            for (Map.Entry<String, Class<?>> entry : specializedTypeParameters.entrySet()) {
                if (!entry.getValue().isAssignableFrom(boundTypeParameters.get(entry.getKey()).getJavaType())) {
                    return null;
                }
            }
            Class<?> returnContainerType = getNullAwareContainerType(typeManager.getType(boundSignature.getReturnType()).getJavaType(), nullable);
            if (!returnContainerType.equals(methodHandle.type().returnType())) {
                return null;
            }
            for (int i = 0; i < boundSignature.getArgumentTypes().size(); i++) {
                Class<?> argumentContainerType = getNullAwareContainerType(typeManager.getType(boundSignature.getArgumentTypes().get(i)).getJavaType(), nullableArguments.get(i));
                if (!argumentNativeContainerTypes.get(i).isAssignableFrom(argumentContainerType)) {
                    return null;
                }
            }
            MethodHandle methodHandle = this.methodHandle;
            for (ImplementationDependency dependency : dependencies) {
                methodHandle = methodHandle.bindTo(dependency.resolve(boundTypeParameters, typeManager, functionRegistry));
            }
            MethodHandle constructor = null;
            if (this.constructor.isPresent()) {
                constructor = this.constructor.get();
                for (ImplementationDependency dependency : constructorDependencies) {
                    constructor = constructor.bindTo(dependency.resolve(boundTypeParameters, typeManager, functionRegistry));
                }
            }
            return new MethodHandleAndConstructor(methodHandle, Optional.ofNullable(constructor));
        }

        private static Class<?> getNullAwareContainerType(Class<?> clazz, boolean nullable)
        {
            if (nullable) {
                return Primitives.wrap(clazz);
            }
            checkArgument(clazz != void.class);
            return clazz;
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

        public MethodHandle getMethodHandle()
        {
            return methodHandle;
        }

        public List<ImplementationDependency> getDependencies()
        {
            return dependencies;
        }
    }

    private static final class MethodHandleAndConstructor
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
        Object resolve(Map<String, Type> types, TypeManager typeManager, FunctionRegistry functionRegistry);
    }

    private static final class OperatorImplementationDependency
            implements ImplementationDependency
    {
        private final OperatorType operator;
        private final Signature signature;

        private OperatorImplementationDependency(OperatorType operator, String returnType, List<String> argumentTypes)
        {
            this.operator = requireNonNull(operator, "operator is null");
            requireNonNull(returnType, "returnType is null");
            requireNonNull(argumentTypes, "argumentTypes is null");
            this.signature = internalOperator(operator, returnType, argumentTypes);
        }

        public OperatorType getOperator()
        {
            return operator;
        }

        public Signature getSignature()
        {
            return signature;
        }

        @Override
        public MethodHandle resolve(Map<String, Type> types, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            Signature signature = bindSignature(this.signature, types, this.signature.getArgumentTypes().size());
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
        public Type resolve(Map<String, Type> types, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            return typeManager.getType(signature.bindParameters(types));
        }
    }
}
