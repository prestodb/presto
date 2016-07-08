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

import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.ParametricScalar;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.type.SqlType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.facebook.presto.operator.scalar.annotations.OperatorValidator.validateOperator;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public final class ScalarFromAnnotationsParser
{
    private ScalarFromAnnotationsParser() {}

    public static List<SqlScalarFunction> parseFunctionDefinition(Class<?> clazz)
    {
        ImmutableList.Builder<SqlScalarFunction> builder = ImmutableList.builder();
        for (ScalarHeaderAndMethods scalar : findScalarsInFunctionDefinitionClass(clazz)) {
            builder.add(parseParametricScalar(scalar, findConstructors(clazz)));
        }
        return builder.build();
    }

    public static List<SqlScalarFunction> parseFunctionDefinitions(Class<?> clazz)
    {
        ImmutableList.Builder<SqlScalarFunction> builder = ImmutableList.builder();
        for (ScalarHeaderAndMethods methods : findScalarsInFunctionSetClass(clazz)) {
            builder.add(parseParametricScalar(methods, findConstructors(clazz)));
        }
        return builder.build();
    }

    private static List<ScalarHeaderAndMethods> findScalarsInFunctionDefinitionClass(Class<?> annotated)
    {
        ImmutableList.Builder<ScalarHeaderAndMethods> builder = ImmutableList.builder();
        List<ScalarImplementationHeader> classHeaders = ScalarImplementationHeader.fromAnnotatedElement(annotated);
        checkArgument(!classHeaders.isEmpty(), "Class that defines function must be annotated with @ScalarFunction or @ScalarOperator.");

        for (ScalarImplementationHeader header : classHeaders) {
            Set<Method> methods = findPublicMethodsWithAnnotation(annotated, SqlType.class, ScalarFunction.class, ScalarOperator.class);
            checkArgument(!methods.isEmpty(), "Parametric class %s does not have any annotated methods", annotated.getName());
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
        for (Method method : findPublicMethodsWithAnnotation(annotated, SqlType.class, ScalarFunction.class, ScalarOperator.class)) {
            checkArgument((method.getAnnotation(ScalarFunction.class) != null) || (method.getAnnotation(ScalarOperator.class) != null),
                    "Method [%s] annotated with @SqlType is missing @ScalarFunction or @ScalarOperator", method);
            for (ScalarImplementationHeader header : ScalarImplementationHeader.fromAnnotatedElement(method)) {
                builder.add(new ScalarHeaderAndMethods(header, ImmutableSet.of(method)));
            }
        }
        return builder.build();
    }

    private static SqlScalarFunction parseParametricScalar(ScalarHeaderAndMethods scalar, Map<Set<TypeParameter>, Constructor<?>> constructors)
    {
        ImmutableMap.Builder<Signature, ScalarImplementation> exactImplementations = ImmutableMap.builder();
        ImmutableList.Builder<ScalarImplementation> specializedImplementations = ImmutableList.builder();
        ImmutableList.Builder<ScalarImplementation> genericImplementations = ImmutableList.builder();
        Optional<Signature> signature = Optional.empty();
        ScalarImplementationHeader header = scalar.getHeader();
        checkArgument(!header.getName().isEmpty());

        for (Method method : scalar.getMethods()) {
            ScalarImplementation implementation = ScalarImplementation.Parser.parseImplementation(header.getName(), method, constructors);
            if (implementation.getSignature().getTypeVariableConstraints().isEmpty()
                    && implementation.getSignature().getArgumentTypes().stream().noneMatch(TypeSignature::isCalculated)
                    && !implementation.getSignature().getReturnType().isCalculated()) {
                exactImplementations.put(implementation.getSignature(), implementation);
                continue;
            }

            if (implementation.hasSpecializedTypeParameters()) {
                specializedImplementations.add(implementation);
            }
            else {
                genericImplementations.add(implementation);
            }

            signature = signature.isPresent() ? signature : Optional.of(implementation.getSignature());
            validateSignature(signature, implementation.getSignature());
        }

        Signature scalarSignature = signature.orElseGet(() -> getOnlyElement(exactImplementations.build().keySet()));

        header.getOperatorType().ifPresent(operatorType ->
                validateOperator(operatorType, scalarSignature.getReturnType(), scalarSignature.getArgumentTypes()));

        ScalarImplementations implementations = new ScalarImplementations(exactImplementations.build(), specializedImplementations.build(), genericImplementations.build());

        return new ParametricScalar(scalarSignature, header.getHeader(), implementations);
    }

    private static void validateSignature(Optional<Signature> signatureOld, Signature signatureNew)
    {
        if (!signatureOld.isPresent()) {
            return;
        }
        checkArgument(signatureOld.get().equals(signatureNew), "Implementations with type parameters must all have matching signatures. %s does not match %s", signatureOld.get(), signatureNew);
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

    @SafeVarargs
    private static Set<Method> findPublicMethodsWithAnnotation(Class<?> clazz, Class<? extends Annotation>... annotationClasses)
    {
        ImmutableSet.Builder<Method> methods = ImmutableSet.builder();
        for (Method method : clazz.getDeclaredMethods()) {
            for (Annotation annotation : method.getAnnotations()) {
                for (Class<?> annotationClass : annotationClasses) {
                    if (annotationClass.isInstance(annotation)) {
                        checkArgument(Modifier.isPublic(method.getModifiers()), "Method [%s] annotated with @%s must be public", method, annotationClass.getSimpleName());
                        methods.add(method);
                    }
                }
            }
        }
        return methods.build();
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
