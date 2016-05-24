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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class ScalarFromAnnotationsParser
{
    private ScalarFromAnnotationsParser()
    {
    }

    public static List<SqlScalarFunction> parseFunctionDefinition(Class<?> clazz)
    {
        ImmutableList.Builder<SqlScalarFunction> builder = ImmutableList.builder();
        for (ScalarHeaderAndMethods scalar : findScalarsInFunctionDefinitionClass(clazz)) {
            builder.add(parseParametricScalar(scalar, findConstructors(clazz), clazz.getSimpleName()));
        }
        return builder.build();
    }

    public static List<SqlScalarFunction> parseFunctionDefinitions(Class<?> clazz)
    {
        ImmutableList.Builder<SqlScalarFunction> builder = ImmutableList.builder();
        for (ScalarHeaderAndMethods methods : findScalarsInFunctionSetClass(clazz)) {
            builder.add(parseParametricScalar(methods, findConstructors(clazz), clazz.getSimpleName()));
        }
        return builder.build();
    }

    private static List<ScalarHeaderAndMethods> findScalarsInFunctionDefinitionClass(Class annotated)
    {
        ImmutableList.Builder<ScalarHeaderAndMethods> builder = ImmutableList.builder();
        List<ScalarImplementationHeader> classHeaders = ScalarImplementationHeader.fromAnnotatedElement(annotated);
        checkArgument(!classHeaders.isEmpty(), "Class that defines function must be annotated with @ScalarFunction or @ScalarOperator.");

        for (ScalarImplementationHeader header : classHeaders) {
            builder.add(new ScalarHeaderAndMethods(header, findPublicMethodsWithAnnotation(annotated, SqlType.class)));
        }

        return builder.build();
    }

    private static List<ScalarHeaderAndMethods> findScalarsInFunctionSetClass(Class annotated)
    {
        ImmutableList.Builder<ScalarHeaderAndMethods> builder = ImmutableList.builder();
        for (Method method : findPublicMethodsWithAnnotation(annotated, SqlType.class)) {
            for (ScalarImplementationHeader header : ScalarImplementationHeader.fromAnnotatedElement(method)) {
                builder.add(new ScalarHeaderAndMethods(header, ImmutableList.of(method)));
            }
        }
        return builder.build();
    }

    private static SqlScalarFunction parseParametricScalar(ScalarHeaderAndMethods scalar, Map<Set<TypeParameter>, Constructor<?>> constructors, String objectName)
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
            else if (implementation.hasSpecializedTypeParameters()) {
                specializedImplementations.add(implementation);
            }
            else {
                genericImplementations.add(implementation);
            }

            signature = signature.isPresent() ? signature : Optional.of(implementation.getSignature());
            validateSignature(signature, implementation.getSignature());
        }

        Map<Signature, ScalarImplementation> exactImplementationsMap = exactImplementations.build();
        if (!signature.isPresent()) {
            signature = Optional.of(getOnlyElement(exactImplementationsMap.entrySet()).getKey());
        }

        ScalarImplementations implementations = new ScalarImplementations(exactImplementations.build(), specializedImplementations.build(), genericImplementations.build());

        return new ParametricScalar(signature.get(), header.getHeader(), implementations);
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

    private static class ScalarHeaderAndMethods
    {
        private final ScalarImplementationHeader header;
        private final List<Method> methods;

        public ScalarHeaderAndMethods(ScalarImplementationHeader header, List<Method> methods)
        {
            this.header = requireNonNull(header);
            this.methods = requireNonNull(methods);
        }

        public ScalarImplementationHeader getHeader()
        {
            return header;
        }

        public List<Method> getMethods()
        {
            return methods;
        }
    }
}
