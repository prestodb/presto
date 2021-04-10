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
package com.facebook.presto.operator.annotations;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.IsNull;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.LongVariableConstraint;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.function.TypeParameterSpecialization;
import com.facebook.presto.spi.function.TypeVariableConstraint;
import com.facebook.presto.type.Constraint;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.facebook.presto.common.function.OperatorType.BETWEEN;
import static com.facebook.presto.common.function.OperatorType.CAST;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.HASH_CODE;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.operator.annotations.ImplementationDependency.isImplementationDependencyAnnotation;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;
import static java.util.Arrays.asList;

public class FunctionsParserHelper
{
    private static final Set<OperatorType> COMPARABLE_TYPE_OPERATORS = ImmutableSet.of(EQUAL, NOT_EQUAL, HASH_CODE);
    private static final Set<OperatorType> ORDERABLE_TYPE_OPERATORS = ImmutableSet.of(LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, BETWEEN);

    private FunctionsParserHelper()
    {}

    public static boolean containsAnnotation(Annotation[] annotations, Predicate<Annotation> predicate)
    {
        return Arrays.stream(annotations).anyMatch(predicate);
    }

    public static boolean containsImplementationDependencyAnnotation(Annotation[] annotations)
    {
        return containsAnnotation(annotations, ImplementationDependency::isImplementationDependencyAnnotation);
    }

    public static List<TypeVariableConstraint> createTypeVariableConstraints(Iterable<TypeParameter> typeParameters, List<ImplementationDependency> dependencies)
    {
        Set<String> orderableRequired = new HashSet<>();
        Set<String> comparableRequired = new HashSet<>();
        for (ImplementationDependency dependency : dependencies) {
            if (dependency instanceof OperatorImplementationDependency) {
                OperatorType operator = ((OperatorImplementationDependency) dependency).getOperator();
                if (operator == CAST) {
                    continue;
                }
                Set<String> argumentTypes = ((OperatorImplementationDependency) dependency).getArgumentTypes().stream()
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
                typeVariableConstraints.add(new TypeVariableConstraint(name, false, true, null, false, typeParameter.boundedBy()));
            }
            else if (comparableRequired.contains(name)) {
                typeVariableConstraints.add(new TypeVariableConstraint(name, true, false, null, false, typeParameter.boundedBy()));
            }
            else {
                typeVariableConstraints.add(new TypeVariableConstraint(name, false, false, null, false, typeParameter.boundedBy()));
            }
        }
        return typeVariableConstraints.build();
    }

    public static void validateSignaturesCompatibility(Optional<Signature> signatureOld, Signature signatureNew)
    {
        if (!signatureOld.isPresent()) {
            return;
        }
        checkArgument(signatureOld.get().equals(signatureNew), "Implementations with type parameters must all have matching signatures. %s does not match %s", signatureOld.get(), signatureNew);
    }

    @SafeVarargs
    public static Set<Method> findPublicStaticMethods(Class<?> clazz, Class<? extends Annotation>... includedAnnotations)
    {
        return findPublicStaticMethods(clazz, ImmutableSet.copyOf(asList(includedAnnotations)), ImmutableSet.of());
    }

    public static Set<Method> findPublicStaticMethods(Class<?> clazz, Set<Class<? extends Annotation>> includedAnnotations, Set<Class<? extends Annotation>> excludedAnnotations)
    {
        return findMethods(
                clazz.getMethods(),
                method -> checkArgument(isStatic(method.getModifiers()) && isPublic(method.getModifiers()), "Annotated method [%s] must be static and public", method.getName()),
                includedAnnotations,
                excludedAnnotations);
    }

    @SafeVarargs
    public static Set<Method> findPublicMethods(Class<?> clazz, Class<? extends Annotation>... includedAnnotations)
    {
        return findPublicMethods(clazz, ImmutableSet.copyOf(asList(includedAnnotations)), ImmutableSet.of());
    }

    public static Set<Method> findPublicMethods(Class<?> clazz, Set<Class<? extends Annotation>> includedAnnotations, Set<Class<? extends Annotation>> excludedAnnotations)
    {
        return findMethods(
                clazz.getDeclaredMethods(),
                method -> checkArgument(isPublic(method.getModifiers()), "Annotated method [%s] must be public"),
                includedAnnotations,
                excludedAnnotations);
    }

    public static Set<Method> findMethods(
            Method[] allMethods,
            Consumer<Method> methodChecker,
            Set<Class<? extends Annotation>> includedAnnotations,
            Set<Class<? extends Annotation>> excludedAnnotations)
    {
        ImmutableSet.Builder<Method> methods = ImmutableSet.builder();
        for (Method method : allMethods) {
            boolean included = false;
            boolean excluded = false;

            for (Annotation annotation : method.getAnnotations()) {
                for (Class<?> annotationClass : excludedAnnotations) {
                    if (annotationClass.isInstance(annotation)) {
                        excluded = true;
                        break;
                    }
                }
                if (excluded) {
                    break;
                }
                if (included) {
                    continue;
                }
                for (Class<?> annotationClass : includedAnnotations) {
                    if (annotationClass.isInstance(annotation)) {
                        included = true;
                        break;
                    }
                }
            }

            if (included && !excluded) {
                methodChecker.accept(method);
                methods.add(method);
            }
        }
        return methods.build();
    }

    public static Optional<Constructor<?>> findConstructor(Class<?> clazz)
    {
        Constructor<?>[] constructors = clazz.getConstructors();
        checkArgument(constructors.length <= 1, "Class [%s] must have no more than 1 public constructor");
        if (constructors.length == 0) {
            return Optional.empty();
        }
        return Optional.of(constructors[0]);
    }

    public static Set<String> parseLiteralParameters(Method method)
    {
        LiteralParameters literalParametersAnnotation = method.getAnnotation(LiteralParameters.class);
        if (literalParametersAnnotation == null) {
            return ImmutableSet.of();
        }

        return ImmutableSet.copyOf(literalParametersAnnotation.value());
    }

    public static boolean containsLegacyNullable(Annotation[] annotations)
    {
        return Arrays.stream(annotations)
                .map(Annotation::annotationType)
                .map(Class::getName)
                .anyMatch(name -> name.equals(Nullable.class.getName()));
    }

    public static boolean isPrestoAnnotation(Annotation annotation)
    {
        return isImplementationDependencyAnnotation(annotation) ||
                annotation instanceof SqlType ||
                annotation instanceof SqlNullable ||
                annotation instanceof IsNull;
    }

    public static Optional<String> parseDescription(AnnotatedElement base, AnnotatedElement override)
    {
        Optional<String> overrideDescription = parseDescription(override);
        if (overrideDescription.isPresent()) {
            return overrideDescription;
        }

        return parseDescription(base);
    }

    public static Optional<String> parseDescription(AnnotatedElement base)
    {
        Description description = base.getAnnotation(Description.class);
        return (description == null) ? Optional.empty() : Optional.of(description.value());
    }

    public static List<LongVariableConstraint> parseLongVariableConstraints(Method inputFunction)
    {
        return Stream.of(inputFunction.getAnnotationsByType(Constraint.class))
                .map(annotation -> new LongVariableConstraint(annotation.variable(), annotation.expression()))
                .collect(toImmutableList());
    }

    public static Map<String, Class<?>> getDeclaredSpecializedTypeParameters(Method method, Set<TypeParameter> typeParameters)
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
}
