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
package io.prestosql.operator.annotations;

import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.spi.function.Convention;
import io.prestosql.spi.function.FunctionDependency;
import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.spi.function.OperatorDependency;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.type.LiteralParameter;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.operator.annotations.FunctionsParserHelper.containsImplementationDependencyAnnotation;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;

public interface ImplementationDependency
{
    Object resolve(BoundVariables boundVariables, TypeManager typeManager, FunctionRegistry functionRegistry);

    static boolean isImplementationDependencyAnnotation(Annotation annotation)
    {
        return annotation instanceof TypeParameter ||
                annotation instanceof LiteralParameter ||
                annotation instanceof FunctionDependency ||
                annotation instanceof OperatorDependency;
    }

    static Optional<Annotation> getImplementationDependencyAnnotation(AnnotatedElement element)
    {
        if (!containsImplementationDependencyAnnotation(element.getAnnotations())) {
            return Optional.empty();
        }
        Annotation[] annotations = element.getAnnotations();
        checkArgument(annotations.length == 1, "Meta parameters may only have a single annotation [%s]", element);
        return Optional.of(annotations[0]);
    }

    static void validateImplementationDependencyAnnotation(AnnotatedElement element, Annotation annotation, Set<String> typeParametersNames, Collection<String> literalParameters)
    {
        if (annotation instanceof TypeParameter) {
            checkTypeParameters(parseTypeSignature(((TypeParameter) annotation).value()), typeParametersNames, element);
        }
        if (annotation instanceof LiteralParameter) {
            checkArgument(literalParameters.contains(((LiteralParameter) annotation).value()), "Parameter injected by @LiteralParameter must be declared with @LiteralParameters on the method [%s]", element);
        }
    }

    static void checkTypeParameters(TypeSignature typeSignature, Set<String> typeParameterNames, AnnotatedElement element)
    {
        // Check recursively if `typeSignature` contains something like `T<bigint>`
        if (typeParameterNames.contains(typeSignature.getBase())) {
            checkArgument(typeSignature.getParameters().isEmpty(), "Expected type parameter not to take parameters, but got %s on method [%s]", typeSignature.getBase(), element);
            return;
        }

        for (TypeSignatureParameter parameter : typeSignature.getParameters()) {
            Optional<TypeSignature> childTypeSignature = parameter.getTypeSignatureOrNamedTypeSignature();
            if (childTypeSignature.isPresent()) {
                checkTypeParameters(childTypeSignature.get(), typeParameterNames, element);
            }
        }
    }

    class Factory
    {
        private Factory()
        {}

        public static ImplementationDependency createDependency(Annotation annotation, Set<String> literalParameters)
        {
            if (annotation instanceof TypeParameter) {
                return new TypeImplementationDependency(((TypeParameter) annotation).value());
            }
            if (annotation instanceof LiteralParameter) {
                return new LiteralImplementationDependency(((LiteralParameter) annotation).value());
            }

            if (annotation instanceof FunctionDependency) {
                FunctionDependency functionDependency = (FunctionDependency) annotation;
                return new FunctionImplementationDependency(
                        functionDependency.name(),
                        parseTypeSignature(functionDependency.returnType(), literalParameters),
                        Arrays.stream(functionDependency.argumentTypes())
                                .map(signature -> parseTypeSignature(signature, literalParameters))
                                .collect(toImmutableList()),
                        toInvocationConvention(functionDependency.convention()));
            }
            if (annotation instanceof OperatorDependency) {
                OperatorDependency operatorDependency = (OperatorDependency) annotation;
                return new OperatorImplementationDependency(
                        operatorDependency.operator(),
                        parseTypeSignature(operatorDependency.returnType(), literalParameters),
                        Arrays.stream(operatorDependency.argumentTypes())
                                .map(signature -> parseTypeSignature(signature, literalParameters))
                                .collect(toImmutableList()),
                        toInvocationConvention(operatorDependency.convention()));
            }

            throw new IllegalArgumentException("Unsupported annotation " + annotation.getClass().getSimpleName());
        }

        private static Optional<InvocationConvention> toInvocationConvention(Convention convention)
        {
            if (convention.$notSpecified()) {
                return Optional.empty();
            }
            List<InvocationConvention.InvocationArgumentConvention> argumentConventions = new ArrayList<>();
            Collections.addAll(argumentConventions, convention.arguments());
            return Optional.of(new InvocationConvention(argumentConventions, convention.result(), convention.session()));
        }
    }
}
