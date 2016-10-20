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

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.function.FunctionDependency;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.LiteralParameter;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Set;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.collect.ImmutableList.toImmutableList;

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

    public static class Factory
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
    }
}
